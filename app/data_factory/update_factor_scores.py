import pandas as pd
from markets.market import Region
from db.db_common import DB
from db.duckdb import DuckDBController
from loguru import logger
import argparse


class FactorScoresUpdater:
    def __init__(self, db: DuckDBController):
        self.db = db
        # 需要标准化的连续因子列（按此顺序进行 z-score）
        self.factor_cols = [
            "trend_ma5", "trend_ma10", "trend_ma20", "trend_ma60",
            "trend_macd",
            "mom_5d", "mom_20d", "mom_60d",
            "rsi_factor",
            "vol_atr", "vol_boll_width",
            "vol_ratio", "obv_slope",
            "price_position"
        ]

    def calculate_factor_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有因子、评分及信号"""
        df = df.sort_values("date").copy()

        # ==============================
        # 1. 原始因子计算（未标准化）
        # ==============================
        close = df["close"]

        # --- 趋势因子 ---
        df["trend_ma5"] = (close - df["ma5"]) / df["ma5"]
        df["trend_ma10"] = (close - df["ma10"]) / df["ma10"]
        df["trend_ma20"] = (close - df["ma20"]) / df["ma20"]
        df["trend_ma60"] = (close - df["ma60"]) / df["ma60"]
        df["trend_macd"] = df["macd_hist"]

        # --- 动量因子 ---
        df["mom_5d"] = df["ret_5d"]                     # 5日收益率
        df["mom_20d"] = df["ret_20d"]                   # 20日收益率
        df["mom_60d"] = close.pct_change(60)            # 60日收益率
        df["rsi_factor"] = (df["rsi14"] - 50) / 50      # RSI 偏离度

        # --- 波动率因子 ---
        df["vol_atr"] = df["atr14"] / close
        df["vol_boll_width"] = (df["boll_up"] - df["boll_down"]) / close

        # --- 成交量因子 ---
        df["vol_ratio"] = df["volume"] / df["vol_ma20"]
        df["obv_slope"] = df["obv"].diff(5) / 5

        # --- 价格位置 ---
        df["price_position"] = df["pct_from_ma20"]       # 偏离20日均线的百分比

        # ==============================
        # 2. 标准化（按日期分组 z-score）
        # ==============================
        for col in self.factor_cols:
            df[col] = df.groupby("date")[col].transform(
                lambda x: (x - x.mean()) / (x.std() + 1e-9)
            )

        # ==============================
        # 3. 二次因子（基于标准化后的值）
        # ==============================
        df["trend_acceleration"] = df["trend_ma5"] - df["trend_ma10"]
        df["momentum_acceleration"] = df["mom_5d"] - df["mom_20d"]

        # ==============================
        # 4. 各维度评分
        # ==============================
        df["trend_score"] = (
            df["trend_ma5"] * 0.20 +
            df["trend_ma10"] * 0.20 +
            df["trend_ma20"] * 0.15 +
            df["trend_ma60"] * 0.10 +
            df["trend_macd"] * 0.15 +
            df["trend_acceleration"] * 0.20
        )

        df["momentum_score"] = (
            df["mom_5d"] * 0.25 +
            df["mom_20d"] * 0.45 +
            df["mom_60d"] * 0.20 +
            df["rsi_factor"] * 0.10
        )

        df["volatility_score"] = (
            df["vol_atr"] * 0.50 +
            df["vol_boll_width"] * 0.50
        )

        df["volume_score"] = (
            df["vol_ratio"] * 0.70 +
            df["obv_slope"] * 0.30
        )

        df["composite_score"] = (
            df["trend_score"] * 0.35 +
            df["momentum_score"] * 0.40 +
            df["volume_score"] * 0.20 -
            df["volatility_score"] * 0.05
        )

        # ==============================
        # 5. 信号层（布尔值）
        # ==============================
        # 趋势加速信号
        df["acc_signal"] = df["trend_acceleration"] > 0

        # 多头/空头排列（基于原始移动均线判断）
        df["trend_strong"] = (
            (df["ma5"] > df["ma10"]) &
            (df["ma10"] > df["ma20"]) &
            (df["ma20"] > df["ma60"])
        )
        df["trend_weak"] = (
            (df["ma5"] < df["ma10"]) &
            (df["ma10"] < df["ma20"])
        )

        # 动量强势
        df["momentum_strong"] = (df["mom_5d"] > 0) & (df["mom_20d"] > 0)

        # 波动率过滤
        atr_mean = df["vol_atr"].rolling(20, min_periods=1).mean()
        df["low_volatility"] = df["vol_atr"] < atr_mean
        df["high_volatility"] = df["vol_atr"] > atr_mean * 1.5

        # 成交量异动
        df["volume_spike"] = df["volume"] > df["vol_ma5"] * 1.5
        df["volume_trend"] = (
            (df["vol_ma5"] > df["vol_ma10"]) &
            (df["vol_ma10"] > df["vol_ma20"])
        )

        # 突破确认
        df["breakout_confirm"] = (
            (df["close"] > df["boll_up"]) &
            df["volume_spike"] &
            df["acc_signal"]
        )

        # 反转信号
        df["reversal_signal"] = (
            (df["rsi14"] < 30) &
            (df["close"] < df["boll_down"]) &
            df["acc_signal"]
        )

        return df

    def update_factor_scores(self, region: Region | None = None):
        """更新指定市场（或全部市场）的因子分数表"""
        # 1. 获取需要更新的股票列表
        if region is None:
            rows = self.db.read("""
                SELECT
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(f.date) AS factor_max_date
                FROM stock_daily d
                LEFT JOIN stock_factor_scores f ON d.symbol = f.symbol
                GROUP BY d.symbol
            """, fetch_mode="all")
        else:
            rows = self.db.read(f"""
                SELECT
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(f.date) AS factor_max_date
                FROM stock_daily d
                LEFT JOIN stock_factor_scores f ON d.symbol = f.symbol
                WHERE d.market = '{region.value.upper()}'
                GROUP BY d.symbol
            """, fetch_mode="all")

        symbols_to_update = []
        for symbol, daily_max, factor_max in rows:
            if factor_max is None or daily_max > factor_max:
                symbols_to_update.append(symbol)

        if not symbols_to_update:
            logger.info("No symbols need factor score update.")
            return

        # 2. 逐个股票计算并写入
        for symbol in symbols_to_update:
            logger.info(f"Updating factor scores for {symbol}")

            # 获取该股票已存在的最大日期（用于增量）
            result = self.db.read(
                f"SELECT date FROM stock_factor_scores WHERE symbol = '{symbol}' ORDER BY date DESC LIMIT 1;",
                fetch_mode="one"
            )
            last_date = result[0] if result and result[0] is not None else None

            # 拉取原始数据（行情 + 技术指标）
            # 假设技术指标表名为 stock_indicators
            if last_date is None:
                query = f"""
                    SELECT
                        d.symbol, d.date, d.close, d.volume,
                        i.ma5, i.ma10, i.ma20, i.ma60,
                        i.macd_hist,
                        i.rsi14,
                        i.atr14,
                        i.boll_up, i.boll_down,
                        i.vol_ma5, i.vol_ma10, i.vol_ma20,
                        i.obv,
                        i.ret_5d, i.ret_20d, i.pct_from_ma20
                    FROM stock_daily d
                    JOIN stock_indicators i ON d.symbol = i.symbol AND d.date = i.date
                    WHERE d.symbol = '{symbol}'
                    ORDER BY d.date
                """
            else:
                # 增量更新：拉取最后日期前60天至今的数据（保证滚动计算所需窗口）
                query = f"""
                    SELECT
                        d.symbol, d.date, d.close, d.volume,
                        i.ma5, i.ma10, i.ma20, i.ma60,
                        i.macd_hist,
                        i.rsi14,
                        i.atr14,
                        i.boll_up, i.boll_down,
                        i.vol_ma5, i.vol_ma10, i.vol_ma20,
                        i.obv,
                        i.ret_5d, i.ret_20d, i.pct_from_ma20
                    FROM stock_daily d
                    JOIN stock_indicators i ON d.symbol = i.symbol AND d.date = i.date
                    WHERE d.symbol = '{symbol}'
                      AND d.date >= DATE '{last_date}' - INTERVAL 60 DAY
                    ORDER BY d.date
                """

            df = self.db.read(query, fetch_mode="df")
            if df.empty:
                logger.warning(f"No data found for {symbol}, skip.")
                continue

            # 计算因子
            df = self.calculate_factor_scores(df)

            # 仅保留新增的日期（增量模式）
            if last_date is not None:
                df = df[df["date"] > last_date]

            if df.empty:
                logger.info(f"No new dates for {symbol}")
                continue

            # 准备写入数据库（只选取目标表中存在的字段）
            df_out = df[[
                "symbol", "date",
                "trend_ma5", "trend_ma10", "trend_ma20", "trend_ma60", "trend_macd",
                "trend_acceleration",
                "mom_5d", "mom_20d", "mom_60d", "momentum_acceleration", "rsi_factor",
                "vol_atr", "vol_boll_width",
                "vol_ratio", "obv_slope",
                "price_position",
                "trend_score", "momentum_score", "volatility_score", "volume_score",
                "composite_score",
                "acc_signal",
                "trend_strong", "trend_weak",
                "momentum_strong",
                "low_volatility", "high_volatility",
                "volume_spike", "volume_trend",
                "breakout_confirm", "reversal_signal"
            ]]

            # 写入临时表并执行 INSERT OR IGNORE
            sql = """
                INSERT OR IGNORE INTO stock_factor_scores (
                    symbol, date,
                    trend_ma5, trend_ma10, trend_ma20, trend_ma60, trend_macd,
                    trend_acceleration,
                    mom_5d, mom_20d, mom_60d, momentum_acceleration, rsi_factor,
                    vol_atr, vol_boll_width,
                    vol_ratio, obv_slope,
                    price_position,
                    trend_score, momentum_score, volatility_score, volume_score,
                    composite_score,
                    acc_signal,
                    trend_strong, trend_weak,
                    momentum_strong,
                    low_volatility, high_volatility,
                    volume_spike, volume_trend,
                    breakout_confirm, reversal_signal
                )
                SELECT
                    symbol, date,
                    trend_ma5, trend_ma10, trend_ma20, trend_ma60, trend_macd,
                    trend_acceleration,
                    mom_5d, mom_20d, mom_60d, momentum_acceleration, rsi_factor,
                    vol_atr, vol_boll_width,
                    vol_ratio, obv_slope,
                    price_position,
                    trend_score, momentum_score, volatility_score, volume_score,
                    composite_score,
                    acc_signal,
                    trend_strong, trend_weak,
                    momentum_strong,
                    low_volatility, high_volatility,
                    volume_spike, volume_trend,
                    breakout_confirm, reversal_signal
                FROM temp_df
            """
            self.db.write(df_out, sql=sql, view_name="temp_df")
            logger.info(f"Inserted {len(df_out)} rows for {symbol}")

    def update_by_markets(self):
        """更新所有市场"""
        for region in Region:
            logger.info(f"Updating factor scores for region: {region.value.upper()}")
            self.update_factor_scores(region)
        logger.info("Finished updating factor scores for all regions")


def run_factor_scores(db: DuckDBController, market: str | None = None):
    """Step 3: 计算 stock_factor_scores（依赖 stock_indicators）"""
    logger.info("=" * 60)
    logger.info("[Step 3/3] 开始计算 stock_factor_scores...")
    logger.info("=" * 60)

    updater = FactorScoresUpdater(db)
    if market:
        region = Region(market.lower())
        updater.update_factor_scores(region)
    else:
        updater.update_by_markets()

    logger.info("[Step 3/3] ✅ stock_factor_scores 计算完成")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="统一Factors计算 — 随时运行，永不重复计算"
    )
    parser.add_argument(
        "--market",
        "-m",
        type=str,
        default=None,
        choices=["cn", "hk", "us"],
        help="只更新指定市场（默认更新全部）",
    )
    args = parser.parse_args()

    logger.info("🚀 启动Factors计算")
    logger.info(f"   DB Path: {DB}")
    logger.info(f"   Market:  {args.market or 'ALL'}")

    db = DuckDBController(DB)
    run_factor_scores(db, args.market)
