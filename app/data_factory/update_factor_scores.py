import pandas as pd
from markets.market import Region
from db.duckdb import DuckDBController
from utils.log_manager import get_logger

logger = get_logger(__name__)


class FactorScoresUpdater():
    def __init__(self, db: DuckDBController):
        self.db = db

    def calculate_factor_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date")
        close = df["close"]

        # ==============================
        # Trend（新增短周期）
        # ==============================
        df["trend_ma5"] = (close - df["ma5"]) / df["ma5"]
        df["trend_ma10"] = (close - df["ma10"]) / df["ma10"]
        df["trend_ma20"] = (close - df["ma20"]) / df["ma20"]
        df["trend_ma60"] = (close - df["ma60"]) / df["ma60"]
        df["trend_macd"] = df["macd_hist"]
        df["trend_acceleration"] = df["trend_ma5"] - df["trend_ma10"]

        # ==============================
        # Momentum
        # ==============================
        df["mom_5d"] = df["ret_5d"]
        df["mom_20d"] = df["ret_20d"]
        df["mom_60d"] = close.pct_change(60)
        df["rsi_factor"] = (df["rsi14"] - 50) / 50

        # ==============================
        # Volatility
        # ==============================
        df["vol_atr"] = df["atr14"] / close
        df["vol_boll_width"] = (df["boll_up"] - df["boll_down"]) / close

        # ==============================
        # Volume
        # ==============================
        df["vol_ratio"] = df["volume"] / df["vol_ma20"]
        df["obv_slope"] = df["obv"].diff(5) / 5

        # ==============================
        # Price Position
        # ==============================
        df["price_position"] = df["pct_from_ma20"]

        # ==============================
        # 标准化（包含新增字段）
        # ==============================
        factor_cols = [
            "trend_ma5", "trend_ma10", "trend_ma20", "trend_ma60",
            "trend_macd", "trend_acceleration",

            "mom_5d", "mom_20d", "mom_60d",
            "momentum_acceleration",
            "rsi_factor",

            "vol_atr", "vol_boll_width",
            "vol_ratio", "obv_slope",

            "price_position"
        ]

        for col in factor_cols:
            df[col] = df.groupby("date")[col].transform(
                lambda x: (x - x.mean()) / (x.std() + 1e-9)
            )

        # ==============================
        # Trend Score（重点：短周期权重更高）
        # ==============================
        df["trend_score"] = (
            df["trend_ma5"] * 0.2 +
            df["trend_ma10"] * 0.2 +
            df["trend_ma20"] * 0.15 +
            df["trend_ma60"] * 0.1 +
            df["trend_macd"] * 0.15 +
            df["trend_acceleration"] * 0.2   # ✅ 新增核心因子
        )

        # ==============================
        # Momentum Score
        # ==============================
        df["momentum_score"] = (
            df["mom_5d"] * 0.25 +
            df["mom_20d"] * 0.45 +
            df["mom_60d"] * 0.2 +
            df["rsi_factor"] * 0.1
        )

        # ==============================
        # Volatility Score
        # ==============================
        df["volatility_score"] = (
            df["vol_atr"] * 0.5 +
            df["vol_boll_width"] * 0.5
        )

        # ==============================
        # Volume Score
        # ==============================
        df["volume_score"] = (
            df["vol_ratio"] * 0.7 +
            df["obv_slope"] * 0.3
        )

        # ==============================
        # 综合评分（偏短线优化）
        # ==============================
        df["composite_score"] = (
            df["trend_score"] * 0.35 +
            df["momentum_score"] * 0.4 +
            df["volume_score"] * 0.2 -
            df["volatility_score"] * 0.05
        )

        # ===== Trend Enhancement =====
        df["trend_acceleration"] = df["trend_ma5"] - df["trend_ma10"]

        df["acc_signal"] = df["trend_acceleration"] > 0

        df["trend_strong"] = (
            (df["ma5"] > df["ma10"]) &
            (df["ma10"] > df["ma20"]) &
            (df["ma20"] > df["ma60"])
        )

        df["trend_weak"] = (
            (df["ma5"] < df["ma10"]) &
            (df["ma10"] < df["ma20"])
        )

        # ===== Momentum Enhancement =====
        df["momentum_acceleration"] = df["mom_5d"] - df["mom_20d"]

        df["momentum_strong"] = (
            (df["mom_5d"] > 0) &
            (df["mom_20d"] > 0)
        )

        # ===== Volatility Filter =====
        df["atr_mean"] = df["vol_atr"].rolling(20).mean()

        df["low_volatility"] = df["vol_atr"] < df["atr_mean"]
        df["high_volatility"] = df["vol_atr"] > df["atr_mean"] * 1.5

        # ===== Volume Enhancement =====
        df["volume_spike"] = df["volume"] > df["vol_ma5"] * 1.5

        df["volume_trend"] = (
            (df["vol_ma5"] > df["vol_ma10"]) &
            (df["vol_ma10"] > df["vol_ma20"])
        )

        # ===== Breakout Confirm =====
        df["breakout_confirm"] = (
            (df["close"] > df["boll_up"]) &
            df["volume_spike"] &
            df["acc_signal"]
        )

        # ===== Reversal Signal =====
        df["reversal_signal"] = (
            (df["rsi14"] < 30) &
            (df["close"] < df["boll_down"]) &
            df["acc_signal"]
        )
        return df

    def update_factor_scores(self, region: Region | None = None):
        if region is None:
            rows = self.db.read(
                """SELECT 
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(i.date) AS ind_max_date
                FROM stock_daily d
                LEFT JOIN stock_factor_scores i
                ON d.symbol = i.symbol
                GROUP BY d.symbol
                """, fetch_mode="all")
        else:
            rows = self.db.read(
                f"""SELECT 
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(i.date) AS ind_max_date
                FROM stock_daily d
                LEFT JOIN stock_factor_scores i
                ON d.symbol = i.symbol
                WHERE  d.market = '{region.value.upper()}'
                GROUP BY d.symbol
                """, fetch_mode="all")

        symbols = []
        for symbol, daily_max, ind_max in rows:
            if ind_max is None:
                # print(symbol, "从头计算 factor scores")
                symbols.append(symbol)

            elif daily_max > ind_max:
                # print(symbol, "增量计算 factor scores")
                symbols.append(symbol)

        for symbol in symbols:
            logger.info(f"factor scores update: {symbol}")

            # 1️⃣ factor 表最后日期
            result = self.db.read(f"""
                SELECT MAX(date)
                FROM stock_factor_scores
                WHERE symbol = '{symbol}'
            """, fetch_mode="one")

            # 2. Python层：安全取值
            if result and result[0] is not None:
                last_date = result[0]
            else:
                last_date = None  # 或设置初始日期，如'1970-01-01'

            # 2️⃣ 拉 indicator + price（需要 rolling）
            if last_date is None:
                df = self.db.read(f"""
                    SELECT d.*, i.*
                    FROM stock_daily d
                    JOIN stock_factor_scores i
                    ON d.symbol = i.symbol AND d.date = i.date
                    WHERE d.symbol = '{symbol}'
                    ORDER BY d.date
                """, fetch_mode="df")
            else:
                df = self.db.read(f"""
                    SELECT d.*, i.*
                    FROM stock_daily d
                    JOIN stock_factor_scores i
                    ON d.symbol = i.symbol AND d.date = i.date
                    WHERE d.symbol = '{symbol}'
                    AND d.date >= DATE '{last_date}' - INTERVAL 60 DAY
                    ORDER BY d.date
                """, fetch_mode="df")

            if len(df) == 0:
                continue

            df = self.calculate_factor_scores(df)

            # 3️⃣ 只保留新增
            if last_date is not None:
                df = df[df["date"] > last_date]

            if len(df) == 0:
                continue

            df = df[[
                "symbol", "date",
                "trend_ma5", "trend_ma10", "trend_ma20", "trend_ma60", "trend_macd", "trend_acceleration",
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
                "breakout_confirm",
                "reversal_signal"
            ]]

            rows = len(df)

            sql = """
                INSERT OR IGNORE INTO stock_factor_scores (
                    symbol, date,

                    trend_ma5, trend_ma10, trend_ma20, trend_ma60, trend_macd, trend_acceleration,
                    
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
                    breakout_confirm,
                    reversal_signal
                )
                SELECT
                    symbol, date,
                    trend_ma5, trend_ma10, trend_ma20, trend_ma60, trend_macd, trend_acceleration,
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
                    breakout_confirm,
                    reversal_signal
                FROM temp_df
            """
            df = self.db.write(
                df, sql=sql, view_name="temp_df"
            )

            logger.info(f"Updated factor scores for {symbol} - inserted rows: {rows}")

    def update_by_markets(self):
        for region in Region:
            logger.info(f"Updating factor scores for region: {region.value.upper()}")
            self.update_factor_scores(region)

        logger.info("Finished updating factor socres for all regions")


if __name__ == "__main__":
    db = DuckDBController("../data/stock.duckdb")
    updater = FactorScoresUpdater(db)
    updater.update_by_markets()
