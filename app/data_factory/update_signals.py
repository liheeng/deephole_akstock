import argparse

import pandas as pd
from loguru import logger

from db.db_common import DB
from db.duckdb import DuckDBController
from markets.market import Region


class SignalsUpdater:

    def __init__(self, db: DuckDBController):
        self.db = db

    @staticmethod
    def safe_gt(a: pd.Series, b: pd.Series) -> pd.Series:
        """
        安全 >
        数据不足时返回 NULL 而不是 False
        """
        result = pd.Series(pd.NA, index=a.index, dtype="boolean")

        mask = a.notna() & b.notna()

        result.loc[mask] = a.loc[mask] > b.loc[mask]

        return result

    @staticmethod
    def safe_lt(a: pd.Series, b: pd.Series) -> pd.Series:
        """
        安全 <
        """
        result = pd.Series(pd.NA, index=a.index, dtype="boolean")

        mask = a.notna() & b.notna()

        result.loc[mask] = a.loc[mask] < b.loc[mask]

        return result

    def calculate_signals(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:

        # =========================
        # 基础清洗
        # =========================
        df = df.sort_values("date").copy()

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )

        # =========================
        # 趋势
        # =========================
        df["ma5_above_ma20"] = self.safe_gt(
            df["ma5"],
            df["ma20"]
        )

        df["ma20_above_ma60"] = self.safe_gt(
            df["ma20"],
            df["ma60"]
        )

        df["close_above_ma20"] = self.safe_gt(
            df["close"],
            df["ma20"]
        )

        # =========================
        # RSI
        # =========================
        rsi70 = pd.Series(
            70,
            index=df.index
        )

        rsi30 = pd.Series(
            30,
            index=df.index
        )

        df["rsi_overbought"] = self.safe_gt(
            df["rsi14"],
            rsi70
        )

        df["rsi_oversold"] = self.safe_lt(
            df["rsi14"],
            rsi30
        )

        # =========================
        # Breakout / Breakdown
        # 修复未来函数
        # =========================
        df["hhv20"] = (
            df["high"]
            .rolling(
                20,
                min_periods=20
            )
            .max()
            .shift(1)
        )

        df["llv20"] = (
            df["low"]
            .rolling(
                20,
                min_periods=20
            )
            .min()
            .shift(1)
        )

        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["close"].notna()
            & df["hhv20"].notna()
        )

        result.loc[mask] = (
            df.loc[mask, "close"]
            >= df.loc[mask, "hhv20"]
        )

        df["breakout_20d"] = result

        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["close"].notna()
            & df["llv20"].notna()
        )

        result.loc[mask] = (
            df.loc[mask, "close"]
            <= df.loc[mask, "llv20"]
        )

        df["breakdown_20d"] = result

        # =========================
        # ATR Volatility
        # =========================
        df["atr_mean"] = (
            df["atr14"]
            .rolling(
                20,
                min_periods=20
            )
            .mean()
        )

        df["atr_high_vol"] = self.safe_gt(
            df["atr14"],
            df["atr_mean"]
        )

        # =========================
        # Bollinger
        # =========================
        df["boll_upper_break"] = self.safe_gt(
            df["close"],
            df["boll_up"]
        )

        df["boll_lower_break"] = self.safe_lt(
            df["close"],
            df["boll_down"]
        )

        # =========================
        # Volume
        # =========================
        df["vol_ma5"] = (
            df["volume"]
            .rolling(
                5,
                min_periods=5
            )
            .mean()
        )

        df["vol_ma20"] = (
            df["volume"]
            .rolling(
                20,
                min_periods=20
            )
            .mean()
        )

        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        vol_base = df["vol_ma5"].replace(
            0,
            pd.NA
        )

        mask = (
            df["volume"].notna()
            & vol_base.notna()
        )

        result.loc[mask] = (
            df.loc[mask, "volume"]
            > vol_base.loc[mask] * 1.5
        )

        df["vol_spike"] = result

        df["vol_ma5_above_ma20"] = self.safe_gt(
            df["vol_ma5"],
            df["vol_ma20"]
        )

        # =========================
        # 连续涨跌
        # =========================
        # =========================
        # Factor-style Signals
        # 从 factor_scores 拆分而来
        # =========================

        # --------------------------------
        # Trend Acceleration
        # --------------------------------
        trend_acc = (
            df["ma5"] - df["ma10"]
        )

        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["ma5"].notna()
            & df["ma10"].notna()
        )

        result.loc[mask] = (
            trend_acc.loc[mask] > 0
        )

        df["acc_signal"] = result

        # --------------------------------
        # Trend Strong
        # 多头排列
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["ma5"].notna()
            & df["ma10"].notna()
            & df["ma20"].notna()
            & df["ma60"].notna()
        )

        result.loc[mask] = (
            (df.loc[mask, "ma5"]
             > df.loc[mask, "ma10"])
            &
            (df.loc[mask, "ma10"]
             > df.loc[mask, "ma20"])
            &
            (df.loc[mask, "ma20"]
             > df.loc[mask, "ma60"])
        )

        df["trend_strong"] = result

        # --------------------------------
        # Trend Weak
        # 空头排列
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["ma5"].notna()
            & df["ma10"].notna()
            & df["ma20"].notna()
        )

        result.loc[mask] = (
            (df.loc[mask, "ma5"]
             < df.loc[mask, "ma10"])
            &
            (df.loc[mask, "ma10"]
             < df.loc[mask, "ma20"])
        )

        df["trend_weak"] = result

        # --------------------------------
        # Momentum Strong
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        ret5 = (
            df["close"]
            .ffill()
            .pct_change(5)
        )

        ret20 = (
            df["close"]
            .ffill()
            .pct_change(20)
        )

        mask = (
            ret5.notna()
            & ret20.notna()
        )

        result.loc[mask] = (
            (ret5.loc[mask] > 0)
            &
            (ret20.loc[mask] > 0)
        )

        df["momentum_strong"] = result

        # --------------------------------
        # Volatility Regime
        # 使用 raw ATR
        # --------------------------------
        vol_atr = (
            df["atr14"]
            / df["close"].replace(0, pd.NA)
        )

        atr_mean = (
            vol_atr
            .rolling(
                20,
                min_periods=20
            )
            .mean()
        )

        # low volatility
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            vol_atr.notna()
            & atr_mean.notna()
        )

        result.loc[mask] = (
            vol_atr.loc[mask]
            < atr_mean.loc[mask]
        )

        df["low_volatility"] = result

        # high volatility
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        result.loc[mask] = (
            vol_atr.loc[mask]
            > atr_mean.loc[mask] * 1.5
        )

        df["high_volatility"] = result

        # --------------------------------
        # Volume Spike
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        vol_base = df["vol_ma5"].replace(
            0,
            pd.NA
        )

        mask = (
            df["volume"].notna()
            & vol_base.notna()
        )

        result.loc[mask] = (
            df.loc[mask, "volume"]
            > vol_base.loc[mask] * 1.5
        )

        df["volume_spike"] = result

        # --------------------------------
        # Volume Trend
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["vol_ma5"].notna()
            & df["vol_ma10"].notna()
            & df["vol_ma20"].notna()
        )

        result.loc[mask] = (
            (df.loc[mask, "vol_ma5"]
             > df.loc[mask, "vol_ma10"])
            &
            (df.loc[mask, "vol_ma10"]
             > df.loc[mask, "vol_ma20"])
        )

        df["volume_trend"] = result

        # --------------------------------
        # Breakout Confirm
        # 修复 future leak
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        boll_up_prev = (
            df["boll_up"].shift(1)
        )

        mask = (
            df["close"].notna()
            & boll_up_prev.notna()
            & df["volume_spike"].notna()
            & df["acc_signal"].notna()
        )

        result.loc[mask] = (
            (df.loc[mask, "close"]
             > boll_up_prev.loc[mask])
            &
            (df.loc[mask, "volume_spike"])
            &
            (df.loc[mask, "acc_signal"])
        )

        df["breakout_confirm"] = result

        # --------------------------------
        # Reversal Signal
        # 修复 future leak
        # --------------------------------
        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        boll_down_prev = (
            df["boll_down"].shift(1)
        )

        mask = (
            df["rsi14"].notna()
            & df["close"].notna()
            & boll_down_prev.notna()
            & df["acc_signal"].notna()
        )

        result.loc[mask] = (
            (df.loc[mask, "rsi14"] < 30)
            &
            (df.loc[mask, "close"]
             < boll_down_prev.loc[mask])
            &
            (df.loc[mask, "acc_signal"])
        )

        df["reversal_signal"] = result

        _close = df["close"].ffill()

        df["ret"] = _close.pct_change()

        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        mask = (
            df["ret"].notna()
            & df["ret"].shift(1).notna()
            & df["ret"].shift(2).notna()
        )

        result.loc[mask] = (
            (df.loc[mask, "ret"] > 0)
            & (df.loc[mask, "ret"].shift(1) > 0)
            & (df.loc[mask, "ret"].shift(2) > 0)
        )

        df["up_3days"] = result

        result = pd.Series(
            pd.NA,
            index=df.index,
            dtype="boolean"
        )

        result.loc[mask] = (
            (df.loc[mask, "ret"] < 0)
            & (df.loc[mask, "ret"].shift(1) < 0)
            & (df.loc[mask, "ret"].shift(2) < 0)
        )

        df["down_3days"] = result

        # =========================
        # 清理临时列
        # =========================
        drop_cols = [
            "hhv20",
            "llv20",
            "atr_mean",
            "vol_ma5",
            "vol_ma20",
            "ret",
        ]

        df = df.drop(
            columns=drop_cols,
            errors="ignore"
        )

        return df

    def update_signals(
        self,
        region: Region | None = None
    ):

        # =========================
        # 获取需要更新的symbol
        # =========================
        if region is None:

            rows = self.db.read(
                """
                SELECT
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(s.date) AS signal_max_date
                FROM stock_daily d
                LEFT JOIN stock_signals s
                ON d.symbol = s.symbol
                GROUP BY d.symbol
                ORDER BY d.symbol
                """,
                fetch_mode="all"
            )

        else:

            rows = self.db.read(
                f"""
                SELECT
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(s.date) AS signal_max_date
                FROM stock_daily d
                LEFT JOIN stock_signals s
                ON d.symbol = s.symbol
                WHERE d.market = '{region.value.upper()}'
                GROUP BY d.symbol
                ORDER BY d.symbol
                """,
                fetch_mode="all"
            )

        symbols = []

        for symbol, daily_max, signal_max in rows:

            if (
                signal_max is None
                or daily_max > signal_max
            ):
                symbols.append(symbol)

        logger.info(
            f"Need update symbols: {len(symbols)}"
        )

        # =========================
        # 更新每个symbol
        # =========================
        count = 0
        total = len(symbols)
        for symbol in symbols:

            logger.info(
                f"Updating signals({count}/{total}): {symbol}"
            )
            count += 1

            # =========================
            # 获取signals最后日期
            # =========================
            result = self.db.read(
                f"""
                SELECT date
                FROM stock_signals
                WHERE symbol = '{symbol}'
                ORDER BY date DESC
                LIMIT 1
                """,
                fetch_mode="one"
            )

            last_date = (
                result[0]
                if result and result[0] is not None
                else None
            )

            # =========================
            # 拉取最近200 bars
            # 避免rolling窗口不足
            # =========================
            if last_date is None:

                df = self.db.read(
                    f"""
                    SELECT
                        d.symbol,
                        d.date,
                        d.open,
                        d.high,
                        d.low,
                        d.close,
                        d.volume,

                        i.ma5,
                        i.ma20,
                        i.ma60,

                        i.rsi14,

                        i.atr14,

                        i.boll_up,
                        i.boll_down

                    FROM stock_daily d
                    JOIN stock_indicators i
                    ON d.symbol = i.symbol
                    AND d.date = i.date

                    WHERE d.symbol = '{symbol}'

                    ORDER BY d.date
                    """,
                    fetch_mode="df"
                )

            else:

                df = self.db.read(
                    f"""
                    SELECT *
                    FROM (

                        SELECT
                            d.symbol,
                            d.date,
                            d.open,
                            d.high,
                            d.low,
                            d.close,
                            d.volume,

                            i.ma5,
                            i.ma20,
                            i.ma60,

                            i.rsi14,

                            i.atr14,

                            i.boll_up,
                            i.boll_down

                        FROM stock_daily d
                        JOIN stock_indicators i
                        ON d.symbol = i.symbol
                        AND d.date = i.date

                        WHERE d.symbol = '{symbol}'

                        ORDER BY d.date DESC
                        LIMIT 200
                    )
                    ORDER BY date
                    """,
                    fetch_mode="df"
                )

            if df.empty:

                logger.warning(
                    f"{symbol}: no data"
                )

                continue

            # =========================
            # 计算signals
            # =========================
            df = self.calculate_signals(df)

            # =========================
            # 只保留新增部分
            # =========================
            if last_date is not None:

                df = df[
                    df["date"] > last_date
                ]

            if df.empty:

                logger.info(
                    f"{symbol}: no new rows"
                )

                continue

            # =========================
            # 保留字段
            # =========================
            df = df[
                [
                    "symbol",
                    "date",

                    "ma5_above_ma20",
                    "ma20_above_ma60",
                    "close_above_ma20",

                    "rsi_overbought",
                    "rsi_oversold",

                    "breakout_20d",
                    "breakdown_20d",

                    "atr_high_vol",

                    "boll_upper_break",
                    "boll_lower_break",

                    "vol_spike",
                    "vol_ma5_above_ma20",

                    "up_3days",
                    "down_3days",

                    "acc_signal",
                    "trend_strong",
                    "trend_weak",
                    "momentum_strong",
                    "low_volatility",
                    "high_volatility",
                    "volume_spike",
                    "volume_trend",
                    "breakout_confirm",
                    "reversal_signal",
                ]
            ]

            inserted_rows = len(df)

            # =========================
            # INSERT OR REPLACE
            # 修复历史错误
            # =========================
            sql = """
                INSERT OR REPLACE INTO stock_signals (

                    symbol,
                    date,

                    ma5_above_ma20,
                    ma20_above_ma60,
                    close_above_ma20,

                    rsi_overbought,
                    rsi_oversold,

                    breakout_20d,
                    breakdown_20d,

                    atr_high_vol,

                    boll_upper_break,
                    boll_lower_break,

                    vol_spike,
                    vol_ma5_above_ma20,

                    up_3days,
                    down_3days,

                    acc_signal,
                    trend_strong,
                    trend_weak,
                    momentum_strong,
                    low_volatility,
                    high_volatility,
                    volume_spike,
                    volume_trend,
                    breakout_confirm,
                    reversal_signal,

                    create_time,
                    update_time
                )

                SELECT

                    symbol,
                    date,

                    ma5_above_ma20,
                    ma20_above_ma60,
                    close_above_ma20,

                    rsi_overbought,
                    rsi_oversold,

                    breakout_20d,
                    breakdown_20d,

                    atr_high_vol,

                    boll_upper_break,
                    boll_lower_break,

                    vol_spike,
                    vol_ma5_above_ma20,

                    up_3days,
                    down_3days,

                    acc_signal,
                    trend_strong,
                    trend_weak,
                    momentum_strong,
                    low_volatility,
                    high_volatility,
                    volume_spike,
                    volume_trend,
                    breakout_confirm,
                    reversal_signal,

                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP

                FROM temp_df
            """

            self.db.write(
                df,
                sql=sql,
                view_name="temp_df"
            )

            logger.info(
                f"{symbol} updated rows: "
                f"{inserted_rows}"
            )

    def update_by_markets(self):

        for region in Region:

            logger.info(
                f"Updating region: "
                f"{region.value.upper()}"
            )

            self.update_signals(region)

        logger.info(
            "Finished all signals update"
        )


def run_signals(
    db: DuckDBController,
    market: str | None = None
):

    logger.info("=" * 60)
    logger.info(
        "[Step 2/3] "
        "Start calculating signals..."
    )
    logger.info("=" * 60)

    updater = SignalsUpdater(db)

    if market:

        region = Region(
            market.lower()
        )

        updater.update_signals(region)

    else:

        updater.update_by_markets()

    logger.info(
        "[Step 2/3] "
        "stock_signals finished"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "统一 Signal 计算 "
            "- 增量更新版"
        )
    )

    parser.add_argument(
        "--market",
        "-m",
        type=str,
        default=None,
        choices=["cn", "hk", "us"],
        help="只更新指定市场"
    )

    args = parser.parse_args()

    logger.info("🚀 Start signal updater")
    logger.info(f"DB Path: {DB}")

    logger.info(
        f"Market: {args.market or 'ALL'}"
    )

    db = DuckDBController(DB)

    run_signals(db, args.market)