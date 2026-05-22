import argparse

import pandas as pd
from loguru import logger

from db.db_common import DB
from db.duckdb import DuckDBController
from markets.market import Region


class IndicatorUpdater:
    def __init__(self, db: DuckDBController):
        self.db = db

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有技术指标（增强版）
        修复：
        - RSI 0/0
        - KDJ 分母为0
        - 中间 NULL 传播
        - 脏数据
        - rolling 窗口问题
        """

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

        df[numeric_cols] = df[numeric_cols].apply(
            pd.to_numeric,
            errors="coerce"
        )

        # 删除关键字段缺失
        df = df.dropna(subset=["close", "high", "low"])

        # volume 缺失填0
        df["volume"] = df["volume"].fillna(0)

        # 避免价格为0
        df.loc[df["close"] <= 0, "close"] = pd.NA
        df.loc[df["high"] <= 0, "high"] = pd.NA
        df.loc[df["low"] <= 0, "low"] = pd.NA

        df = df.dropna(subset=["close", "high", "low"])

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        # =========================
        # MA
        # =========================
        df["ma5"] = close.rolling(5, min_periods=5).mean()
        df["ma10"] = close.rolling(10, min_periods=10).mean()
        df["ma20"] = close.rolling(20, min_periods=20).mean()
        df["ma60"] = close.rolling(60, min_periods=60).mean()
        df["ma120"] = close.rolling(120, min_periods=120).mean()

        # =========================
        # EMA
        # =========================
        df["ema12"] = close.ewm(span=12, adjust=False).mean()
        df["ema26"] = close.ewm(span=26, adjust=False).mean()

        # =========================
        # MACD
        # =========================
        df["macd"] = df["ema12"] - df["ema26"]
        df["macd_signal"] = df["macd"].ewm(
            span=9,
            adjust=False
        ).mean()

        df["macd_hist"] = (
            df["macd"] - df["macd_signal"]
        )

        # =========================
        # RSI14
        # 修复 0/0 NULL
        # =========================
        delta = close.diff()

        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(
            14,
            min_periods=14
        ).mean()

        avg_loss = loss.rolling(
            14,
            min_periods=14
        ).mean()

        rs = avg_gain / avg_loss.replace(0, pd.NA)

        df["rsi14"] = 100 - (
            100 / (1 + rs)
        )

        # 特殊情况
        df.loc[
            (avg_loss == 0) & (avg_gain > 0),
            "rsi14"
        ] = 100

        df.loc[
            (avg_gain == 0) & (avg_loss > 0),
            "rsi14"
        ] = 0

        # =========================
        # ATR14
        # =========================
        high_low = high - low

        high_close = (
            high - close.shift()
        ).abs()

        low_close = (
            low - close.shift()
        ).abs()

        tr = pd.concat(
            [high_low, high_close, low_close],
            axis=1
        ).max(axis=1)

        df["atr14"] = tr.rolling(
            14,
            min_periods=14
        ).mean()

        # =========================
        # Bollinger
        # =========================
        mid = close.rolling(
            20,
            min_periods=20
        ).mean()

        std = close.rolling(
            20,
            min_periods=20
        ).std()

        df["boll_mid"] = mid
        df["boll_up"] = mid + 2 * std
        df["boll_down"] = mid - 2 * std

        # =========================
        # KDJ
        # 修复 high_n == low_n
        # =========================
        low_n = low.rolling(
            9,
            min_periods=9
        ).min()

        high_n = high.rolling(
            9,
            min_periods=9
        ).max()

        denom = (high_n - low_n)

        rsv = (
            (close - low_n)
            / denom.replace(0, pd.NA)
        ) * 100

        # 无波动时默认50
        rsv = rsv.fillna(50)

        df["k"] = rsv.ewm(
            alpha=1 / 3,
            adjust=False
        ).mean()

        df["d"] = df["k"].ewm(
            alpha=1 / 3,
            adjust=False
        ).mean()

        df["j"] = (
            3 * df["k"] - 2 * df["d"]
        )

        # =========================
        # Volume MA
        # =========================
        df["vol_ma5"] = volume.rolling(
            5,
            min_periods=5
        ).mean()

        df["vol_ma10"] = volume.rolling(
            10,
            min_periods=10
        ).mean()

        df["vol_ma20"] = volume.rolling(
            20,
            min_periods=20
        ).mean()

        # =========================
        # OBV
        # =========================
        direction = close.diff().apply(
            lambda x:
            1 if x > 0 else (
                -1 if x < 0 else 0
            )
        )

        df["obv"] = (
            (direction * volume)
            .fillna(0)
            .cumsum()
        )

        # =========================
        # Returns
        # =========================
        _close = close.ffill()

        df["ret_1d"] = _close.pct_change(1)
        df["ret_5d"] = _close.pct_change(5)
        df["ret_20d"] = _close.pct_change(20)

        # =========================
        # Price Position
        # =========================
        ma20_safe = df["ma20"].replace(
            0,
            pd.NA
        )

        df["pct_from_ma20"] = (
            (close - ma20_safe)
            / ma20_safe
        )

        # =========================
        # inf -> NULL
        # =========================
        df = df.replace(
            [float("inf"), float("-inf")],
            pd.NA
        )

        return df

    def update_indicators(
        self,
        region: Region | None = None
    ):

        if region is None:
            rows = self.db.read(
                """
                SELECT
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(i.date) AS ind_max_date
                FROM stock_daily d
                LEFT JOIN stock_indicators i
                ON d.symbol = i.symbol
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
                    MAX(i.date) AS ind_max_date
                FROM stock_daily d
                LEFT JOIN stock_indicators i
                ON d.symbol = i.symbol
                WHERE d.market = '{region.value.upper()}'
                GROUP BY d.symbol
                ORDER BY d.symbol
                """,
                fetch_mode="all"
            )

        symbols = []

        for symbol, daily_max, ind_max in rows:
            if ind_max is None or daily_max > ind_max:
                symbols.append(symbol)

        logger.info(
            f"Need update symbols: {len(symbols)}"
        )

        for symbol in symbols:

            logger.info(f"Updating: {symbol}")

            # =========================
            # indicators 最后日期
            # =========================
            result = self.db.read(
                f"""
                SELECT date
                FROM stock_indicators
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
            # 读取最近300根K线
            # 避免窗口不足
            # =========================
            if last_date is None:

                df = self.db.read(
                    f"""
                    SELECT *
                    FROM stock_daily
                    WHERE symbol = '{symbol}'
                    ORDER BY date
                    """,
                    fetch_mode="df"
                )

            else:

                df = self.db.read(
                    f"""
                    SELECT *
                    FROM (
                        SELECT *
                        FROM stock_daily
                        WHERE symbol = '{symbol}'
                        ORDER BY date DESC
                        LIMIT 300
                    )
                    ORDER BY date
                    """,
                    fetch_mode="df"
                )

            if len(df) == 0:
                logger.warning(
                    f"{symbol}: no daily data"
                )
                continue

            # =========================
            # 计算指标
            # =========================
            df = self.calculate_indicators(df)

            # =========================
            # 仅保留新增数据
            # =========================
            if last_date is not None:
                df = df[
                    df["date"] > last_date
                ]

            if len(df) == 0:
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

                    "ma5",
                    "ma10",
                    "ma20",
                    "ma60",
                    "ma120",

                    "ema12",
                    "ema26",

                    "macd",
                    "macd_signal",
                    "macd_hist",

                    "rsi14",
                    "atr14",

                    "k",
                    "d",
                    "j",

                    "boll_mid",
                    "boll_up",
                    "boll_down",

                    "vol_ma5",
                    "vol_ma10",
                    "vol_ma20",

                    "obv",

                    "ret_1d",
                    "ret_5d",
                    "ret_20d",

                    "pct_from_ma20",
                ]
            ]

            inserted_rows = len(df)

            # =========================
            # REPLACE
            # 修复历史NULL
            # =========================
            sql = """
                INSERT OR REPLACE INTO stock_indicators (
                    symbol,
                    date,

                    ma5,
                    ma10,
                    ma20,
                    ma60,
                    ma120,

                    ema12,
                    ema26,

                    macd,
                    macd_signal,
                    macd_hist,

                    rsi14,
                    atr14,

                    k,
                    d,
                    j,

                    boll_mid,
                    boll_up,
                    boll_down,

                    vol_ma5,
                    vol_ma10,
                    vol_ma20,

                    obv,

                    ret_1d,
                    ret_5d,
                    ret_20d,

                    pct_from_ma20,

                    create_time,
                    update_time
                )
                SELECT
                    symbol,
                    date,

                    ma5,
                    ma10,
                    ma20,
                    ma60,
                    ma120,

                    ema12,
                    ema26,

                    macd,
                    macd_signal,
                    macd_hist,

                    rsi14,
                    atr14,

                    k,
                    d,
                    j,

                    boll_mid,
                    boll_up,
                    boll_down,

                    vol_ma5,
                    vol_ma10,
                    vol_ma20,

                    obv,

                    ret_1d,
                    ret_5d,
                    ret_20d,

                    pct_from_ma20,

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

            self.update_indicators(region)

        logger.info(
            "Finished all markets update"
        )


def run_indicators(
    db: DuckDBController,
    market: str | None = None
):

    logger.info("=" * 60)
    logger.info(
        "[Step 1/3] "
        "Start calculating indicators..."
    )
    logger.info("=" * 60)

    updater = IndicatorUpdater(db)

    if market:

        region = Region(market.lower())

        updater.update_indicators(region)

    else:

        updater.update_by_markets()

    logger.info(
        "[Step 1/3] "
        "stock_indicators finished"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "统一指标计算 "
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

    logger.info("🚀 Start indicator updater")
    logger.info(f"DB Path: {DB}")
    logger.info(
        f"Market: {args.market or 'ALL'}"
    )

    db = DuckDBController(DB)

    run_indicators(db, args.market)