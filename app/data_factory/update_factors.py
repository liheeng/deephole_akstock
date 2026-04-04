import pandas as pd
from markets.market import Region
from db.duckdb import DuckDBController
from utils.log_manager import get_logger

logger = get_logger(__name__)


class FactorUpdater():
    def __init__(self, db: DuckDBController):
        self.db = db

    def calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date")

        # ===== 趋势 =====
        df["ma5_above_ma20"] = df["ma5"] > df["ma20"]
        df["ma20_above_ma60"] = df["ma20"] > df["ma60"]
        df["close_above_ma20"] = df["close"] > df["ma20"]

        # ===== RSI =====
        df["rsi_overbought"] = df["rsi14"] > 70
        df["rsi_oversold"] = df["rsi14"] < 30

        # ===== 突破（20日高低）=====
        df["hhv20"] = df["high"].rolling(20).max()
        df["llv20"] = df["low"].rolling(20).min()

        df["breakout_20d"] = df["close"] >= df["hhv20"]
        df["breakdown_20d"] = df["close"] <= df["llv20"]

        # ===== 波动 =====
        df["atr_mean"] = df["atr14"].rolling(20).mean()
        df["atr_high_vol"] = df["atr14"] > df["atr_mean"]

        df["boll_upper_break"] = df["close"] > df["boll_up"]
        df["boll_lower_break"] = df["close"] < df["boll_down"]

        # ===== 成交量 =====
        df["vol_ma5"] = df["volume"].rolling(5).mean()
        df["vol_ma20"] = df["volume"].rolling(20).mean()

        df["vol_spike"] = df["volume"] > df["vol_ma5"] * 1.5
        df["vol_ma5_above_ma20"] = df["vol_ma5"] > df["vol_ma20"]

        # ===== 连续上涨/下跌 =====
        df["ret"] = df["close"].pct_change()

        df["up_3days"] = (
            (df["ret"] > 0)
            & (df["ret"].shift(1) > 0)
            & (df["ret"].shift(2) > 0)
        )

        df["down_3days"] = (
            (df["ret"] < 0)
            & (df["ret"].shift(1) < 0)
            & (df["ret"].shift(2) < 0)
        )

        return df

    def update_factors(self, region: Region | None = None):
        if region is None:
            rows = self.db.read(
                """SELECT 
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(i.date) AS ind_max_date
                FROM stock_daily d
                LEFT JOIN stock_factors i
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
                LEFT JOIN stock_factors i
                ON d.symbol = i.symbol
                WHERE  d.market = '{region.value.upper()}'
                GROUP BY d.symbol
                """, fetch_mode="all")
        # symbols = [s[0] for s in symbols]

        symbols = []
        for symbol, daily_max, ind_max in rows:
            if ind_max is None:
                # print(symbol, "从头计算 factor")
                symbols.append(symbol)

            elif daily_max > ind_max:
                # print(symbol, "增量计算 factor")
                symbols.append(symbol)

        for symbol in symbols:
            logger.info(f"factor update: {symbol}")

            # 1️⃣ factor 表最后日期
            result = self.db.read(f"""
                SELECT MAX(date)
                FROM stock_factors
                WHERE symbol = '{symbol}'
            """, fetch_mode="one")[0]

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
                    JOIN stock_indicators i
                    ON d.symbol = i.symbol AND d.date = i.date
                    WHERE d.symbol = '{symbol}'
                    ORDER BY d.date
                """, fetch_mode="df")
            else:
                df = self.db.read(f"""
                    SELECT d.*, i.*
                    FROM stock_daily d
                    JOIN stock_indicators i
                    ON d.symbol = i.symbol AND d.date = i.date
                    WHERE d.symbol = '{symbol}'
                    AND d.date >= DATE '{last_date}' - INTERVAL 60 DAY
                    ORDER BY d.date
                """, fetch_mode="df")

            if len(df) == 0:
                continue

            df = self.calculate_factors(df)

            # 3️⃣ 只保留新增
            if last_date is not None:
                df = df[df["date"] > last_date]

            if len(df) == 0:
                continue

            df = df[[
                "symbol", "date",
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
                "down_3days"
            ]]

            rows = len(df)

            sql = """
                INSERT OR IGNORE INTO stock_factors (
                    symbol, date,
                    ma5_above_ma20, ma20_above_ma60, close_above_ma20,
                    rsi_overbought, rsi_oversold,
                    breakout_20d, breakdown_20d,
                    atr_high_vol,
                    boll_upper_break, boll_lower_break,
                    vol_spike, vol_ma5_above_ma20,
                    up_3days, down_3days
                )
                SELECT symbol, date, ma5_above_ma20, ma20_above_ma60, close_above_ma20, rsi_overbought, rsi_oversold, breakout_20d, breakdown_20d, atr_high_vol, boll_upper_break, boll_lower_break, vol_spike, vol_ma5_above_ma20, up_3days, down_3days FROM temp_df
            """
            df = self.db.write(
                df, sql=sql, view_name="temp_df"
            )

            logger.info(f"Updated factors for {symbol} - inserted rows: {rows}")

    def update_by_markets(self):
        for region in Region:
            logger.info(f"Updating factors for region: {region.value.upper()}")
            self.update_factors(region)

        logger.info("Finished updating factors for all regions")


if __name__ == "__main__":
    db = DuckDBController("../data/stock.duckdb")
    updater = FactorUpdater(db)
    updater.update_by_markets()
