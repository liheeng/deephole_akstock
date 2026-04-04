import pandas as pd
from markets.market import Region
from db.duckdb import DuckDBController
from utils.log_manager import get_logger

logger = get_logger(__name__)


class IndicatorUpdater():
    def __init__(self, db: DuckDBController):
        self.db = db

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date")

        # ===== MA =====
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma10"] = df["close"].rolling(10).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma60"] = df["close"].rolling(60).mean()
        df["ma120"] = df["close"].rolling(120).mean()

        # ===== RSI =====
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["rsi14"] = 100 - (100 / (1 + rs))

        # ===== ATR =====
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["atr14"] = tr.rolling(14).mean()

        # ===== Bollinger =====
        mid = df["close"].rolling(20).mean()
        std = df["close"].rolling(20).std()
        df["boll_mid"] = mid
        df["boll_up"] = mid + 2 * std
        df["boll_down"] = mid - 2 * std

        return df

    def update_indicators(self, region: Region | None = None):
        if region is None:
            rows = self.db.read(
                """SELECT 
                    d.symbol,
                    MAX(d.date) AS daily_max_date,
                    MAX(i.date) AS ind_max_date
                FROM stock_daily d
                LEFT JOIN stock_indicators i
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
                LEFT JOIN stock_indicators i
                ON d.symbol = i.symbol
                WHERE  d.market = '{region.value.upper()}'
                GROUP BY d.symbol
                """, fetch_mode="all")

        symbols = []
        for symbol, daily_max, ind_max in rows:
            if ind_max is None:
                # print(symbol, "从头计算 indicator")
                symbols.append(symbol)

            elif daily_max > ind_max:
                # print(symbol, "增量计算 indicator")
                symbols.append(symbol)

            # else:
            #     print(symbol, "无需更新")

        for symbol in symbols:
            logger.info(f"update: {symbol}")

            # 1. indicator 表中最后日期
            result = self.db.read(f"""
                SELECT MAX(date)
                FROM stock_indicators
                WHERE symbol = '{symbol}'
            """, fetch_mode="one")

            # 2. Python层：安全取值
            if result and result[0] is not None:
                last_date = result[0]
            else:
                last_date = None  # 或设置初始日期，如'1970-01-01'

            if last_date is None:
                # 新股票，取全部
                df = self.db.read(f"""
                    SELECT *
                    FROM stock_daily
                    WHERE symbol = '{symbol}'
                    ORDER BY date
                """, fetch_mode="df")
            else:
                # 取最近 200 天 + 新数据（用于 rolling）
                df = self.db.read(f"""
                    SELECT *
                    FROM stock_daily
                    WHERE symbol = '{symbol}'
                    AND date >= DATE '{last_date}' - INTERVAL 200 DAY
                    ORDER BY date
                """, fetch_mode="df")

            if len(df) == 0:
                continue

            df = self.calculate_indicators(df)

            # 只保留新增部分
            if last_date is not None:
                df = df[df["date"] > last_date]

            if len(df) == 0:
                continue

            df = df[[
                "symbol", "date",
                "ma5", "ma10", "ma20", "ma60", "ma120",
                "rsi14", "atr14",
                "boll_mid", "boll_up", "boll_down"
            ]]
            rows = len(df)

            sql = """
                INSERT OR IGNORE INTO stock_indicators (
                    symbol, date,
                    ma5, ma10, ma20, ma60, ma120,
                    rsi14, atr14,
                    boll_mid, boll_up, boll_down
                )
                SELECT symbol, date, ma5, ma10, ma20, ma60, ma120, rsi14, atr14, boll_mid, boll_up, boll_down FROM temp_df
            """
            df = self.db.write(
                df, sql=sql, view_name="temp_df"
            )

            logger.info(f"Updated indicators for {symbol} - inserted rows: {rows}")

    def update_by_markets(self):
        for region in Region:
            logger.info(f"Updating indicators for region: {region.value.upper()}")
            self.update_indicators(region)

        logger.info("Finished updating indicators for all regions")


if __name__ == "__main__":

    db = DuckDBController("../data/stock.duckdb")
    updater = IndicatorUpdater(db)
    updater.update_by_markets()

