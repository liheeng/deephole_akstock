import duckdb
from core.normalizer import normalize
from utils.retry import retry
from utils.time import random_sleep
from utils.log_manager import get_task_logger
import pandas as pd
from datetime import date

class Updater:

    def __init__(self, db_path):
        self.db_path = db_path

    def get_last_date(self, con, symbol):
        r = con.execute(
            "SELECT max(date) FROM stock_daily WHERE symbol=?",
            [symbol]
        ).fetchone()[0]
        return r

    @retry(3)
    def fetch(self, source, symbol, start):
        return source.fetch_daily(symbol, start)

    def run(self, market):

        con = duckdb.connect(self.db_path)

        source = market.get_source()
        symbols = market.get_symbol_list()

        _today = date.today()
        for symbol in symbols:

            last = self.get_last_date(con, symbol)
            
            # Skip if last date is today
            if last and last.date() == _today:
                get_task_logger().info(f"{symbol} already updated today, skipping")
                continue
            
            start = last.strftime("%Y%m%d") if last else "19900101"

            df = self.fetch(source, symbol, start)

            if df is None or len(df) == 0:
                continue
            
            df = normalize(df, symbol, market.name)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")  # Ensure date is in the correct format
            
            number_of_rows = len(df)

            # Register the DataFrame as a temporary table in DuckDB
            con.register('temp_df', df)

            # Insert from the temporary table
            sql = """
            INSERT OR IGNORE INTO stock_daily (
                symbol, market, date, open, high, low, close, volume, amount, pct, turnover
            ) 
            SELECT symbol, market, date, open, high, low, close, volume, amount, pct, turnover FROM temp_df
            """

            result = con.execute(sql)
            
            inserted = result.rowcount if result.rowcount >= 0 else number_of_rows # 👈 关键
            market_name = market.name
            get_task_logger().info(f"{market_name}-{symbol} inserted {inserted} rows")

            random_sleep()

        con.close()
