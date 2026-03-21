import duckdb
from core.normalizer import normalize
from utils.retry import retry
from utils.time import random_sleep
from utils.log_manager import get_default_logger
import pandas as pd
from datetime import date
from db.duckdb import DuckDBController

class Updater:

    def __init__(self):
        self.dbc = DuckDBController()
  
    def get_last_date(self, symbol):
        r = self.dbc.execute(
            "SELECT max(date) FROM stock_daily WHERE symbol=?", 
            [symbol],
            callback= lambda res: res.fetchone()[0] if res else None)
        return r if r else None

    @retry(3)
    def fetch(self, source, symbol, start):
        return source.fetch_daily(symbol, start)

    def run(self, market):

        # con = duckdb.connect(self.db_path)

        source = market.get_source()
        symbols = market.get_symbol_list()

        _today = date.today()
        for symbol in symbols:

            last = self.get_last_date(symbol)
            
            # Skip if last date is today
            if last and last == _today:
                get_default_logger().info(f"{symbol} already updated today, skipping")
                continue
            
            start = last.strftime("%Y%m%d") if last else "19900101" # type: ignore

            df = self.fetch(source, symbol, start)

            if df is None or len(df) == 0:
                continue
            
            df = normalize(df, symbol, market.name)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")  # Ensure date is in the correct format
            
            number_of_rows = len(df)

            # Register the DataFrame as a temporary table in DuckDB
            # con.register('temp_df', df)

            # Insert from the temporary table
            sql = """
            INSERT OR IGNORE INTO stock_daily (
                symbol, market, date, open, high, low, close, volume, amount, pct, turnover
            ) 
            SELECT symbol, market, date, open, high, low, close, volume, amount, pct, turnover FROM temp_df
            """

            result = self.dbc.write(
                df, sql=sql, view_name="temp_df", if_exists="append"
            )
            # result = con.execute(sql)
            
            inserted = result.rowcount if result.rowcount >= 0 else number_of_rows # 👈 关键
            market_name = market.name
            get_default_logger().info(f"{market_name}-{symbol} inserted {inserted} rows")

            random_sleep()

        # con.lose()
