import pandas as pd
from typing import Dict
from datetime import date, datetime

from core.normalizer import normalize
from utils.retry import retry
from utils.time import random_sleep
from utils.log_manager import get_logger
from db.duckdb import DuckDBController
from markets.market import Market
from sources.data_source import DataSource

logger = get_logger(__name__)


class Updater:

    def __init__(self):
        self.dbc = DuckDBController()
  
    def get_last_date(self, symbol):
        r = self.dbc.execute(
            "SELECT max(date) FROM stock_daily WHERE symbol=?", 
            [symbol],
            fetch_mode="one")
            # callback= lambda res: res.fetchone()[0] if res else None)
        return r[0] if r else None

    @retry(3)
    def fetch(self, market_name: str, source: DataSource, symbols, next: int, start: datetime, end: datetime | None = None) -> tuple[int, str, pd.DataFrame | None]:
        next, candidate_symbols = source.candidate_symbols(symbols, next, start, end)
        fetched_data = source.fetch_daily(candidate_symbols, start)

        total_df: pd.DataFrame | None = None
        if isinstance(fetched_data, dict):    
            for symbol, df in fetched_data.items():
                df = normalize(df, symbol, market_name)
                total_df = df if total_df is None else pd.concat([total_df, df])
        elif isinstance(fetched_data, pd.DataFrame):
            total_df = normalize(fetched_data, candidate_symbols, market_name)
        elif fetched_data is not None:
            raise ValueError(f"Unexpected result type: {type(fetched_data)}")
        
        if total_df is not None and len(total_df) > 0:
            total_df["date"] = pd.to_datetime(total_df["date"]).dt.strftime("%Y-%m-%d")  # Ensure date is in the correct format
        
        return next, candidate_symbols, total_df
        
            
    def run(self, market: Market):

        # con = duckdb.connect(self.db_path)

        source: DataSource = market.get_source()
        symbols = market.get_symbol_list()
        symbol_len = len(symbols)
        _today = date.today()

        next = 0
        while next < symbol_len:
            # Reset property values of source for next.
            source.prepare_fetch()

            last = self.get_last_date(symbols[next])  
            # Skip if last date is today
            if last and last == _today:
                logger.info(f"{symbols[next]} already updated today, skipping")
                continue

            start = last.strftime("%Y%m%d") if last else "1990-01-01"  # type: ignore
            start = pd.to_datetime(start)

            next, fetch_symbols, total_df = self.fetch(market_name=market.name, source=source, symbols=symbols, next=next, start=start, end=pd.to_datetime(_today))

            if total_df is None or len(total_df) == 0:
                continue
            
            number_of_rows = len(total_df)

            # Insert from the temporary table
            sql = """
            INSERT OR IGNORE INTO stock_daily (
                symbol, market, date, open, high, low, close, volume, amount, pct, turnover
            ) 
            SELECT symbol, market, date, open, high, low, close, volume, amount, pct, turnover FROM temp_df
            """

            df = self.dbc.write(
                total_df, sql=sql, view_name="temp_df", if_exists="append"
            )
            # result = con.execute(sql)
            
            inserted = df.rowcount if df.rowcount >= 0 else number_of_rows   # 👈 关键
            logger.info(f"{market.name}-{fetch_symbols} inserted {inserted} rows")

            random_sleep()

        # for symbol in symbols:

        #     last = self.get_last_date(symbol)
            
        #     # Skip if last date is today
        #     if last and last == _today:
        #         logger.info(f"{symbol} already updated today, skipping")
        #         continue
            
        #     start = last.strftime("%Y%m%d") if last else "1990-01-01" # type: ignore
        #     start = pd.to_datetime(start)

        #     df = self.fetch(source, symbol, start)

        #     if df is None or len(df) == 0:
        #         continue
            
        #     df = normalize(df, symbol, market.name)
        #     df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")  # Ensure date is in the correct format
            
        #     number_of_rows = len(df)

        #     # Register the DataFrame as a temporary table in DuckDB
        #     # con.register('temp_df', df)

        #     # Insert from the temporary table
        #     sql = """
        #     INSERT OR IGNORE INTO stock_daily (
        #         symbol, market, date, open, high, low, close, volume, amount, pct, turnover
        #     ) 
        #     SELECT symbol, market, date, open, high, low, close, volume, amount, pct, turnover FROM temp_df
        #     """

        #     result = self.dbc.write(
        #         df, sql=sql, view_name="temp_df", if_exists="append"
        #     )
        #     # result = con.execute(sql)
            
        #     inserted = result.rowcount if result.rowcount >= 0 else number_of_rows # 👈 关键
        #     market_name = market.name
        #     logger.info(f"{market_name}-{symbol} inserted {inserted} rows")

        #     random_sleep()

        # con.lose()
