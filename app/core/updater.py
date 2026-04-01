import pandas as pd
from datetime import date, datetime

from core.job import Job
from core.normalizer import normalize
from utils.retry import retry
from utils.time import random_sleep
from utils.log_manager import get_logger
from db.duckdb import DuckDBController
from markets.market import Market
from sources.data_source import QueryOptions, FetchResult, DataSource, HIS_BATCH_SIZE_LIMIT, HIS_BATCH_SYMBOLS_LIMIT
from utils.common import ResultStatus

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
    def fetch(self, market_name: str, source: DataSource, symbols: list, start: datetime, end: datetime | None = None) -> tuple[pd.DataFrame | None, list[str] | None]:
        symbols_str = ",".join(symbols)
        options = QueryOptions(start=start, end=end)
        fetch_result: FetchResult | None = source.fetch_daily(symbols_str, options)
        if fetch_result is None:
            raise ValueError("fetch result is None") 
        
        fetched_data = fetch_result.data
        total_df: pd.DataFrame | None = None
        if isinstance(fetched_data, dict):    
            for symbol, df in fetched_data.items():
                df = normalize(df, symbol, market_name)
                total_df = df if total_df is None else pd.concat([total_df, df])
        elif isinstance(fetched_data, pd.DataFrame):
            total_df = normalize(fetched_data, symbols[0], market_name)
        
        failed_symbols = None
        if fetch_result.status != ResultStatus.SUCCESS:
            logger.error(f"some symbols are filed to fetch, message: {fetch_result.message}, failed_symbols: {fetch_result.failed_symbols}") if fetch_result.failed_symbols else logger.error(fetch_result.message)
            failed_symbols = fetch_result.failed_symbols
        
        if total_df is not None and len(total_df) > 0:
            total_df["date"] = pd.to_datetime(total_df["date"]).dt.strftime("%Y-%m-%d")  # Ensure date is in the correct format
        
        return total_df, failed_symbols
        
    def run(self, market: Market, job: Job):

        # con = duckdb.connect(self.db_path)

        source: DataSource = market.get_source()
        symbols = market.get_symbol_list()
        symbol_len = len(symbols)
        _today = date.today()

        end = datetime.now()
        next = 0
        while next < symbol_len:
            
            # 计算一次取多少数据
            last = self.get_last_date(symbols[next])  
            if last and last == _today:
                logger.info(f"{symbols[next]} already updated today, skipping")
                next += 1
                continue
            
            start = last.strftime("%Y%m%d") if last else "1990-01-01"  # type: ignore
            start = pd.to_datetime(start)
            start_index = next
            end_index = self.calculate_range(symbols, start_index, start, end)
            next = end_index

            select_symbols = []
            for symbol in symbols[start_index:end_index]:
                select_symbols.append(symbol)
                
            total_df, failed_symbols = self.fetch(market_name=market.name, source=source, symbols=select_symbols, start=start, end=end)
            success_symbols = select_symbols if failed_symbols is None else [symbol for symbol in select_symbols if symbol not in failed_symbols]
            
            # TODO ...
            # Need to save failed_symbols to DB or have a retry mechanism!!!

            if total_df is None or len(total_df) == 0:
                continue
            
            total_df = total_df.drop(columns=["update_time"], errors="ignore")
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
            logger.info(f"{market.name}-{success_symbols} \n -- inserted {inserted} rows")

            random_sleep()

    def calculate_range(self, symbols, start_index, start, end):
        _days = (end.date() - start.date()).days
        _days = 1 if _days == 0 else _days
        count = HIS_BATCH_SIZE_LIMIT // _days
        count = HIS_BATCH_SYMBOLS_LIMIT if count > HIS_BATCH_SYMBOLS_LIMIT else count
        end_index = start_index + count
        if end_index > len(symbols):
            end_index = len(symbols)
        return end_index
