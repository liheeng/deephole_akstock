import requests
from typing import List
import pandas as pd
from sources.data_source import DataSource, DataSourceApiName
from sources.us_datasource import USStockSource
from markets.market import SymbolType
from markets.market import Region, Market
from loguru import logger
import akshare as ak
from db.duckdb import DuckDBController



NYSE_LIST_FILE = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.json"
NASDAQ_LIST_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json"
AMEX_LIST_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_full_tickers.json"

db = DuckDBController()


class USStockMarket(Market):

    region: Region = Region.US
    name: str = region.value.upper()

    def get_nyse_symbol_list(self):
        nyse_symbol_list = requests.get(NYSE_LIST_FILE).json()

        # 直接转 DataFrame（超级简单）
        nyse_symbol_df = pd.DataFrame(nyse_symbol_list)

        df = [
            f"{code}.NYSE" for code in nyse_symbol_df["symbol"]
        ]
        logger.info(f"Fetched {len(df)} symbols from NYSE")

        return df

    def get_nasdaq_symbol_list(self):
        nasdaq_symbol_list = requests.get(NASDAQ_LIST_URL).json()

        # 直接转 DataFrame（超级简单）
        nasdaq_symbol_df = pd.DataFrame(nasdaq_symbol_list)

        df = [
            f"{code}.NASDAQ" for code in nasdaq_symbol_df["symbol"]
        ]
        logger.info(f"Fetched {len(df)} symbols from NASDAQ")

        return df

    def get_amex_symbol_list(self):
        amex_symbol_list = requests.get(AMEX_LIST_URL).json()

        # 直接转 DataFrame（超级简单）
        amex_symbol_df = pd.DataFrame(amex_symbol_list)

        df = [
            f"{code}.AMEX" for code in amex_symbol_df["symbol"]
        ]
        logger.info(f"Fetched {len(df)} symbols from AMEX")

        return df

    def _get_symbol_list_from_sina(self) -> List[str]:
        us_spot_df = ak.stock_us_spot()
        if us_spot_df is None:
            raise Exception("Failed to fetch symbols from Sina")

        # Save US symbols
        sql = "CREATE OR REPLACE TABLE sina_us_spot AS SELECT * FROM temp_df"
        db.write(df=us_spot_df, sql=sql)
        
        return (us_spot_df["symbol"].astype(str) + "." + us_spot_df["market"]).tolist()

    def get_symbol_list_from_sina(self) -> List[str] | None:
        try:
            logger.info("Trying to fetch symbols from Sina")
            return self._get_symbol_list_from_sina()
        except Exception as e:
            logger.error(f"Failed to fetch symbols from Sina: {e}")
            # try to read from sina_us_sport table
            symbol_df = db.read("SELECT symbol, market from sina_us_spot", fetch_mode="df")
            if len(symbol_df) != 0:
                logger.info(f"Fetched {len(symbol_df)} symbols from sina_us_spot table")
                return (symbol_df["symbol"].astype(str) + "." + symbol_df["market"]).tolist()
        return None

    def get_symbol_list(self) -> List[str]:
        symbols_list = self.get_symbol_list_from_sina()
        if symbols_list is not None:
            logger.info(f"Fetched {len(symbols_list)} symbols from Sina")
            return symbols_list
        logger.warning("Failed to fetch symbols from Sina and local database, trying to fetch from github")
        return self.get_nyse_symbol_list() + self.get_nasdaq_symbol_list() + self.get_amex_symbol_list()

    def get_source(self, datasource_api: DataSourceApiName | None) -> DataSource:
        return USStockSource(data_source_api=datasource_api)

    def identify_symbol_type(self, code: str) -> SymbolType:
        if code.startswith('^'):
            return SymbolType.INDEX        # 指数
        
        parts = code.split('^')
        if len(parts) >= 4:
            return SymbolType.OPTION       # 期权（典型格式：code^date^C/P^strike）
        
        if '^' in code:
            return SymbolType.OTC_PREFERRED     # OTC、优先股、ADR 等特殊标的
        
        return SymbolType.STOCK            # 普通股票