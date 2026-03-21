import requests
import pandas as pd
import akshare as ak
from sources.akshare_us import AkshareUSStockSource
from utils.log_manager import get_default_logger

NYSE_LIST_FILE = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.json"
NASDAQ_LIST_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json"
AMEX_LIST_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_full_tickers.json"

class USStockMarket:

    name = "US"

    def get_nyse_symbol_list(self):
        nyse_symbol_list = requests.get(NYSE_LIST_FILE).json()

        # 直接转 DataFrame（超级简单）
        nyse_symbol_df = pd.DataFrame(nyse_symbol_list)

        df = [
            f"{code}.NYSE" for code in nyse_symbol_df["symbol"]
        ]
        get_default_logger().info(f"Fetched {len(df)} symbols from NYSE")

        return df

    def get_nasdaq_symbol_list(self):
        nasdaq_symbol_list = requests.get(NASDAQ_LIST_URL).json()

        # 直接转 DataFrame（超级简单）
        nasdaq_symbol_df = pd.DataFrame(nasdaq_symbol_list)

        df = [
            f"{code}.NASDAQ" for code in nasdaq_symbol_df["symbol"]
        ]
        get_default_logger().info(f"Fetched {len(df)} symbols from NASDAQ")

        return df

    def get_amex_symbol_list(self):
        amex_symbol_list = requests.get(AMEX_LIST_URL).json()

        # 直接转 DataFrame（超级简单）
        amex_symbol_df = pd.DataFrame(amex_symbol_list)

        df = [
            f"{code}.AMEX" for code in amex_symbol_df["symbol"]
        ]
        get_default_logger().info(f"Fetched {len(df)} symbols from AMEX")

        return df

    def get_symbol_list(self):
        return self.get_nyse_symbol_list() + self.get_nasdaq_symbol_list() + self.get_amex_symbol_list()

    def get_source(self):
        return AkshareUSStockSource()
