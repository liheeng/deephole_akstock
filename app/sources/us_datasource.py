import akshare as ak
import yfinance as yf
import pandas as pd
from typing import Dict, List
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceAPI, DataSource
from sources.datasource_adapter import convert_symbol
from sources.ifind.ifind_api import IfindApi, HIS_BATCH_SIZE_LIMIT
from utils.log_manager import get_default_logger
from utils.symbol import fix_preferred_symbol
from datetime import datetime


class AKshareSinaUSSource:
    source_api_type: DataSourceAPI = DataSourceAPI.AKSHARE_SINA_API
    
    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        新浪格式转换
        """
        df = df.copy()

        df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume"
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume"
        ]]

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        return next_index + 1, symbols[next_index]
    
    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:

        # code = symbol.split(".")[0]
        code = convert_symbol(symbol, DataSourceType.AKSHARE, Region.US, self.source_api_type)
        code = fix_preferred_symbol(code)

        # 🥇 尝试新浪
        try:
            df = ak.stock_us_daily(symbol=code, adjust="qfq")
            # filter those data only is later than specific start
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= start]

            if df is not None and not df.empty:
                print(f"[SINA] success: {symbol}")
                get_default_logger().info(f"[SINA] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[SINA] failed: {symbol}, error={e}")
            get_default_logger().error(f"[SINA] failed: {symbol}, error={e}")  
            raise e  # 上层重试


class AKshareYFinanceSource:
    source_api_type: DataSourceAPI = DataSourceAPI.YFINANCE_API

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:   
        """
        YFinance格式转换
        """
        df = df.copy()

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume"
        ]]

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        return next_index + 1, symbols[next_index]
    
    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:
        # code = symbol.split(".")[0]
        code = convert_symbol(symbol, DataSourceType.YFINANCE, Region.US, self.source_api_type)
        code = fix_preferred_symbol(code)
        start = start.strftime("%Y-%m-%d")
        # 🥈 尝试YFinance
        try:
            ticker = yf.Ticker(code)
    
            # 美股：直接用 start/end 完全没问题！
            df = ticker.history(start=start, auto_adjust=True)  # auto_adjust=True = 前复权
            
            # 清理列
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df.columns = ["open", "high", "low", "close", "volume"]
            df = df.reset_index()  # 把日期变成列

            if df is not None and not df.empty:
                print(f"[YFINANCE] success: {symbol}")
                get_default_logger().info(f"[YFINANCE] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[YFINANCE] failed: {symbol}, error={e}")
            get_default_logger().error(f"[YFINANCE] failed: {symbol}, error={e}")
            raise e  # 上层重试


class IFinDUSSource:
    """ 同花顺iFinD
    """
    source_api_type: DataSourceAPI = DataSourceAPI.IFIND_API

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        iFinD格式转换（注意中文字段）
        """
        df = df.copy()

        df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
            "changeRatio": "pct",
            "turnoverRatio": "turnover",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "pct", "turnover"    # noqa
        ]]

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        end = end or datetime.now()
        _days = (end.date() - start.date()).days
        _days = 1 if _days == 0 else _days
        count = HIS_BATCH_SIZE_LIMIT // _days
        symbols_str = ""
        for symbol in symbols[next_index:next_index + count]:
            symbols_str = symbol if len(symbols_str) == 0 else f"{symbols_str},{symbol}"

        return next_index + count, symbols_str
    
    def fetch_daily(self, symbols_str, start: datetime) -> pd.DataFrame | Dict[str, pd.DataFrame] | None:
        if not IfindApi.instance().is_available():
            raise Exception("iFinD is not available")

        codes_str = ""
        origin_symbols_list = []
        for symbol in symbols_str.split(","):
            origin_symbols_list.append(symbol)
            symbol = convert_symbol(symbol, DataSourceType.IFIND, Region.US)
            codes_str = symbol if len(codes_str) == 0 else f"{codes_str},{symbol}"    
            
        # iFinD
        try:
            his_data: Dict[str, pd.DataFrame] = IfindApi.instance().get_historical_data(
                codes=codes_str,
                start=start.strftime("%Y-%m-%d")
            ) # type: ignore

            if his_data is None or len(his_data) == 0:
                return None
            
            print(f"[iFinD] success: {symbols_str}")
            get_default_logger().info(f"[iFinD] success: {symbols_str}")
            new_his_data = {}
            his_data_keys = list(his_data.keys())
            for i in range(len(his_data_keys)):
                new_his_data[origin_symbols_list[i]] = self.normalize(his_data[his_data_keys[i]], origin_symbols_list[i])
            
            return new_his_data

        except Exception as e:
            print(f"[iFinD] failed: {symbols_str}, error={e}")
            # get_default_logger().error(f"[iFinD] failed: {symbols_str}, error={e}")
            raise e  # 上层重试


class USStockSource(DataSource):
    source_api_list = []
    source_api_cursor: int

    def __init__(self):
        super().__init__()
        self.source_api_list = [
            IFinDUSSource,
            AKshareSinaUSSource,
            AKshareYFinanceSource
        ]
        self.source_api_cursor = -1

    def prepare_fetch(self):
        # Reset cursor to use first source API
        self.source_api_cursor = -1

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        cursor = self.source_api_cursor + 1
        if cursor >= len(self.source_api_list):
            cursor = 0

        return self.source_api_list[cursor]().candidate_symbols(symbols, next_index, start, end)
       
    def fetch_daily(self, symbols_str, start: datetime) -> pd.DataFrame | Dict[str, pd.DataFrame] | None:
        self.source_api_cursor += 1
        if self.source_api_cursor >= len(self.source_api_list):
            self.source_api_cursor = 0
    
        source_api = self.source_api_list[self.source_api_cursor]
        
        try:
            instance = source_api()
            source_api_name = instance.source_api_type.value
            get_default_logger().info(f"trying {source_api_name} API for {symbols_str} daily data since {start}")
            return instance.fetch_daily(symbols_str, start)
        except Exception as e:
            get_default_logger().error(f"trying from {source_api_name} failed: {symbols_str}, error={e}")
            raise e
    
