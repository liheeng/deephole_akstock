import akshare as ak
import yfinance as yf
import pandas as pd
from markets.market import Region
from sources.data_source import DataSource, DataSourceAPI
from sources.datasource_adapter import convert_symbol
from sources.ifind.ifind_api import IfindApi
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

    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:

        # code = symbol.split(".")[0]
        code = convert_symbol(symbol, DataSource.AKSHARE, Region.US, self.source_api_type)
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

    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:
        # code = symbol.split(".")[0]
        code = convert_symbol(symbol, DataSource.YFINANCE, Region.US, self.source_api_type)
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

    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:
        if not IfindApi.instance().is_available():
            raise Exception("iFinD is not available")

        code = convert_symbol(symbol, DataSource.IFIND, Region.US)
        start = start.strftime("%Y-%m-%d")
        # iFinD
        try:
            df = IfindApi.instance().get_historical_data(
                code=code,
                start=start
            )

            if df is not None and not df.empty:
                print(f"[iFinD] success: {symbol}")
                get_default_logger().info(f"[iFinD] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[iFinD] failed: {symbol}, error={e}")
            get_default_logger().error(f"[iFinD] failed: {symbol}, error={e}")
            raise e  # 上层重试


class USStockSource:
    
    def fetch_daily(self, symbol, start: datetime):

        # 先尝试iFinD, 失败再尝试新浪，失败重试一次，再失败尝试EASTQUOTATION(腾讯), 最后失败尝试东财
        try:
            get_default_logger().info(f"Trying iFinD for {symbol} daily data since {start}")
            return IFinDUSSource().fetch_daily(symbol, start)
        except Exception as e:
            get_default_logger().error(f"[iFinD] failed: {symbol}, error={e}")
            pass
        try: 
            get_default_logger().info(f"Trying SINA for {symbol} daily data since {start}")
            return AKshareSinaUSSource().fetch_daily(symbol, start)
        except Exception as e:
            get_default_logger().error(f"[SINA] failed: {symbol}, error={e}")
            pass
        try:
            get_default_logger().info(f"Trying YFINANCE for {symbol} daily data since {start}")
            return AKshareYFinanceSource().fetch_daily(symbol, start)
        except Exception as e:
            get_default_logger().error(f"[YFINANCE] failed: {symbol}, error={e}")
            pass
        
        # ❌ 全失败
        get_default_logger().error(f"[FAIL] no data: {symbol}")

        return pd.DataFrame(columns=[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount"
        ])


        
