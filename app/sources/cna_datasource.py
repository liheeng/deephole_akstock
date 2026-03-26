import akshare as ak
import pandas as pd
from utils.log_manager import get_default_logger
from markets.market import Region
from sources.data_source import DataSource, DataSourceAPI
from sources.datasource_adapter import convert_symbol
from sources.ifind.ifind_api import IfindApi
from datetime import datetime
from typing import Dict


class AKshareSinaCNASource:
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
            "volume": "volume",
            "amount": "amount",
            "turnover": "turnover",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "turnover"
        ]]

    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:

        # code = symbol.split(".")[-1].lower()  + symbol.split(".")[0]
        code = convert_symbol(symbol, DataSource.AKSHARE, Region.CN, self.source_api_type)
        start = start.strftime("%Y%m%d")

        # 🥇 尝试新浪
        try:
            df = ak.stock_zh_a_daily(
                symbol=code,
                start_date=start,
                adjust="qfq"
            )

            if df is not None and not df.empty:
                print(f"[SINA] success: {symbol}")
                get_default_logger().info(f"[SINA] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[SINA] failed: {symbol}, error={e}")
            get_default_logger().error(f"[SINA] failed: {symbol}, error={e}")  
            raise e  # 上层重试


class AKshareTencentCNASource:
    source_api_type: DataSourceAPI = DataSourceAPI.AKSHARE_TENCENT_API
    
    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        腾讯格式转换
        """
        df = df.copy()

        df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "amount": "amount",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "amount"
        ]]

    def fetch_daily(self, symbol, start: datetime) -> pd.DataFrame | None:

        # code = symbol.split(".")[-1].lower()  + symbol.split(".")[0]
        code = convert_symbol(symbol, DataSource.AKSHARE, Region.CN, self.source_api_type)
        start = start.strftime("%Y%m%d")
        # 尝试腾讯
        try:
            df = ak.stock_zh_a_hist_tx(
                symbol=code,
                start_date=start,
                adjust="qfq"
            )

            if df is not None and not df.empty:
                print(f"[TENCENT] success: {symbol}")
                get_default_logger().info(f"[TENCENT] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[TENCENT] failed: {symbol}, error={e}")
            get_default_logger().error(f"[TENCENT] failed: {symbol}, error={e}")
            raise e  # 上层重试


class IFinDCNASource:
    """ 同花顺iFinD
    """
    source_api_type: DataSourceAPI = DataSourceAPI.IFIND_API

    def normalize(self, his_data: Dict[str, pd.DataFrame], symbol: str) -> pd.DataFrame:
        """
        iFinD格式转换
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

        code = convert_symbol(symbol, DataSource.IFIND, Region.CN)
        start = start.strftime("%Y-%m-%d")
        
        # iFinD
        try:
            his_data: Dict[str, pd.DataFrame] = IfindApi.instance().get_historical_data(
                codes=code,
                start=start
            )

            if his_data is not None and len(his_data) > 0:
                print(f"[iFinD] success: {symbol}")
                get_default_logger().info(f"[iFinD] success: {symbol}")
                return self.normalize(his_data, symbol)

        except Exception as e:
            print(f"[iFinD] failed: {symbol}, error={e}")
            get_default_logger().error(f"[iFinD] failed: {symbol}, error={e}")
            raise e  # 上层重试


class CNAStockSource:
    
    def fetch_daily(self, symbol, start: datetime):

        # 先尝试iFind，失败重试一次, 接着尝试新浪，失败重试一次，再失败尝试东财，最后失败尝试腾讯
        try:
            get_default_logger().info(f"Trying iFinD for {symbol} daily data since {start}")
            return IFinDCNASource().fetch_daily(symbol, start)
        except Exception as e:
            get_default_logger().error(f"[iFinD] failed: {symbol}, error={e}")
            pass
        try: 
            get_default_logger().info(f"Trying SINA for {symbol} daily data since {start}")
            return AKshareSinaCNASource().fetch_daily(symbol, start)
        except Exception as e:
            get_default_logger().error(f"[SINA] failed: {symbol}, error={e}")
            pass
        try:
            get_default_logger().info(f"Trying TENCENT for {symbol} daily data since {start}")
            return AKshareTencentCNASource().fetch_daily(symbol, start)
        except Exception as e:
            get_default_logger().error(f"[TENCENT] failed: {symbol}, error={e}")
            pass
        
        # ❌ 全失败
        get_default_logger().error(f"[FAIL] no data: {symbol}")

        return pd.DataFrame(columns=[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "pct", "turnover"
        ])


        
