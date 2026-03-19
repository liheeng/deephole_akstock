import akshare as ak
import pandas as pd
from utils.log_manager import get_task_logger
from sources.akshare_stock_source import StockSource

class AKshareSinaCNASource:
    SOURCE: StockSource = StockSource.SINA
    
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

    def fetch_daily(self, symbol, start):

        code = symbol.split(".")[-1].lower()  + symbol.split(".")[0]

        # 🥇 尝试新浪
        try:
            df = ak.stock_zh_a_daily(
                symbol=code,
                start_date=start,
                adjust="qfq"
            )

            if df is not None and not df.empty:
                print(f"[SINA] success: {symbol}")
                get_task_logger().info(f"[SINA] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[SINA] failed: {symbol}, error={e}")
            get_task_logger().error(f"[SINA] failed: {symbol}, error={e}")  
            raise e  # 上层重试

class AKshareTencentCNASource:
    SOURCE: StockSource = StockSource.TENCENT
    
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

    def fetch_daily(self, symbol, start):

        code = symbol.split(".")[-1].lower()  + symbol.split(".")[0]

        # 尝试腾讯
        try:
            df = ak.stock_zh_a_hist_tx(
                symbol=code,
                start_date=start,
                adjust="qfq"
            )

            if df is not None and not df.empty:
                print(f"[TENCENT] success: {symbol}")
                get_task_logger().info(f"[TENCENT] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[TENCENT] failed: {symbol}, error={e}")
            get_task_logger().error(f"[TENCENT] failed: {symbol}, error={e}")
            raise e  # 上层重试
        
class AKshareEastMoneyCNASource:
    SOURCE: StockSource = StockSource.EAST

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:   
        """
        东财格式转换（注意中文字段）
        """
        df = df.copy()

        df.rename(columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turnover",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "turnover"
        ]]

    def fetch_daily(self, symbol, start) -> pd.DataFrame:
        code = symbol.split(".")[0]
        
        # 东财
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                start_date=start,
                adjust="qfq",
                period="daily"  # 有些版本区分来源
            )

            if df is not None and not df.empty:
                print(f"[EASTMONEY] success: {symbol}")
                get_task_logger().info(f"[EASTMONEY] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[EASTMONEY] failed: {symbol}, error={e}")
            get_task_logger().error(f"[EASTMONEY] failed: {symbol}, error={e}")
            raise e  # 上层重试

        
class AkshareCNAStockSource:
    
    def fetch_daily(self, symbol, start):

        # 先尝试新浪，失败重试一次，再失败尝试东财，最后失败尝试腾讯
        try: 
            get_task_logger().info(f"Trying SINA for {symbol} daily data since {start}")
            return AKshareSinaCNASource().fetch_daily(symbol, start)
        except:
            pass
        try:
            get_task_logger().info(f"Trying EASTMONEY for {symbol} daily data since {start}")       
            return AKshareEastMoneyCNASource().fetch_daily(symbol, start)
        except:
            pass
        try:
            get_task_logger().info(f"Trying TENCENT for {symbol} daily data since {start}")
            return AKshareTencentCNASource().fetch_daily(symbol, start)
        except:
            pass
        
        # ❌ 全失败
        get_task_logger().error(f"[FAIL] no data: {symbol}")

        return pd.DataFrame(columns=[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount"
        ])


        
