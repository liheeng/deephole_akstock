import akshare as ak
import pandas as pd
from sources.akshare_stock_source import StockSource
from utils.log_manager import get_task_logger
import easyquotation as eq
import yfinance as yf

class AKshareSinaUSSource:
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
            "volume": "volume"
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume"
        ]]

    def fetch_daily(self, symbol, start):

        code = symbol.split(".")[0]

        # 🥇 尝试新浪
        try:
            df = ak.stock_us_daily(symbol=code, adjust="qfq")
            # filter those data only is later than specific start
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= start]

            if df is not None and not df.empty:
                print(f"[SINA] success: {symbol}")
                get_task_logger().info(f"[SINA] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[SINA] failed: {symbol}, error={e}")
            get_task_logger().error(f"[SINA] failed: {symbol}, error={e}")  
            raise e  # 上层重试

class AKshareYFinanceSource:
    SOURCE: StockSource = StockSource.YFINANCE

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

    def fetch_daily(self, symbol, start) -> pd.DataFrame:
        code = symbol.split(".")[0]
        
        # 🥈 fallback 东财
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
                get_task_logger().info(f"[YFINANCE] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[YFINANCE] failed: {symbol}, error={e}")
            get_task_logger().error(f"[YFINANCE] failed: {symbol}, error={e}")
            raise e  # 上层重试
        
class AkshareUSStockSource:
    
    def fetch_daily(self, symbol, start):

        # 先尝试新浪，失败重试一次，再失败尝试EASTQUOTATION(腾讯), 最后失败尝试东财
        try: 
            get_task_logger().info(f"Trying SINA for {symbol} daily data since {start}")
            return AKshareSinaUSSource().fetch_daily(symbol, start)
        except:
            pass
        try:
            get_task_logger().info(f"Trying YFINANCE for {symbol} daily data since {start}")
            return AKshareYFinanceSource().fetch_daily(symbol, start)
        except:
            pass
        
        # ❌ 全失败
        get_task_logger().error(f"[FAIL] no data: {symbol}")

        return pd.DataFrame(columns=[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount"
        ])


        
