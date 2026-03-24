import akshare as ak
import pandas as pd
from sources.akshare_stock_source import SourceAPI
from utils.log_manager import get_default_logger
import easyquotation as eq

class AKshareSinaHKSource:
    SOURCE: SourceAPI = SourceAPI.SINA
    
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

    def fetch_daily(self, symbol, start) -> pd.DataFrame | None:

        code = symbol.split(".")[0]

        # 🥇 尝试新浪
        try:
            df = ak.stock_hk_daily(symbol=code, adjust="qfq")
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

class AKshareEastQuotationHKSource:
    SOURCE: SourceAPI = SourceAPI.EAST_QUOTATION

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        东方量化（腾讯）格式转换
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

    def fetch_daily(self, symbol, start) -> pd.DataFrame | None:

        code = symbol.split(".")[0]

        # 尝试EastQuotation(腾讯)
        try:
            # 使用港股日K线数据源
            quotation = eq.use("daykline")
            # 获取历史日K（默认返回全部历史）
            data = quotation.real([code])

            # 转换为 DataFrame
            df_list = []
            for code, rows in data.items():
                for row in rows:
                    # 腾讯返回格式：[日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额]
                    df_list.append([row[0], row[1], row[2], row[3], row[4], row[5]])

            df = pd.DataFrame(df_list, columns=["date", "open", "high", "low", "close", "amount"])
            # 日期格式处理
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            # 数值转 float
            df[["open", "high", "low", "close", "amount"]] = df[["open", "high", "low", "close", "amount"]].astype(float)
            
            # 过滤日期
            df = df[df["date"] >= start]

            if df is not None and not df.empty:
                print(f"[EASTQUOTATION] success: {symbol}")
                get_default_logger().info(f"[EASTQUOTATION] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[EASTQUOTATION] failed: {symbol}, error={e}")
            get_default_logger().error(f"[EASTQUOTATION] failed: {symbol}, error={e}")
            raise e  # 上层重试
        
class AKshareEastMoneyHKSource:
    SOURCE: SourceAPI = SourceAPI.EAST

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

    def fetch_daily(self, symbol, start) -> pd.DataFrame | None:
        code = symbol.split(".")[0]
        
        # 东财
        try:
            df = ak.stock_hk_hist(
                symbol=code,
                start_date=start,
                adjust="qfq",
                period="daily"  # 有些版本区分来源
            )

            if df is not None and not df.empty:
                print(f"[EASTMONEY] success: {symbol}")
                get_default_logger().info(f"[EASTMONEY] success: {symbol}")
                return self.normalize(df, symbol)

        except Exception as e:
            print(f"[EASTMONEY] failed: {symbol}, error={e}")
            get_default_logger().error(f"[EASTMONEY] failed: {symbol}, error={e}")
            raise e  # 上层重试

        
class AkshareHKStockSource:
    
    def fetch_daily(self, symbol, start):

        # 先尝试新浪，失败重试一次，再失败尝试EASTQUOTATION(腾讯), 最后失败尝试东财
        try: 
            get_default_logger().info(f"Trying SINA for {symbol} daily data since {start}")
            return AKshareSinaHKSource().fetch_daily(symbol, start)
        except:
            pass
        try:
            get_default_logger().info(f"Trying EASTQUOTATION for {symbol} daily data since {start}")
            return AKshareEastQuotationHKSource().fetch_daily(symbol, start)
        except:
            pass
        try:
            get_default_logger().info(f"Trying EASTMONEY for {symbol} daily data since {start}")       
            return AKshareEastMoneyHKSource().fetch_daily(symbol, start)
        except:
            pass
        
        # ❌ 全失败
        get_default_logger().error(f"[FAIL] no data: {symbol}")

        return pd.DataFrame(columns=[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount"
        ])


        
