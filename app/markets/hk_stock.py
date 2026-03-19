import akshare as ak
from sources.akshare_hk import AkshareHKStockSource
from utils.log_manager import get_task_logger

class HongKongStockMarket:

    name = "HK"

    def get_symbol_list(self):

        stock_hk_df = ak.ak.stock_hk_spot()
        df = [
            f"{code}.HK" for code in stock_hk_df["代码"]
        ]
        get_task_logger().info(f"Fetched {len(df)} symbols from Hong Kong Stock Exchange")
        
        return df

    def get_source(self):
        return AkshareHKStockSource()
