import akshare as ak
from sources.hk_datasource import HKStockSource
from markets.market import Region
from utils.log_manager import get_default_logger


class HongKongStockMarket:

    region: Region = Region.HK
    name: str = region.value.upper()

    def get_symbol_list(self):

        stock_hk_df = ak.stock_hk_spot()
        df = [
            f"{code}.HK" for code in stock_hk_df["代码"]
        ]
        get_default_logger().info(f"Fetched {len(df)} symbols from Hong Kong \
                                  Stock Exchange")
        
        return df

    def get_source(self):
        return HKStockSource()
