import akshare as ak
from sources.akshare_cna import AkshareCNAStockSource
from utils.log_manager import get_default_logger

class CNAStockMarket:

    name = "CN"

    def get_shanghai_symbol_list(self):

        stock_info_sh_name_code_df = ak.stock_info_sh_name_code(symbol="主板A股")
        df = [
            f"{code}.SH" for code in stock_info_sh_name_code_df["证券代码"]
        ]
        get_default_logger().info(f"Fetched {len(df)} symbols from Shanghai Stock Exchange")

        return df
    
    def get_shenzhen_symbol_list(self):

        stock_info_sz_name_code_df = ak.stock_info_sz_name_code(symbol="A股列表")
        df = [
            f"{code}.SZ" for code in stock_info_sz_name_code_df["A股代码"]
        ]
        get_default_logger().info(f"Fetched {len(df)} symbols from Shenzhen Stock Exchange") 

        return df
    
    def get_symbol_list(self):
        return self.get_shanghai_symbol_list() + self.get_shenzhen_symbol_list()

    def get_source(self):
        return AkshareCNAStockSource()
