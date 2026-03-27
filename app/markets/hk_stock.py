import akshare as ak
from typing import List
from sources.data_source import DataSource
from sources.hk_datasource import HKStockSource
from markets.market import Region, Market
from utils.log_manager import get_logger

logger = get_logger(__name__)


class HongKongStockMarket(Market):

    region: Region = Region.HK
    name: str = region.value.upper()

    def get_symbol_list(self) -> List[str]:

        stock_hk_df = ak.stock_hk_spot()
        df = [
            f"{code}.HK" for code in stock_hk_df["代码"]
        ]
        logger.info(f"Fetched {len(df)} symbols from Hong Kong \
                                  Stock Exchange")
        
        return df

    def get_source(self) -> DataSource:
        return HKStockSource()
