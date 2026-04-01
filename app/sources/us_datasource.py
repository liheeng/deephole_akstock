from typing import List
from sources.ifind.ifind_api_all_hist import IFindUSHistoric
from sources.data_source import DataSource, AbstractDataSourceAPI, FetchResult, QueryOptions
from sources.akshare.akshare_api_us_hist_sina import AKshareApiUSHistoricSina
from utils.log_manager import get_logger


logger = get_logger(__name__)


class USStockSource(DataSource):
    source_api_list: List[AbstractDataSourceAPI] = []
    source_api_cursor: int

    def __init__(self):
        super().__init__()
        self.source_api_list = [
            IFindUSHistoric()
            # AKshareApiUSHistoricSina()
            # AKshareYFinanceSource
        ]
        self.source_api_cursor = -1

    def fetch_daily(self, symbols_str, options: QueryOptions | None = None  ) -> FetchResult | None:
        self.source_api_cursor += 1
        if self.source_api_cursor >= len(self.source_api_list):
            self.source_api_cursor = 0
    
        instance = self.source_api_list[self.source_api_cursor]
        
        try:
            source_api_name = instance.source_api_type.value
            logger.info(f"trying {source_api_name} API for {symbols_str} daily data since {options.get('start') if options else None}")
            return instance.fetch_hist(symbols_str, options)
        except Exception as e:
            logger.exception(f"trying from {source_api_name} failed: {symbols_str}, error={e}")
            raise e
    
