# coding=utf-8

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime
from utils.common import ResultStatus

# 每次最多获取 10000 条历史数据
HIS_BATCH_SIZE_LIMIT = 10000
# 每次最多获取 500 个股票的历史数据
HIS_BATCH_SYMBOLS_LIMIT = 500


class DataSourceType(enum.Enum):
    AKSHARE = "akshare"     # akshare, support CN-A, HK, US
    YFINANCE = "yfinance"   # yahoo finance, support US, also support CN-A and HK
    IFIND = "ifind"     # ifind, support CN-A, HK and US
    EASTQUOTATION = "eastquotation"     # eastquotation, support HK


class DataSourceApiName(enum.Enum):
    AKSHARE_EASTMONEY_API = f"{DataSourceType.AKSHARE.value}.eastmoney"
    AKSHARE_SINA_API = f"{DataSourceType.AKSHARE.value}.sina"
    AKSHARE_TENCENT_API = f"{DataSourceType.AKSHARE.value}.tencent"
    
    CN_SSE_API = "cn.sse"   # 上交所
    CN_SZSE_API = "cn.szse"     # 深交所
    CN_SINA_API = "cn.sina"     # 新浪
    EASTMONEY_API = "eastmoney"     # 东方财富
    XUEQIU_API = "xueqiu"   # 雪球
    HKEX_API = "hkex"   # 港交所
    NYSE_API = "nyse"   # 纽交所
    NASDAQ_API = "nasdaq"   # 纳斯达克
    YAHOO_FINANCE_API = "yahoo_finance"     # 雅虎财经
    TENCENT_API = "tencent"     # 腾讯财经
    EAST_QUOTATION_API = "eastquotation"    # 东方量化
    YFINANCE_API = "yfinance"   # yfinance
    IFIND_API = "ifind"

class StartPriority(enum.Enum):
    """
        There is a start_priority in QueryOptions, if the value is DATABASE, then 
        while fetching symbol data from market/source, the start_date will use the 
        latest date between actual symbol's last date in datebase and the specified start date
    """
    DATABASE = 1
    USER_SPECIFIED = 2


class QueryOptions(Dict[str, Any]):
    def __init__(
        self,
        start: datetime,
        end: Optional[datetime] = None,
        start_priority: StartPriority = StartPriority.DATABASE,
        support_parallel: bool = False,
        **kwargs
    ):
        # 注意：你之前拼写错误 start_prirorty → start_priority
        super().__init__(
            start=start,
            end=end,
            start_priority=start_priority,  # ✅ 修复拼写
            support_parallel=support_parallel,
            **kwargs
        )

    @property
    def start(self) -> datetime:
        return self.get("start")

    @start.setter
    def start(self, value: datetime):
        self["start"] = value

    @property
    def start_priority(self) -> StartPriority:
        return self.get("start_priority")

    @start_priority.setter
    def start_priority(self, value: StartPriority):
        self["start_priority"] = value

    @property
    def end(self) -> Optional[datetime]:
        return self.get("end")

    @end.setter
    def end(self, value: Optional[datetime]):
        self["end"] = value

    @property
    def support_parallel(self) -> bool:
        return self.get("support_parallel")

    @support_parallel.setter
    def support_parallel(self, value: bool):
        self["support_parallel"] = value


@dataclass
class FetchResult:
    status: ResultStatus = ResultStatus.FAILED
    data: Dict[str, pd.DataFrame] | pd.DataFrame | None = None
    failed_symbols: List[str] | None = None
    message: str = ""
    error: str = ""


class DataSource(ABC):
    
    data_source_api: DataSourceApiName

    @abstractmethod
    def fetch_daily(self, symbols_str: str, options: QueryOptions | None = None) -> FetchResult | None:
        pass


class AbstractDataSourceAPI(ABC):
    source_api_type: DataSourceApiName
    name: str
    
    @abstractmethod
    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_hist(self, symbols_str: str, options: QueryOptions | None = None) -> FetchResult:
        pass

