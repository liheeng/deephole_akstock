import enum
from abc import ABC, abstractmethod
from typing import List
import pandas as pd
from sources.data_source import DataSource


class Region(enum.Enum):
    CN = "cn"
    HK = "hk"
    US = "us"


class ExchangeType(enum.Enum):
    SH = "SH"   # Shanghai, China
    SZ = "SZ"   # Shenzhen, China
    HK = "HK"   # Hong Kong
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"
    AMEX = "AMEX"
    OTC = "OTC"


class SymbolType(enum.Enum):
    STOCK = "stock"
    INDEX = "index"
    FUTURE = "future"
    OPTION = "option"
    OTC_PREFERRED = "otc/preferred"


class Market(ABC):
    region: Region
    name: str

    @abstractmethod
    def get_symbol_list(self) -> List[str]:
        pass

    @abstractmethod
    def get_source(self) -> DataSource:
        pass
