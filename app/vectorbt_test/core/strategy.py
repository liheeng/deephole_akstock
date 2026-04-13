from abc import ABC, abstractmethod
from enum import Enum
from vectorbt_test.core.portfolio import PortfolioContext


class StrategyMode(Enum):
    AUTO = "auto"
    TIME_SERIES = "ts"
    CROSS_SECTION = "cs"


class Strategy(ABC):
    @property
    def name(self):
        return self.__class__.__name__
    
    def bind_data_adapter(self, data_adapter):
        self.data_adapter = data_adapter

    @abstractmethod
    def generate(self, data, context: PortfolioContext) -> dict:
        pass
