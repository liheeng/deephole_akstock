from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from vectorbt_test.core.context import PortfolioContext
from typing import Any


@dataclass
class StrategyResult:
    type: str
    entries: Any | None
    exits: Any | None
    weights: Any | None


class StrategyMode(Enum):
    AUTO = "auto"
    TIME_SERIES = "ts"
    CROSS_SECTION = "cs"


class Strategy(ABC):
    @property
    def name(self):
        # return self.__class__.__name__
        return self._name or self.__class__.__name__
    
    @name.setter
    def name(self, value):
        self._name = value
    
    def bind_data_adapter(self, data_adapter):
        self.data_adapter = data_adapter

    @abstractmethod
    def generate(self, data, context: PortfolioContext) -> StrategyResult:
        pass

