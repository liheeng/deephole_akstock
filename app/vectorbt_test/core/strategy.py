from .factor import Factor
from typing import List
from abc import ABC, abstractmethod
from enum import Enum


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
    def generate(self, data, cache, context: dict | None = None):
        pass
