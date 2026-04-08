from abc import ABC, abstractmethod
import pandas as pd
from vectorbt_test.core.factor import Factor
from vectorbt_test.engine.signal_engine import SignalEngine
from typing import List


class BaseStrategy(ABC):
    def __init__(self, name: str, factors: List[Factor]):
        self.name = name
        self.factors = factors
    
    @abstractmethod
    def score(self, data: pd.DataFrame, signal_engine: SignalEngine | None = None) -> pd.Series:
        pass
