from abc import ABC, abstractmethod
import pandas as pd
from vectorbt_test.engine.signal_expr import SignalGroup


class BaseStrategy(ABC):
    def __init__(self, name: str, buy_signals: SignalGroup, sell_signals: SignalGroup | None):
        self.name = name
        self.buy_signals = buy_signals
        self.sell_signals = sell_signals
    
    @abstractmethod
    def score(self, data: pd.DataFrame) -> pd.Series:
        pass
