import enum
from dataclasses import dataclass
from typing import Sequence
from abc import ABC, abstractmethod
import pandas as pd
from vectorbt_test.core.strategy import Strategy
from vectorbt_test.core.signals import Signal, SignalGroup
from vectorbt_test.core.node_builder import NodeBuilder
import vectorbt as vbt


class PortfolioType(enum.Enum):
    SIGNAL_STRATEGY = "signal_strategy"
    WEIGHT_STRATEGY = "weight_strategy"


@dataclass
class PortfolioParameters:
    freq: str
    init_cash: float
    top_n: int | None = 10
    hold_days: int = 5


class StrategyPortfolio(ABC):
    def __init__(self,
                 strategies: Sequence[Strategy],
                 schedule_signal: str | Signal | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        self.strategies = strategies
        built_signal = NodeBuilder().build(schedule_signal) if isinstance(schedule_signal, str) else schedule_signal
        self.schedule_signal: Signal | None = built_signal if isinstance(built_signal, Signal) or built_signal is None else None
        if self.schedule_signal is not None:
            assert self.schedule_signal.is_signal and self.schedule_signal.is_group(SignalGroup.CS.value | SignalGroup.TS_CS.value)
        self.params = portfolio_params

    @abstractmethod
    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        pass


class PortfolioWrapper():
    def __init__(self, portfolio: vbt.Portfolio):
        self.portfolio = portfolio

    def stats(self):
        pf = self.portfolio
        if pf.wrapper.shape[1] == 1:
            return pf.stats(column=pf.wrapper.columns[0])
        else:
            return pf.stats()