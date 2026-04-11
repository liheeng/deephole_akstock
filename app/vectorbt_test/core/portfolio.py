from dataclasses import dataclass
from typing import List, Sequence
from abc import ABC, abstractmethod
import pandas as pd
from vectorbt_test.core.strategy import Strategy


@dataclass
class PortfolioParameters:
    freq: str
    init_cash: float
    top_n: int | None
    hold_days: int


class StrategyPortfolio(ABC):
    def __init__(self,
                 strategies: Sequence[Strategy],
                 portfolio_params: PortfolioParameters | None = None):
        self.strategies = strategies
        self.params = portfolio_params

    @abstractmethod
    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        pass
