from dataclasses import dataclass
from typing import Sequence, Dict, Any
from abc import ABC, abstractmethod
import pandas as pd
from vectorbt_test.core.strategy import Strategy
from vectorbt_test.engine.data_adapter import DataAdapter
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.core.signals import Signal, SignalScope
from vectorbt_test.core.node_builder import NodeBuilder


@dataclass
class PortfolioParameters:
    freq: str
    init_cash: float
    top_n: int | None
    hold_days: int


class StrategyPortfolio(ABC):
    def __init__(self,
                 strategies: Sequence[Strategy],
                 schedule_signal: str | Signal | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        self.strategies = strategies
        built_signal = NodeBuilder().build(schedule_signal) if isinstance(schedule_signal, str) else schedule_signal
        self.schedule_signal: Signal | None = built_signal if isinstance(built_signal, Signal) or built_signal is None else None
        if self.schedule_signal is not None:
            assert self.schedule_signal.is_signal and self.schedule_signal.is_scope(SignalScope.CS.value | SignalScope.TS_CS.value)
        self.params = portfolio_params

    @abstractmethod
    def run(self, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        pass


class PortfolioContext(Dict[str, Any]):
    def __init__(
        self,
        # data_provider: DataProvider,
        # data_adapter: DataAdapter,
        **kwargs
    ):
        super().__init__(
            # data_provider=data_provider,
            # data_adapter=data_adapter,
            **kwargs
        )

    @property
    def data_provider(self) -> DataProvider | None:
        return self.get("data_provider")

    @data_provider.setter
    def data_provider(self, provider: DataProvider):
        self["data_provider"] = provider

    @property
    def data_adapter(self) -> DataAdapter | None:
        return self.get("data_adapter")

    @data_adapter.setter
    def data_adapter(self, adapter: DataAdapter):
        self["data_adapter"] = adapter
