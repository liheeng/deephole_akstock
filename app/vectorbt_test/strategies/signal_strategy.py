from vectorbt_test.core.signals import Signal
from vectorbt_test.core.factors import Factor
from vectorbt_test.core.strategy import StrategyMode
from typing import List, Dict
from vectorbt_test.strategies.hybrid_strategy import HybridStrategy


class SignalStrategy(HybridStrategy):

    def __init__(self, name, factors: List[Factor | str | Dict[str, str]], signal: str | Signal | Dict[str, str]| None = None, top_n=10, threshold=0):
        super().__init__(name, factors, signal, strategy_mode=StrategyMode.TIME_SERIES, top_n=top_n, threshold=threshold)