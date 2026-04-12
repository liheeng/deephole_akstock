from vectorbt_test.core.factors import Factor
from vectorbt_test.core.strategy import StrategyMode
from typing import List
from .hybrid_strategy import HybridStrategy


class WeightStrategy(HybridStrategy):

    def __init__(self, name, factors: List[Factor | str], signal: str | Signal | None = None, top_n=10, threshold=0):
        super().__init__(name, factors, signal, mode=StrategyMode.CROSS_SECTION, top_n=top_n, threshold=threshold)