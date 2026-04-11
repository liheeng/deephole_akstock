from vectorbt_test.core.factor import FactorNode
from vectorbt_test.core.strategy import StrategyMode
from typing import List
from .hybrid_strategy import HybridStrategy


class SignalStrategy(HybridStrategy):

    def __init__(self, factors: List[FactorNode], top_n=10, threshold=0):
        super().__init__(factors, mode=StrategyMode.TIME_SERIES, top_n=top_n, threshold=threshold)