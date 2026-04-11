from vectorbt_test.core.factors import FactorNode
from vectorbt_test.core.strategy import StrategyMode
from typing import List
from .hybrid_strategy import HybridStrategy


class WeightStrategy(HybridStrategy):

    def __init__(self, factors: List[FactorNode], top_n=10, threshold=0):
        super().__init__(factors, mode=StrategyMode.CROSS_SECTION, top_n=top_n, threshold=threshold)