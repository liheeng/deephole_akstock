from vectorbt_test.core.factors import FactorNode
from vectorbt_test.core.strategy import Strategy, StrategyMode
from typing import List


class HybridStrategy(Strategy):

    def __init__(self, factors: List[FactorNode], mode=StrategyMode.AUTO, top_n=10, threshold=0):
        self.factors = factors
        self.mode = mode
        self.top_n = top_n
        self.threshold = threshold
    
    def generate(self, data, cache):
        # ===== 1. 合成 alpha =====
        alpha = None
        for f in self.factors:
            s = f.score(data, cache)
            alpha = s if alpha is None else alpha + s

        # ===== 2. 判断模式 =====
        mode = self.mode

        if mode == StrategyMode.AUTO:
            mode = StrategyMode.CROSS_SECTION if self.data_adapter.is_cross_section else StrategyMode.TIME_SERIES

        # ===== 3. 分支 =====
        if mode == StrategyMode.TIME_SERIES:
            return self._ts_strategy(alpha)

        else:
            return self._cs_strategy(alpha)
        
    def _ts_strategy(self, alpha):
        entries = alpha > self.threshold
        exits   = alpha < -self.threshold

        return {
            "type": "signal",
            "entries": entries,
            "exits": exits
        }
    
    def _cs_strategy(self, alpha):
        ranks = self.data_adapter.cs_rank(alpha, ascending=False)

        mask = ranks <= self.top_n

        weights = (self.top_n - ranks + 1).where(mask, 0)
        weights = self.data_adapter.cs_normalize(weights).fillna(0)

        return {
            "type": "weight",
            "weights": weights
        }
