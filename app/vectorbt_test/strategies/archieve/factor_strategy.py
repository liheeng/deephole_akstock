import pandas as pd
from vectorbt_test.engine.signal_engine import SignalEngine
from vectorbt_test.core.base_strategy import BaseStrategy
from vectorbt_test.core.factor import Factor
from typing import List
from vectorbt_test.utils.quota_funcs import maybe_normalize


class FactorStrategy(BaseStrategy):

    def __init__(self, name: str, factors: List[Factor], factor_weights: List[float] | None = None):
        super().__init__(name, factors=factors)
        self.factor_weights = factor_weights or [1.0 / len(self.factors)] * len(self.factors)

    def score(self, data: pd.DataFrame, signal_engine: SignalEngine | None = None) -> pd.Series:
        s_engine = signal_engine or SignalEngine()

        # ===== 收集所有 signals =====
        all_signals = []
        for factor in self.factors:
            all_signals.extend(factor.signals())

        # 去重（关键）
        unique_signals = list({s.name: s for s in all_signals}.values())

        # ===== 一次性计算 =====
        signal_values = s_engine.generate(
            data=data,
            signals=unique_signals
        )

        # ===== 计算 alpha =====
        alpha = None
        for factor, w in zip(self.factors, self.factor_weights):
            weighted = factor.score(signal_values) * w

            alpha = weighted if alpha is None else alpha + weighted
        
        # 加横截面标准化（必须！）
        alpha = maybe_normalize(alpha)
        return alpha
