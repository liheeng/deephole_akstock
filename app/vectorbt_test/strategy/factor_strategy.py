import pandas as pd
from vectorbt_test.engine.signal_engine import SignalEngine
from vectorbt_test.core.base_strategy import BaseStrategy
from vectorbt_test.core.factor import Factor
from typing import List


class FactorStrategy(BaseStrategy):

    def __init__(self, name: str, factors: List[Factor], factor_weights: List[float] | None = None):
        super().__init__(name, factors=factors)
        self.factor_weights = factor_weights or [1.0 / len(self.factors)] * len(self.factors)

    def score(self, data: pd.DataFrame, signal_engine: SignalEngine | None = None) -> pd.Series:
        alpha = None

        s_engine = signal_engine or SignalEngine()
        for factor, w in zip(self.factors, self.factor_weights):
            if factor is None:
                raise ValueError("Missing factor parameter, the factors parameter must have at least 1 factor")

            signals_values = s_engine.generate(
                data=data, 
                signals_list=[factor.signals()])[0]
            weighted = factor.score(signals_values) * w

            if alpha is None:
                alpha = weighted
            else:
                alpha += weighted

        return alpha
