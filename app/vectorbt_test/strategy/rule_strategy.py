from vectorbt_test.core.base_strategy import BaseStrategy
from vectorbt_test.core.factor import Factor
from vectorbt_test.engine.signal_engine import SignalEngine
from typing import List
import pandas as pd


class RuleStrategy(BaseStrategy):
    def __init__(self, name: str, factors: List[Factor]):
        super().__init__(name, factors)
        if len(self.factors) < 2:
            raise ValueError("Missing sell factor parameter, the factors parameter must have at least 2 factors")
        if self.factors[0] is None or self.factors[1] is None:
            raise ValueError("Missing buy or sell factor parameter, the factors parameter must have at least 2 factors")
        
        self.buy_factor = factors[0]
        self.sell_factor = factors[1]
        
    def score(self, data: pd.DataFrame, signal_engine: SignalEngine | None = None) -> pd.Series:
        entries, exits = self.run(data, signal_engine)
        return entries.astype(int) - exits.astype(int)
    
    def run(self, data: pd.DataFrame, signal_engine: SignalEngine | None = None) -> tuple[pd.Series, pd.Series]:
        # 1️⃣ 收集 signals
        buy_signals = self.buy_factor.signals()
        sell_signals = self.sell_factor.signals()

        # 2️⃣ 生成 engine
        s_engine = signal_engine or SignalEngine()
        # 3️⃣ 生成 signal values which are vectorized (pd.Series)
        result = s_engine.generate(
            data=data,
            signals_list=[buy_signals, sell_signals],
            normalize=True)
        buy_vals, sell_vals = result[0], result[1]

        # 4️⃣ 生成 entry / exit
        entries = self.buy_factor.check(buy_vals)
        exits = self.sell_factor.check(sell_vals)

        return entries, exits
