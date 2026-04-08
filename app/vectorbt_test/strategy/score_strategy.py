import pandas as pd
from vectorbt_test.engine.signal_engine import SignalEngine
from vectorbt_test.strategy.base_strategy import BaseStrategy
from vectorbt_test.engine.signal_expr import SignalGroup


class ScoreStrategy(BaseStrategy):

    def __init__(self, name: str, signals: SignalGroup):
        super().__init__(name, buy_signals=signals, sell_signals=None)

    def score(self, data: pd.DataFrame) -> pd.Series:
        buy_signals = {s.name: s for s in self.buy_signals.signals()}
        sell_signals = None
        if self.sell_signals is not None:
            sell_signals = {s.name: s for s in self.sell_signals.signals()}

        engine = SignalEngine(data, buy_signals, sell_signals)
        buy_vals, sell_vals = engine.generate()

        buy_score = self.buy_signals.score(buy_vals)
        if self.sell_signals is not None:
            sell_score = self.sell_signals.score(sell_vals)
            return buy_score - sell_score
        else:
            return buy_score
