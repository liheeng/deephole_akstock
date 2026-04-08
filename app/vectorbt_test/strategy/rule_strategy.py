from vectorbt_test.strategy.base_strategy import BaseStrategy
from vectorbt_test.engine.signal_expr import SignalGroup
from vectorbt_test.engine.signal_engine import SignalEngine


class RuleStrategy(BaseStrategy):
    def __init__(self, name: str, buy_signals: SignalGroup, sell_signals: SignalGroup | None):
        super().__init__(name, buy_signals, sell_signals)
        if self.sell_signals is None:
            raise ValueError("Missing sell signals parameter")

    def score(self, data):
        entries, exits = self.run(data)
        return entries.astype(int) - exits.astype(int)
    
    def run(self, data: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        # 1️⃣ 收集 signals
        buy_signals = {s.name: s for s in self.buy_signals.signals()}
        sell_signals = None
        if self.sell_signals is not None:
            sell_signals = {s.name: s for s in self.sell_signals.signals()}

        # 2️⃣ 生成 engine
        engine = SignalEngine(data, buy_signals, sell_signals)

        # 3️⃣ 生成 signal values which are vectorized (pd.Series)
        buy_vals, sell_vals = engine.generate(normalize=True)

        # 4️⃣ 生成 entry / exit
        entries = self.buy_signals.check(buy_vals)
        exits = self.sell_signals.check(sell_vals)

        return entries, exits
