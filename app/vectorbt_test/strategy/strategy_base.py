import pandas as pd
import vectorbt as vbt
from vectorbt_test.engine.signal_engine import SignalEngine
from vectorbt_test.engine.signal_expr import Trigger


class BaseStrategy:
    def __init__(self, name: str, buy_trigger: Trigger, sell_trigger: Trigger):
        self.name = name
        self.buy_trigger = buy_trigger
        self.sell_trigger = sell_trigger

    def run(self, data: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        # 1️⃣ 收集 signals
        buy_signals = {s.name: s for s in self.buy_trigger.signals()}
        sell_signals = {s.name: s for s in self.sell_trigger.signals()}

        # 2️⃣ 生成 engine
        engine = SignalEngine(buy_signals, sell_signals, data)

        # 3️⃣ 生成 signal values which are vectorized (pd.Series)
        buy_vals, sell_vals = engine.generate()

        # 4️⃣ 生成 entry / exit
        entries = self.buy_trigger.check(buy_vals)
        exits = self.sell_trigger.check(sell_vals)

        return entries, exits
        
        # # 5️⃣ 回测
        # pf = vbt.Portfolio.from_signals(
        #     close=data["close"],
        #     entries=entries,
        #     exits=exits,
        #     init_cash=100000
        # )

        # return pf
