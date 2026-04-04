# signals/rsi_signal.py

import pandas as pd
import vectorbt as vbt
from .base_signal import BaseSignal

# 经典规则：
# RSI < 30 → 买
# RSI > 70 → 卖


class RSISignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"RSI_{self.period}_{self.lower}_{self.upper}"

    def __init__(self, period=14, lower=30, upper=70):
        self.period = period
        self.lower = lower
        self.upper = upper

    def generate(self, df: pd.DataFrame):
        rsi = vbt.RSI.run(df["close"], window=self.period).rsi

        signal = pd.Series(0, index=df.index)

        signal[rsi < self.lower] = 1
        signal[rsi > self.upper] = -1

        return signal