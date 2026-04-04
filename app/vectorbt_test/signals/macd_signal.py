import pandas as pd
import vectorbt as vbt
from .base_signal import BaseSignal


class MACDSignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"MACD_{self.fast}_{self.slow}_{self.signal}"

    def __init__(self, fast=12, slow=26, signal=9):
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def generate(self, df) -> pd.Series:
        
        macd = vbt.MACD.run(
            df["close"],
            fast_window=self.fast,
            slow_window=self.slow,
            signal_window=self.signal
        )

        m = macd.macd
        s = macd.signal

        signal_series = pd.Series(0, index=df.index)

        cross_up = (m > s) & (m.shift(1) <= s.shift(1))
        cross_down = (m < s) & (m.shift(1) >= s.shift(1))

        signal_series[cross_up] = 1
        signal_series[cross_down] = -1

        return signal_series