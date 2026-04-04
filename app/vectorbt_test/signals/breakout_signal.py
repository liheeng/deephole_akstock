# signals/breakout_signal.py

import pandas as pd
from .base_signal import BaseSignal

# 规则：
# 突破 N 日最高 → 买
# 跌破 N 日最低 → 卖


class BreakoutSignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"BREAKOUT_{self.period}"

    def __init__(self, period=20):
        self.period = period

    def generate(self, df: pd.DataFrame):
        high_n = df["high"].rolling(self.period).max()
        low_n = df["low"].rolling(self.period).min()

        signal = pd.Series(0, index=df.index)

        # 突破前N日最高
        signal[df["close"] > high_n.shift(1)] = 1

        # 跌破前N日最低
        signal[df["close"] < low_n.shift(1)] = -1

        return signal