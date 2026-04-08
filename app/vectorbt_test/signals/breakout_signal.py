# signals/breakout_signal.py

import pandas as pd
import numpy as np
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

        mid = (high_n + low_n) / 2

        # 在区间中的位置
        score = (df["close"] - mid) / (high_n - low_n + 1e-9)

        score = np.tanh(score * 2)

        return score
