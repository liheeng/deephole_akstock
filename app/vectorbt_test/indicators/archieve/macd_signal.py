import pandas as pd
import vectorbt as vbt
import numpy as np
from vectorbt_test.core.base_signal import BaseSignal


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

        hist = m - s

        # ===== 1️⃣ 基础趋势强度 =====
        score = hist / (hist.rolling(20).std() + 1e-9)

        # ===== 2️⃣ 交叉检测 =====
        cross_up = (m > s) & (m.shift(1) <= s.shift(1))
        cross_down = (m < s) & (m.shift(1) >= s.shift(1))

        # ===== 3️⃣ 交叉增强（关键）=====
        score[cross_up] += 1.0
        score[cross_down] -= 1.0

        # ===== 4️⃣ 压缩范围 =====
        score = np.tanh(score)

        return score
