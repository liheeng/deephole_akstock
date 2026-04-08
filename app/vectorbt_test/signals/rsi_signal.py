# signals/rsi_signal.py

import pandas as pd
import vectorbt as vbt
import numpy as np
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

        score = pd.Series(0.0, index=df.index)

        # ===== 1️⃣ 超卖区（买）=====
        mask_low = rsi < self.lower
        score[mask_low] = (self.lower - rsi[mask_low]) / self.lower

        # ===== 2️⃣ 超买区（卖）=====
        mask_high = rsi > self.upper
        score[mask_high] = -(rsi[mask_high] - self.upper) / (100 - self.upper)

        # ===== 3️⃣ 压缩（推荐）=====
        score = np.tanh(score * 2)

        return score
