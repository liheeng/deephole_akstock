# signals/volume_signal.py

import pandas as pd
import numpy as np
from vectorbt_test.core.base_signal import BaseSignal

# 规则示例：
# 成交量 > N日均量 × 1.5 → 买
# 成交量 < N日均量 × 0.5 → 卖


class VolumeSignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"VOLUME_{self.period}_{self.multiplier}"

    def __init__(self, period=20, multiplier=1.5):
        self.period = period
        self.multiplier = multiplier

    def generate(self, df: pd.DataFrame):
        vol = df["volume"]
        vol_ma = vol.rolling(self.period).mean()

        # ===== 1️⃣ 相对成交量 =====
        ratio = vol / (vol_ma + 1e-9)

        # ===== 2️⃣ 围绕 multiplier 做中心化 =====
        # multiplier = 1.5 → 只有超过才强
        raw = ratio - self.multiplier

        # ===== 3️⃣ 对称化（让缩量也有负值）=====
        raw = raw / self.multiplier

        # ===== 4️⃣ 压缩范围 =====
        score = np.tanh(raw * 2)

        return score
