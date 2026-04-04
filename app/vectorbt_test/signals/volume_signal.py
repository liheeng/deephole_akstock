# signals/volume_signal.py

import pandas as pd
from .base_signal import BaseSignal

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
        vol_ma = df["volume"].rolling(self.period).mean()

        signal = pd.Series(0, index=df.index)

        # 放量
        signal[df["volume"] > vol_ma * self.multiplier] = 1

        # 极度缩量
        signal[df["volume"] < vol_ma * 0.5] = -1

        return signal