# signals/boll_signal.py
import pandas as pd
import numpy as np
import vectorbt as vbt
from ..core.base_signal import BaseSignal


class BollSignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"BOLL_{self.period}_{self.std}"

    def __init__(self, period=20, std=2):
        self.period = period
        self.std = std

    def generate(self, df: pd.DataFrame) -> pd.Series:
        bb = vbt.BBANDS.run(
            df["close"],
            window=self.period,
            std=self.std
        )

        mid = bb.middle
        up = bb.upper
        down = bb.lower

        # 距离中轨的标准化位置
        score = (df["close"] - mid) / (up - down + 1e-9)

        # 压缩
        score = np.tanh(score * 2)

        return score
