# signals/atr_signal.py

import pandas as pd
import vectorbt as vbt
from .base_signal import BaseSignal

# 波动率
# ATR 常见用法：
# ATR 上升 → 趋势开始 → 买
# ATR 下降 → 趋势结束 → 卖


class ATRSignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"ATR_{self.period}"

    def __init__(self, period=14):
        self.period = period

    def generate(self, df: pd.DataFrame):
        atr = vbt.ATR.run(
            df["high"],
            df["low"],
            df["close"],
            window=self.period
        ).atr

        signal = pd.Series(0, index=df.index)

        # ATR 上升 → 买
        signal[atr > atr.shift(1)] = 1

        # ATR 下降 → 卖
        signal[atr < atr.shift(1)] = -1

        return signal