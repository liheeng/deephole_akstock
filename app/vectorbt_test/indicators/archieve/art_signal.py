# signals/atr_signal.py

import pandas as pd
import numpy as np
import vectorbt as vbt
from vectorbt_test.core.base_signal import BaseSignal

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

        atr_ma = atr.rolling(20).mean()

        # 波动强度
        score = (atr - atr_ma) / (atr_ma + 1e-9)

        score = np.tanh(score)

        return score
