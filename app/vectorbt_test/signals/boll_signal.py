# signals/boll_signal.py
import pandas as pd
from .base_signal import BaseSignal


class BollSignal(BaseSignal):
    @property
    def name(self) -> str:
        return f"BOLL_{self.period}_{self.std}"

    def __init__(self, period=20, std=2):
        self.period = period
        self.std = std

    def generate(self, data: pd.DataFrame) -> pd.Series:
        close = data["close"]

        ma = close.rolling(self.period).mean()
        std = close.rolling(self.period).std()

        upper = ma + self.std * std
        lower = ma - self.std * std

        signal = pd.Series(0, index=data.index)

        # 突破下轨 → 买入
        signal[close < lower] = 1

        # 突破上轨 → 卖出
        signal[close > upper] = -1

        return signal
