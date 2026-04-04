# signals/ma_signal.py

from .base_signal import BaseSignal
import pandas as pd
import vectorbt as vbt


class MASignal(BaseSignal):
    @property
    def name(self):
        return f"MA_{self.period}"
    
    def __init__(self, period=20):
        self.period = period

    def generate(self, data: pd.DataFrame) -> pd.Series:
        sma = vbt.MA.run(data["close"], self.period).ma
        signal = (data["close"] > sma).astype(int)
        signal[data["close"] < sma] = -1
        return signal

    # def generate(self, data: pd.DataFrame) -> pd.Series:
    #     sma = data["close"].rolling(self.period).mean()

    #     signal = pd.Series(0, index=data.index)

    #     signal[data["close"] > sma] = 1
    #     signal[data["close"] < sma] = -1

    #     return signal
