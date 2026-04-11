# signals/ma_signal.py

from ..core.base_signal import BaseSignal
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
       
        # 👉 直接返回 score（不是 signal）
        return (data["close"] - sma) / sma
    
