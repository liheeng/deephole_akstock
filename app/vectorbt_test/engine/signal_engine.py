# engine/signal_engine.py
from typing import Dict, List
import pandas as pd
import numpy as np
from vectorbt_test.core.base_signal import BaseSignal


class SignalEngine:
    
    def __init__(self):
        self.cache_signals = {}
        
    def generate(self, data: pd.DataFrame, signals: List[BaseSignal], normalize=False) -> Dict[str, pd.Series]:
        result = {}

        for signal in signals:
            if signal.name in self.cache_signals:
                result[signal.name] = self.cache_signals[signal.name]
            else:
                val = signal.generate(data)

                if normalize:
                    val = val.fillna(0).apply(np.sign)

                self.cache_signals[signal.name] = val
                result[signal.name] = val

        return result
