# engine/signal_engine.py
from typing import Dict
import pandas as pd
import numpy as np
from vectorbt_test.signals.base_signal import BaseSignal


class SignalEngine:
    def __init__(self, data: pd.DataFrame, buy_signals: Dict[str, BaseSignal], sell_signals: Dict[str, BaseSignal] | None = None):
        self.data = data
        self.buy_signals = buy_signals
        self.sell_signals = sell_signals
        
    def generate(self, normalize: bool = False):
        cache_signals = {}
        buy_signal_values = {}
        for name, signal in self.buy_signals.items():
            if signal.name in cache_signals:
                buy_signal_values[name] = cache_signals[signal.name]
            else:
                buy_signal_values[name] = signal.generate(self.data)
                if normalize:
                    buy_signal_values[name] = buy_signal_values[name].fillna(0).apply(np.sign)
                cache_signals[signal.name] = buy_signal_values[name]

        sell_signal_values = {}
        if self.sell_signals is not None:
            for name, signal in self.sell_signals.items():
                if signal.name in cache_signals:
                    sell_signal_values[name] = cache_signals[signal.name]
                else:
                    sell_signal_values[name] = signal.generate(self.data)
                    sell_signal_values[name] = sell_signal_values[name].fillna(0).apply(np.sign)
                    cache_signals[signal.name] = sell_signal_values[name]

        return buy_signal_values, sell_signal_values
