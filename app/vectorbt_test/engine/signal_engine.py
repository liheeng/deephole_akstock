# engine/signal_engine.py
from typing import Dict, List
import pandas as pd
import numpy as np
from vectorbt_test.core.base_signal import BaseSignal


class SignalEngine:
    
    def __init__(self):
        self.cache_signals = {}
        
    def generate(self, data: pd.DataFrame, signals_list: List[List[BaseSignal]], normalize: bool = False) -> List[Dict[str, BaseSignal]]:
        self.cache_signals = {}
        return_list_map = []
        for signals in signals_list:
            map_signal_values = {}
            for signal in signals:
                if signal.name in self.cache_signals:
                    map_signal_values[signal.name] = self.cache_signals[signal.name]
                else:
                    map_signal_values[signal.name] = signal.generate(data)
                    if normalize:
                        map_signal_values[signal.name] = map_signal_values[signal.name].fillna(0).apply(np.sign)
                    self.cache_signals[signal.name] = map_signal_values[signal.name]
            
            return_list_map.append(map_signal_values)

        return return_list_map
