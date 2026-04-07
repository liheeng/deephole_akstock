import time
from typing import List
import pandas as pd
import vectorbt as vbt
from vectorbt_test.strategy.strategy_base import BaseStrategy


class StrategyPortfolio:
    def __init__(self, strategies: List[BaseStrategy], weights: List[float] = [1]):
        self.strategies = strategies
        self.weights = weights or [1.0] * len(strategies)

    def run(self, data: pd.DataFrame, freq: str = "1D", init_cash: float = 100000) -> vbt.Portfolio:
        combined_signal = None

        for strat, w in zip(self.strategies, self.weights):
            entries, exits = strat.run(data)

            signal = entries.astype(int) - exits.astype(int)

            weighted_signal = signal * w

            if combined_signal is None:
                combined_signal = weighted_signal
            else:
                combined_signal += weighted_signal

        final_entries = combined_signal > 0
        final_exits = combined_signal < 0

        t0 = time.time()
        pf = vbt.Portfolio.from_signals(
            close=data["close"],
            entries=final_entries,
            exits=final_exits,
            init_cash=init_cash,
            freq=freq,
        )
        print("vectorbt run time:", time.time() - t0)

        return pf
