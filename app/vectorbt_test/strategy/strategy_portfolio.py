import time
from typing import List
import pandas as pd
import vectorbt as vbt
from vectorbt_test.core.base_strategy import BaseStrategy
from vectorbt_test.engine.signal_engine import SignalEngine


class StrategyPortfolio:
    def __init__(self, strategies: List[BaseStrategy], strategy_weights: List[float] | None = None, buy_threshold: float = 0.0, sell_threshold: float = 0.0):
        self.strategies = strategies
        self.strategy_weights = strategy_weights or [1.0 / len(self.strategies)] * len(self.strategies)
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def run(self, data: pd.DataFrame, freq: str = "1D", init_cash: float = 100000) -> vbt.Portfolio:
        alpha = None

        signals_engine = SignalEngine()
        for strat, w in zip(self.strategies, self.strategy_weights):
            score = strat.score(data, signals_engine)

            weighted = score * w

            if alpha is None:
                alpha = weighted
            else:
                alpha += weighted

        # 👉 统一决策
        final_entries = alpha > self.buy_threshold
        final_exits = alpha < self.sell_threshold

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
