import time
from typing import List
import pandas as pd
import vectorbt as vbt
from vectorbt_test.strategy.base_strategy import BaseStrategy


class StrategyPortfolio:
    def __init__(self, strategies: List[BaseStrategy], strategy_weights: List[float] = [1.0], threshold: tuple[float, float] = (0, 0)):
        self.strategies = strategies
        self.strategy_weights = strategy_weights or [1.0] * len(strategies)
        self.threshold = threshold

    def run(self, data: pd.DataFrame, freq: str = "1D", init_cash: float = 100000) -> vbt.Portfolio:
        alpha = None

        for strat, w in zip(self.strategies, self.strategy_weights):
            score = strat.score(data)

            weighted = score * w

            if alpha is None:
                alpha = weighted
            else:
                alpha += weighted

        # 👉 统一决策
        final_entries = alpha > self.threshold[0]
        final_exits = alpha < self.threshold[1]

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
