import enum
from typing import List, Sequence
import pandas as pd
import vectorbt as vbt
from vectorbt_test.core.signals import Signal
from vectorbt_test.core.portfolio import PortfolioParameters, StrategyPortfolio
from vectorbt_test.core.strategy import StrategyResult
from vectorbt_test.strategies.signal_strategy import SignalStrategy
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.engine.portofilo_builder import create_context


class StrategyOp(enum.Enum):
    OR = "or"
    AND = "and"
    VOTE = "vote"


class SignalStrategyPortfolio(StrategyPortfolio):
    def __init__(self,
                 strategies: Sequence[SignalStrategy],
                 strategy_op: StrategyOp = StrategyOp.AND,
                 schedule_signal: str | Signal | None = None,
                 vote_weights: List[float] | None = None,
                 portfolio_params: PortfolioParameters | None = None):
        super().__init__(strategies, schedule_signal, portfolio_params)
        self.strategy_op = strategy_op
        self.vote_weights = vote_weights
        self.freq = self.params.freq if self.params else "1D"
        self.init_cash = self.params.init_cash if self.params else 100000
        
    def run(self, data_provider: DataProvider, df: pd.DataFrame, freq: str = "1D", init_cash: float = 100000):
        context = create_context(df, data_provider, freq or self.freq)
        
        adapter = context.data_adapter
        data = adapter.data
        close = adapter.to_vbt(df["close"])
        
        # 🔥 bind 一次
        for strat in self.strategies:
            strat.bind_data_adapter(adapter)

        entries = None
        exits = None

        global_schedule = None
        if self.schedule_signal is not None:
            global_schedule = self.schedule_signal.evaluate(data, context)

        if self.strategy_op == StrategyOp.VOTE:
            vote_entries = None
            vote_exits = None

            vote_weights = self.vote_weights if self.vote_weights is not None else [1.0 / len(self.strategies)] * len(self.strategies)
            for strat, w in zip(self.strategies, vote_weights):
                results: StrategyResult = strat.generate(data, context)

                if results.type != 'signal':
                    raise ValueError("Only signal strategies supported")

                e = results.entries.astype(float) * w
                x = results.exits.astype(float) * w

                vote_entries = e if vote_entries is None else vote_entries + e
                vote_exits = x if vote_exits is None else vote_exits + x

            # 🔥 可调参数
            entry_threshold = 0.6
            exit_threshold = 0.4

            print("vote_entries mean:", vote_entries.mean())
            print("vote_exits mean:", vote_exits.mean())

            entries = vote_entries >= entry_threshold
            exits = vote_exits >= exit_threshold

            if global_schedule is not None:
                entries = entries & global_schedule
                exits = exits & global_schedule
        else:
            for strat in self.strategies:
                results: StrategyResult = strat.generate(data, context)

                if results.type != "signal":
                    raise ValueError(f"{strat} is not signal strategy")

                _entries = results.entries
                _exits = results.exits
                if self.strategy_op == StrategyOp.OR:
                    entries = _entries if entries is None else (entries | _entries)
                    exits = _exits if exits is None else (exits | _exits)

                elif self.strategy_op == StrategyOp.AND:
                    entries = _entries if entries is None else (entries & _entries)
                    exits = _exits if exits is None else (exits | _exits)  # 或改成 OR（推荐）
                
        if entries is None or exits is None:
            raise ValueError("No signals generated")

        if global_schedule is not None:
            entries = entries & global_schedule
            exits = exits & global_schedule

        # 🔥 确保 bool
        entries = entries.astype(bool)
        exits = exits.astype(bool)

        # ===== 转 vectorbt =====
        entries = adapter.to_vbt(entries)
        exits = adapter.to_vbt(exits)

        pf = vbt.Portfolio.from_signals(
            close=close,
            entries=entries,
            exits=exits,
            init_cash=init_cash or self.init_cash,
            freq=freq or self.freq,
        )

        return pf
