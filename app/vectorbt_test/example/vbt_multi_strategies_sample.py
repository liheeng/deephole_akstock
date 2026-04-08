
from vectorbt_test.signals.breakout_signal import BreakoutSignal
from vectorbt_test.signals.macd_signal import MACDSignal
from vectorbt_test.signals.rsi_signal import RSISignal
from vectorbt_test.strategy.strategy_portfolio import StrategyPortfolio
from vectorbt_test.signals.ma_signal import MASignal
from vectorbt_test.signals.boll_signal import BollSignal
from vectorbt_test.strategy.rule_strategy import RuleStrategy
from vectorbt_test.engine.signal_expr import SignalGroup, buy_signal_expr, sell_signal_expr

if __name__ == "__main__":
    ma20 = MASignal(20)
    boll = BollSignal(20, 2)
    macd = MACDSignal()
    rsi = RSISignal(14)
    breakout = BreakoutSignal(50)

    # 趋势策略
    trend_strategy = RuleStrategy(
        "trend",
        buy_signals=SignalGroup(buy_signal_expr(ma20) & buy_signal_expr(macd)),
        sell_signals=SignalGroup(sell_signal_expr(macd))
    )

    # 均值回归
    mean_rev_strategy = RuleStrategy(
        "mean_rev",
        buy_signals=SignalGroup(buy_signal_expr(rsi) & buy_signal_expr(boll)),
        sell_signals=SignalGroup(sell_signal_expr(rsi))
    )

    # 突破
    breakout_strategy = RuleStrategy(
        "breakout",
        buy_signals=SignalGroup(buy_signal_expr(breakout)),
        sell_signals=SignalGroup(sell_signal_expr(breakout))
    )

    portfolio = StrategyPortfolio(
        strategies=[
            trend_strategy,
            mean_rev_strategy,
            breakout_strategy
        ],
        strategy_weights=[0.4, 0.3, 0.3]
    )

    pf = portfolio.run(df)

    print(pf.stats())