
from vectorbt_test.signals.breakout_signal import BreakoutSignal
from vectorbt_test.signals.macd_signal import MACDSignal
from vectorbt_test.signals.rsi_signal import RSISignal
from vectorbt_test.strategy.archieve.strategy_portfolio_v2 import StrategyPortfolio
from vectorbt_test.signals.ma_signal import MASignal
from vectorbt_test.signals.boll_signal import BollSignal
from vectorbt_test.strategy.archieve.rule_strategy import RuleStrategy
from vectorbt_test.core.factor import Factor
from vectorbt_test.core.signal_expr import buy_signal_expr, sell_signal_expr

if __name__ == "__main__":
    ma20 = MASignal(20)
    boll = BollSignal(20, 2)
    macd = MACDSignal()
    rsi = RSISignal(14)
    breakout = BreakoutSignal(50)

    # 趋势策略
    trend_strategy = RuleStrategy(
        "trend",
        factors=[
            Factor(name="buy", expr=buy_signal_expr(ma20) & buy_signal_expr(macd)),
            Factor(name="sell", expr=sell_signal_expr(macd))
        ]
    )

    # 均值回归
    mean_rev_strategy = RuleStrategy(
        "mean_rev",
        factors=[
            Factor(name="buy", expr=buy_signal_expr(rsi) & buy_signal_expr(boll)),
            Factor(name="sell", expr=sell_signal_expr(rsi))
        ]
    )

    # 突破
    breakout_strategy = RuleStrategy(
        "breakout",
        factors=[
            Factor(name="buy", expr=buy_signal_expr(breakout)),
            Factor(name="sell", expr=sell_signal_expr(breakout))
        ]
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