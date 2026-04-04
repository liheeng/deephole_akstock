
from vectorbt_test.signals.breakout_signal import BreakoutSignal
from vectorbt_test.signals.macd_signal import MACDSignal
from vectorbt_test.signals.rsi_signal import RSISignal
from vectorbt_test.strategy.strategy_portfolio import StrategyPortfolio
from vectorbt_test.signals.ma_signal import MASignal
from vectorbt_test.signals.boll_signal import BollSignal
from vectorbt_test.strategy.strategy_base import BaseStrategy
from vectorbt_test.engine.signal_expr import Trigger, buy_signal_expr, sell_signal_expr

from db.duckdb import DuckDBController
from db.stock_daily_util import get_symbol_data
import pandas as pd


def print_trades(pf):
    trades = pf.trades.records_readable

    for _, t in trades.iterrows():
        entry_date = t['Entry Timestamp']
        exit_date = t['Exit Timestamp']
        ret = t['Return'] * 100
        pnl = t['PnL']

        print(f"买入: {entry_date}  卖出: {exit_date}  收益: {ret:.2f}%  PnL: {pnl:.0f}")


if __name__ == "__main__":
    db_controller = DuckDBController(db_path="../data/stock.duckdb")
    df = get_symbol_data(db_controller, "603259.SH", "2025-01-01", "2026-01-01")
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df = df.sort_index()

    ma20 = MASignal(20)
    boll = BollSignal(20, 2)
    macd = MACDSignal()
    rsi = RSISignal(14)
    breakout = BreakoutSignal(50)

    # 趋势策略
    # trend_strategy = BaseStrategy(
    #     "trend",
    #     buy_trigger=Trigger(buy_signal_expr(ma20) & buy_signal_expr(macd)),
    #     sell_trigger=Trigger(sell_signal_expr(macd))
    # )

    # 趋势策略, 适合趋势明显的股票
    ma5 = MASignal(5)
    trend_strategy = BaseStrategy(
        "trend",
        buy_trigger=Trigger(buy_signal_expr(ma5)),
        sell_trigger=Trigger(sell_signal_expr(boll))
        # sell_trigger=Trigger(sell_signal_expr(macd) | sell_signal_expr(rsi))
        # sell_trigger=Trigger(sell_signal_expr(macd))
        # sell_trigger=Trigger(sell_signal_expr(macd) | sell_signal_expr(ma20)) -- 效果一般
        
    )

    # # 震荡策略, 适合区间震荡的股票
    # ma5 = MASignal(5)
    # trend_strategy = BaseStrategy(
    #     "trend",
    #     buy_trigger=Trigger(buy_signal_expr(ma5)),
    #     sell_trigger=Trigger(sell_signal_expr(ma5))
    # )

    portfolio = StrategyPortfolio(
        strategies=[
            trend_strategy
        ],
        weights=[0.4, 0.3, 0.3]
    )

    pf = portfolio.run(df)

    print(pf.stats())

    print("==================================================")
    print_trades(pf)
