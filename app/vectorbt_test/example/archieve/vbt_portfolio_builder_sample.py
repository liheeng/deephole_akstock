
from vectorbt_test.signals.breakout_signal import BreakoutSignal
from vectorbt_test.signals.macd_signal import MACDSignal
from vectorbt_test.signals.rsi_signal import RSISignal
from vectorbt_test.signals.ma_signal import MASignal
from vectorbt_test.signals.boll_signal import BollSignal
from vectorbt_test.core.signal_expr import S_Expr

from db.duckdb import DuckDBController
from db.stock_daily_util import get_symbol_data
import pandas as pd

from vectorbt_test.engine.portfolio_builder import PortfolioBuilder


def print_trades(pf):
    trades = pf.trades.records_readable

    for _, t in trades.iterrows():
        symbol = t['Column']   # ⭐ 核心
        entry_date = t['Entry Timestamp']
        exit_date = t['Exit Timestamp']
        ret = t['Return'] * 100
        pnl = t['PnL']

        print(f"{symbol} | 买入: {entry_date} 卖出: {exit_date} 收益: {ret:.2f}% PnL: {pnl:.0f}")


if __name__ == "__main__":
    db_controller = DuckDBController(db_path="../data/stock.duckdb")
    df = get_symbol_data(db_controller, "603259.SH", "2025-01-01", "2026-03-31")
    # df['date'] = pd.to_datetime(df['date'])
    # df = df.set_index('date')
    # df = df.sort_index()

    ma20 = MASignal(20)
    ma60 = MASignal(60)
    boll = BollSignal(20, 2)
    macd = MACDSignal()
    rsi = RSISignal(14)
    breakout = BreakoutSignal(50)

    # 趋势策略, 适合趋势明显的股票
    ma5 = MASignal(5)
    
    builder = (
        PortfolioBuilder
        .new("SP1")
        .add_strategy("trend")
            .add_factor("trend_factor")
                .add_expr(
                    S_Expr(ma5, 0.7) +
                    S_Expr(macd, 0.3)
                )
            .end_factor()
        .end_strategy()
        .set_strategy_weights([1.0])
        .set_run_parameters(freq="1D", init_cash=10000)
    )   
    portfolio = builder.build()

    pf = portfolio.run(df)


    print(pf.stats())
    print("==================================================")
    print_trades(pf)
