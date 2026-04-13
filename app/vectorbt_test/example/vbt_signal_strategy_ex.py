
from db.duckdb import DuckDBController
from db.stock_daily_util import get_symbol_data
import pandas as pd

from vectorbt_test.engine.portofilo_builder import PortfolioBuilder
from vectorbt_test.portfolios.signal_strategy_portfolio import StrategyOp
from vectorbt_test.core.portfolio import PortfolioParameters
from vectorbt_test.engine.data_provider import DataProvider

from vectorbt_test.engine.init import load_nodes

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
    load_nodes()

    db_controller = DuckDBController(db_path="../data/stock.duckdb")
    df = get_symbol_data(db_controller, "603259.SH", "2025-01-01", "2026-03-31")
    # df['date'] = pd.to_datetime(df['date'])
    # df = df.set_index('date')
    # df = df.sort_index()

    data_provider = DataProvider(None)

    # ma20 = MASignal(20)
    # ma60 = MASignal(60)
    # boll = BollSignal(20, 2)
    # macd = MACDSignal()
    # rsi = RSISignal(14)
    # breakout = BreakoutSignal(50)

    # # 趋势策略, 适合趋势明显的股票
    # ma5 = MASignal(5)
    
    builder = (
        PortfolioBuilder
        .new("SP1", mode="signal_strategy")
        .add_strategy("trend")
            # .add_factor("GFactor(name='trend-factor', expr_str='Cross(ma5, ma20)')")
            # .add_factor("GFactor('trend-factor', 'Cross(ma5, ma20)')")
            .add_factor("Cross(ma5, ma20)")
            .end_factor()
        .end_strategy()
        .set_strategy_op(StrategyOp.OR.value)
        .set_portfolio_params(PortfolioParameters(freq="1D", init_cash=10000, top_n=10, hold_days=5))
    ) 
    portfolio = builder.build()

    pf = portfolio.run(data_provider, df)


    print(pf.stats())
    print("==================================================")
    print_trades(pf)
