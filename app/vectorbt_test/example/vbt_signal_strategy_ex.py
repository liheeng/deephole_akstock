
from db.duckdb import DuckDBController
from db.stock_daily_util import get_symbol_data, get_symbols_data

from vectorbt_test.engine.portfolio_builder import PortfolioBuilder
from vectorbt_test.portfolios.signal_strategy_portfolio import StrategyOp
from vectorbt_test.core.portfolio import PortfolioParameters, PortfolioResultWrapper
from vectorbt_test.engine.data_provider import DataProvider
from vectorbt_test.engine.init import load_register_nodes


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
    load_register_nodes()

    # dd = NodeRegistry.to_dict()
    
    db_controller = DuckDBController(db_path="../data/stock.duckdb")
    # df = get_symbol_data(db_controller, "603259.SH", "2025-01-01", "2026-03-31")
    df = get_symbols_data(db_controller, "603259.SH, 600362.SH", "2025-01-01", "2026-03-31")

    data_provider = DataProvider(None)
    
    builder = (
        PortfolioBuilder
        .new("SP1", portfolio_mode="signal_strategy")
        .add_strategy("trend")
            .add_factor("(MA(5) - MA(20)) / MA(20)")
            .end_factor()
        .end_strategy()
        .set_strategy_op(StrategyOp.OR.value)
        .set_schedule_signal("RSI(14) > 70")
        .set_portfolio_params(PortfolioParameters(freq="1D", init_cash=10000, top_n=10, hold_days=5))
    ) 
    portfolio = builder.build()

    pf = portfolio.run(data_provider, df)

    pfwrapper = PortfolioResultWrapper(pf)
    stats = pfwrapper.get_pf_stats()
    equity = pfwrapper.get_pf_value_dict()

    print("== stats ================================================")
    print(stats)
    print("== equity ================================================")
    print(equity)
    print("== trades ================================================")
    print_trades(pf)
