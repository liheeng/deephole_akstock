
from app.vectorbt_test.strategy.archieve.strategy_portfolio_v2 import StrategyPortfolioV2
from app.vectorbt_test.strategy.archieve.multi_factors_strategy import MultiFactorStrategy
from vectorbt_test.core.factors import FactorNode
from vectorbt_test.core.node_builder import NodeBuilder

from db.duckdb import DuckDBController
from db.stock_daily_util import get_symbol_data, get_symbols_data
import pandas as pd

import vectorbt_test.core.indicators as indicators


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
    # df = get_symbol_data(db_controller, "603259.SH", "2025-01-01", "2026-03-31")
    df = get_symbols_data(db_controller, "603259.SH, 000063.SZ", "2025-01-01", "2026-03-31")
    df['date'] = pd.to_datetime(df['date'])


    nb = NodeBuilder()
    factor_expr = nb.build("ma5 * 1")

    trend_strategy = MultiFactorStrategy(
        factors=[FactorNode(name="trend_factor", expr=factor_expr)] 
    )

    portfolio = StrategyPortfolioV2(
        strategies=[
            trend_strategy
        ],
        strategy_weights=[1.0],
    )

    pf = portfolio.run(df)

    print(pf.stats())

    print("==================================================")
    print_trades(pf)
