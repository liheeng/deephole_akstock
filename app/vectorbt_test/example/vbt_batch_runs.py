from vectorbt_test.signals.breakout_signal import BreakoutSignal
from vectorbt_test.signals.macd_signal import MACDSignal
from vectorbt_test.signals.rsi_signal import RSISignal
from vectorbt_test.strategy.strategy_portfolio import StrategyPortfolio
from vectorbt_test.signals.ma_signal import MASignal
from vectorbt_test.signals.boll_signal import BollSignal
from vectorbt_test.strategy.rule_strategy import RuleStrategy
from vectorbt_test.core.factor import Factor
from vectorbt_test.core.signal_expr import buy_signal_expr, sell_signal_expr

from db.duckdb import DuckDBController
from db.stock_daily_util import get_symbol_data
import pandas as pd

from concurrent.futures import ProcessPoolExecutor, as_completed
import os


def run_single_backtest(symbol, db_path):
    try:
        db_controller = DuckDBController(db_path=db_path)

        df = get_symbol_data(db_controller, symbol, "2025-01-01", "2026-03-31")
        if df is None or len(df) < 100:
            return None

        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()

        # === signals ===
        ma5 = MASignal(5)
        macd = MACDSignal()
        rsi = RSISignal(14)
        breakout = BreakoutSignal(50)

        # === strategy ===
        strategy = RuleStrategy(
            "trend",
            factors=[
                Factor(name="buy", expr=buy_signal_expr(ma5)),
                Factor(name="sell", expr=sell_signal_expr(macd) | sell_signal_expr(rsi) | sell_signal_expr(breakout))
            ]
        )

        portfolio = StrategyPortfolio(
            strategies=[strategy],
            strategy_weights=[1.0]
        )

        pf = portfolio.run(df)

        stats = pf.stats()

        return {
            "symbol": symbol,
            "total_return(%)": stats["Total Return [%]"],
            "sharpe": stats.get("Sharpe Ratio", None),
            "max_dd(%)": stats.get("Max Drawdown [%]", None),
            "trades": stats.get("Total Trades", None)
        }

    except Exception as e:
        print(f"❌ {symbol} error: {e}")
        return None
    

def run_all_backtests(db_path, max_workers=16):
    db_controller = DuckDBController(db_path=db_path)

    symbols = db_controller.read(
        "SELECT DISTINCT symbol FROM stock_daily where market = 'CN'",
        fetch_mode="all"
    )
    symbols = [s[0] for s in symbols]

    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(run_single_backtest, symbol, db_path)
            for symbol in symbols
        ]

        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    return pd.DataFrame(results)


if __name__ == "__main__":
    df_result = run_all_backtests("../data/stock.duckdb", max_workers=20)

    if len(df_result) != 0:
        
        df_result = df_result.sort_values(by="total_return(%)", ascending=False)

        print(df_result.head(20))

        # 保存
        df_result.to_csv("backtest_result.csv", index=False)