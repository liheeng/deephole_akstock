from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.data_scope import DataScope
from vectorbt_test.engine.data_adapter import DataAdapter
from vectorbt_test.engine.data_provider import DataProvider
import pandas as pd


def create_context(df: pd.DataFrame, data_provider: DataProvider, freq: str = "1D") -> PortfolioContext:
    context = PortfolioContext()
    context.data_provider = data_provider   
    context.data_adapter = DataAdapter(df)

    symbols = df['symbol'].unique().tolist()
    min_date = df['date'].min().strftime("%Y-%m-%d %H:%M:%S")
    max_date = df['date'].max().strftime("%Y-%m-%d %H:%M:%S")
    ds = DataScope(symbols=tuple(symbols), timeframe=freq, start=min_date, end=max_date, dataset="local")
    context.data_scope = ds

    return context