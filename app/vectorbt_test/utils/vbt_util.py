import pandas as pd


def to_vbt(series):
    if isinstance(series.index, pd.MultiIndex):
        df = series.unstack()
        return df.sort_index().ffill()
    return series
