import pandas as pd
from vectorbt_test.core.base import Scope


class ExecutionEngine:

    @staticmethod
    def apply(series: pd.Series, func, scope: Scope):
        # 单资产直接执行
        if not isinstance(series.index, pd.MultiIndex):
            return func(series)

        if scope == Scope.TS:
            # 👉 按 symbol 分组
            return (
                series
                .groupby(level=1)
                .apply(func)
                .reset_index(level=0, drop=True)
            )

        elif scope == Scope.CS:
            # 👉 按 date 分组
            return (
                series
                .groupby(level=0)
                .apply(func)
                .reset_index(level=0, drop=True)
            )

        return func(series)