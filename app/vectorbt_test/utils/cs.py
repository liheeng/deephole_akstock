# utils/cs.py

import pandas as pd
from typing import List


def cs_rank(x, ascending=False):
    if isinstance(x.index, pd.MultiIndex):
        return x.groupby(level=0).rank(ascending=ascending)

    if isinstance(x, pd.DataFrame):
        return x.rank(axis=1, ascending=ascending)

    return x.rank(ascending=ascending)


def cs_normalize(x):
    if isinstance(x.index, pd.MultiIndex):
        denom = x.abs().groupby(level=0).sum()
        return x / denom.replace(0, 1)

    if isinstance(x, pd.DataFrame):
        return x.div(x.abs().sum(axis=1).replace(0, 1), axis=0)

    # 单标
    denom = x.abs().sum()
    return x / (denom if denom != 0 else 1)


def cs_zscore(x):
    # ===== 情况1：MultiIndex（最优先判断）=====
    if isinstance(x.index, pd.MultiIndex):
        return x.groupby(level=0).transform(
            lambda v: (v - v.mean()) / (v.std() + 1e-9)
        )

    # ===== 情况2：DataFrame（多标）=====
    if isinstance(x, pd.DataFrame):
        return x.sub(x.mean(axis=1), axis=0).div(x.std(axis=1) + 1e-9, axis=0)

    # ===== 情况3：Series（单标）=====
    return (x - x.mean()) / (x.std() + 1e-9)


def is_cross_section(x):
    if isinstance(x.index, pd.MultiIndex):
        # 判断每个时间点是否有多个资产
        return x.index.get_level_values(0).nunique() < len(x)

    if isinstance(x, pd.DataFrame):
        return x.shape[1] > 1

    return False


def orthogonalize_factors(factors: List[pd.DataFrame]):
    ortho = []

    for f in factors:
        f_new = f.copy()

        for prev in ortho:
            # 投影
            denom = (prev * prev).sum(axis=1)
            denom = denom.replace(0, 1e-9)

            beta = (f_new * prev).sum(axis=1) / denom
            beta = beta.replace([float("inf"), -float("inf")], 0).fillna(0)

            f_new = f_new - prev.mul(beta, axis=0)

        ortho.append(f_new)

    return ortho

