import pandas as pd
from vectorbt_test.core.factors import FactorNode
from typing import List
from vectorbt_test.core.strategy import Strategy
from vectorbt_test.utils.cs import cs_rank, cs_normalize, orthogonalize_factors


class MultiFactorStrategy(Strategy):

    def __init__(self, factors: List[FactorNode], factor_weights: List[float] | None = None, threshold=0, top_n=None):
        self.factors = factors
        self.factor_weights = factor_weights or [1.0 / len(self.factors)] * len(self.factors)
        self.threshold = threshold
        self.top_n = top_n

    def generate(self, data: pd.DataFrame, cache: dict, context: dict | None = None):
        # ===== 1. 计算 factors =====
        factors_list = [
            factor.score(data, cache=cache, context=context)
            for factor in self.factors
        ]

        # ===== 1.1 排序（可选但推荐）=====
        factors_list = sorted(
            factors_list,
            key=lambda x: x.abs().mean().mean(),
            reverse=True
        )

        # ===== 2. 去相关 =====
        factors_list = orthogonalize_factors(factors_list)

        # ===== 3. 合成 alpha =====
        alpha = None
        for f, w in zip(factors_list, self.factor_weights):
            alpha = f * w if alpha is None else alpha + f * w

        # ===== 4. mask =====
        mask = alpha.notna()

        if self.threshold is not None:
            mask &= alpha > self.threshold

        if self.top_n:
            ranks = cs_rank(alpha, ascending=False)
            mask &= ranks <= self.top_n

        # ===== 5. 权重 =====
        weights = alpha.where(mask, 0.0)

        # ===== 6. normalize + 防 NaN =====
        weights = cs_normalize(weights).fillna(0)

        return weights
