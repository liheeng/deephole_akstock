from vectorbt_test.core.factors import FactorNode   


class HybridStrategy:
    def __init__(self, factor: FactorNode, threshold=0, top_n=None):
        self.factor = factor
        self.threshold = threshold
        self.top_n = top_n

    def generate(self, data, cache, context: dict | None = None):
        f = self.factor.score(data, cache, context)

        # 1️⃣ 过滤垃圾信号（关键）
        mask = f > self.threshold

        # 2️⃣ rank（如果多标的）
        if self.top_n:
            ranks = f.rank(axis=1, ascending=False)
            mask &= (ranks <= self.top_n)

        # 3️⃣ score 加权
        weights = f * mask

        # 4️⃣ normalize
        weights = weights.div(weights.abs().sum(axis=1), axis=0)

        return weights