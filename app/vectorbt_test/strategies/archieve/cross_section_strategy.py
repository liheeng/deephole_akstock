from vectorbt_test.core.factor import FactorNode


class CrossSectionStrategy:
    def __init__(self, factor: FactorNode, top_n=10):
        self.factor = factor
        self.top_n = top_n

    def generate(self, data, cache, context: dict | None = None):
        f = self.factor.score(data, cache, context)

        ranks = f.rank(axis=1, ascending=False)

        weights = (ranks <= self.top_n).astype(float)
        weights = weights.div(weights.sum(axis=1), axis=0)

        return weights