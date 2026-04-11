from vectorbt_test.core.factor import FactorNode


class SingleAssetStrategy:
    def __init__(self, factor: FactorNode, threshold=0):
        self.factor = factor
        self.threshold = threshold

    def generate(self, data, cache, context: dict | None = None):
        f = self.factor.score(data, cache, context)

        entries = f > self.threshold
        exits = f < -self.threshold

        return entries, exits