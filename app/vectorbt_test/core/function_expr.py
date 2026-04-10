from .expr import Expr
import pandas as pd


class FunctionExpr(Expr):
    def __init__(self, *args):
        self.args = args

    def nodes(self):
        nodes = []
        for arg in self.args:
            if isinstance(arg, Expr):
                nodes.extend(arg.nodes())
        return list({n.name: n for n in nodes}.values())
    

class CrossExpr(FunctionExpr):
    def evaluate(self, data, cache, context):
        left = self.args[0].evaluate(data, cache, context)
        right = self.args[1].evaluate(data, cache, context)

        cross = (left > right) & (left.shift(1) <= right.shift(1))
        return cross.astype(int)
    

class RankExpr(FunctionExpr):
    def evaluate(self, data, cache, context):
        x = self.args[0].evaluate(data, cache, context)

        # MultiIndex 情况
        if isinstance(x.index, pd.MultiIndex):
            return x.groupby(level=0).rank(ascending=False)

        # 单股票 fallback
        return x.rank(ascending=False)

   
class TopExpr(FunctionExpr):
    def evaluate(self, data, cache, context):
        n = self.args[0]
        expr = self.args[1]

        x = expr.evaluate(data, cache, context)

        if isinstance(x.index, pd.MultiIndex):
            rank = x.groupby(level=0).rank(ascending=False)
            return (rank <= n).astype(int)

        # 单股票 fallback
        return (x.rank(ascending=False) <= n).astype(int)


class DelayExpr(FunctionExpr):
    def evaluate(self, data, cache, context):
        x = self.args[0].evaluate(data, cache, context)
        n = self.args[1]
        return x.shift(n)


class MeanExpr(FunctionExpr):
    def evaluate(self, data, cache, context):
        x = self.args[0].evaluate(data, cache, context)
        n = self.args[1]
        return x.rolling(n).mean()


class ZScoreExpr(FunctionExpr):
    def evaluate(self, data, cache, context):
        x = self.args[0].evaluate(data, cache, context)
        return (x - x.mean()) / (x.std() + 1e-9)


FUNCTION_REGISTRY = {
    "cross": CrossExpr,
    "rank": RankExpr,
    "top": TopExpr,
    "delay": DelayExpr,
    "mean": MeanExpr,
    "zscore": ZScoreExpr
}