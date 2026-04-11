from .expr import Expr
from .node import Node, NodeType
import pandas as pd
from vectorbt_test.utils.cs import cs_zscore, cs_rank


class FactorNode(Node):
    def __init__(self, name: str, expr: Expr):
        super().__init__(NodeType.Factor)
        self._name = name
        self.expr = expr

    @property
    def name(self):
        return self._name
    
    def compute(self, data: pd.DataFrame, cache: dict, context: dict | None = None):
        return self.expr.evaluate(data, cache, context or {})

    def score(self, data: pd.DataFrame, cache: dict, context: dict | None = None):
        s = self.compute(data, cache, context)
        return cs_zscore(s)


# NodeRegistry.register(
#     "momentum_score",
#     lambda: DBNode("momentum_score"),
#     NodeMeta(
#         name="momentum_score",
#         group="factor",
#         desc="动量因子"
#     )
# )
# NodeRegistry.register(
#     "close",
#     lambda: DBNode("close"),
#     NodeMeta(
#         name="close",
#         group="raw",
#         desc="收盘价"
#     )
# )

# if __name__ == "__main__":
#     from .expr_parser import NodeBuilder

#     builder = NodeBuilder()

#     expr = builder.build("0.7*ma5 + 0.3*macd")

#     factor = FactorNode("trend", expr)

#     context = {
#         "db_df": df   # 从数据库读取的指标
#     }

#     score = factor.score(df, context)