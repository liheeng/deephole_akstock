from vectorbt_test.core.nodes import Node, NodeType, NodeDType
from vectorbt_test.core.node_builder import NodeBuilder
from vectorbt_test.core.functions import Rank
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.registry import NodeRegistry, NodeMeta
import pandas as pd


# class SignalToFactor(Node):
#     def __init__(self, signal_node):
#         super().__init__(NodeType.Factor, NodeDType.Numeric)
#         self.signal = signal_node

#     def compute(self, data, context):
#         s = self.signal.evaluate(data, context)
#         return s.astype(int)   # 或 float
    

class Factor(Node):
    def __init__(self, name: str, expr_str: str):
        super().__init__(NodeType.Factor, NodeDType.Numeric)
        self._name = name
        self.node = NodeBuilder().build(expr_str)
        # # 🔥 修复点：允许 Signal
        # if self.node.type == NodeType.Signal:
        #     self.node = SignalToFactor(self.node)
        # assert self.node.type == NodeType.Factor

        assert self.type == NodeType.Factor
        
    @property
    def name(self):
        return self._name
    
    def compute(self, data: pd.DataFrame, context: PortfolioContext):
        assert context.data_provider is not None
        return self.node.evaluate(data, context)

    def score(self, data: pd.DataFrame, context: PortfolioContext):
        s = self.evaluate(data, context)
        return context.data_adapter.cs_zscore(s)

    def rank(self):
        return Rank(self)


class GeneralFactor(Factor):
    def __init__(self, name: str, expr_str: str):
        super().__init__(name, expr_str)


NodeRegistry.register(
    "GFactor",
    lambda name, expr_str: GeneralFactor(name, expr_str),
    NodeMeta(
        name="GeneralFactor",
        group="factor",
        desc="GeneralFactor to create general factors"
    ))

# class Rank(Node):
#     def __init__(self, node):
#         super().__init__(node.type)
#         self.node = node

#     def compute(self, data, context):
#         x = self.node.evaluate(data, context)
#         return x.rank(axis=1, pct=True)

    
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
#     from vectorbt_test.core.expr_parser import NodeBuilder

#     builder = NodeBuilder()

#     expr = builder.build("0.7*ma5 + 0.3*macd")

#     factor = FactorNode("trend", expr)

#     context = {
#         "db_df": df   # 从数据库读取的指标
#     }

#     score = factor.score(df, context)