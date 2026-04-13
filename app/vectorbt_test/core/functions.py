from vectorbt_test.core.nodes import Node, FeatureNode, NodeType, NodeDType, ConstNode, to_node
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.base import Scope


class Function(FeatureNode):
    def __init__(self, *args, dtype, node_type=NodeType.Factor):
        super().__init__(node_type, dtype)
        self.args = [to_node(a) for a in args]

    def _args(self):
        return [a.cache_key() for a in self.args]
 

class Cross(Function):

    def __init__(self, left, right):
        super().__init__(left, right,
                         dtype=NodeDType.Signal,
                         node_type=NodeType.Signal)

    def compute(self, data, context: PortfolioContext):
        left = self.args[0].evaluate(data, context)
        right = self.args[1].evaluate(data, context)

        cross = (left > right) & (left.shift(1) <= right.shift(1))
        return cross.astype(bool)
    

class Rank(Function):
    scope = Scope.CS

    def __init__(self, x):
        super().__init__(x, dtype=NodeDType.Numeric)

    def compute(self, data, context: PortfolioContext):
        x = self.args[0].evaluate(data, context)

        # if isinstance(x.index, pd.MultiIndex):
        #     return x.groupby(level=0).rank(ascending=False)

        # return x.rank(ascending=False)
        return self.apply(
            x,
            lambda df: df.rank(ascending=False),
            context
        )


def get_value(x, data, context: PortfolioContext):
    if isinstance(x, Node):
        return x.evaluate(data, context)
    return x


class Top(Function):
    scope = Scope.CS

    def __init__(self, n, x):
        super().__init__(n, x, dtype=NodeDType.Bool)

    def compute(self, data, context: PortfolioContext):
        
        n = self.args[0].value if isinstance(self.args[0], ConstNode) else self.args[0]
        x = get_value(self.args[1], data, context)

        # if isinstance(x.index, pd.MultiIndex):
        #     rank = x.groupby(level=0).rank(ascending=False)
        #     return (rank <= n).astype(bool)

        # return (x.rank(ascending=False) <= n).astype(bool)
        return self.apply(
            x,
            lambda df: (df.rank(ascending=False) <= n),
            context
        ).astype(bool)


class Delay(Function):
    def __init__(self, x, n):
        super().__init__(x, n, dtype=NodeDType.Numeric)

    def compute(self, data, context: PortfolioContext):
        x = self.args[0].evaluate(data, context)
        n = self.args[1].value
        return x.shift(n)


class Mean(Function):
    def __init__(self, x, n):
        super().__init__(x, n, dtype=NodeDType.Numeric)

    def compute(self, data, context: PortfolioContext):
        x = self.args[0].evaluate(data, context)
        n = self.args[1].value
        return x.rolling(n).mean()


class ZScore(Function):
    scope = Scope.CS

    def __init__(self, x):
        super().__init__(x, dtype=NodeDType.Numeric)

    def compute(self, data, context: PortfolioContext):
        x = self.args[0].evaluate(data, context)
        
        return self.apply(
            x,
            lambda s: (s - s.mean()) / (s.std() + 1e-9),
            context
        )


class ZScoreTS(Node):
    scope = Scope.TS

    def __init__(self, node, window):
        self.node = node
        self.window = window

    def compute(self, data, context):
        x = self.node.evaluate(data, context)

        return self.apply(
            x,
            lambda s: (s - s.rolling(self.window).mean()) /
                      (s.rolling(self.window).std() + 1e-9),
            context
        )


NodeRegistry.register(
    "cross",
    lambda a, b: Cross(a, b),
    NodeMeta(
        name="cross",
        group="function",
        desc="Cross计算",
        params=[
            NodeParam("left", "Node", desc="左节点"),
            NodeParam("right", "Node", desc="右节点")
        ]
    ))

NodeRegistry.register(
    "rank",
    lambda x: Rank(x),
    NodeMeta(
        name="rank",
        group="function",
        desc="Rank计算",
        params=[
            NodeParam("x", "Node", desc="节点")
        ]
    ))

NodeRegistry.register(
    "top",
    lambda n, x: Top(n, x),
    NodeMeta(
        name="top",
        group="function",
        desc="Top计算",
        params=[
            NodeParam("n", "int", desc="阈值"),
            NodeParam("x", "Node", desc="节点")
        ]
    ))

NodeRegistry.register(
    "delay",
    lambda x, n: Delay(x, n),
    NodeMeta(
        name="delay",
        group="function",
        desc="Delay计算",
        params=[
            NodeParam("x", "Node", desc="节点"),
            NodeParam("n", "int", desc="阈值")
        ]
    ))

NodeRegistry.register(
    "mean",
    lambda x, n: Mean(x, n),
    NodeMeta(
        name="mean",
        group="function",
        desc="Mean计算",
        params=[
            NodeParam("x", "Node", desc="节点"),
            NodeParam("n", "int", desc="阈值")
        ]
    ))

NodeRegistry.register(
    "zscore",
    lambda x: ZScore(x),
    NodeMeta(
        name="zscore",
        group="function",
        desc="ZScore计算",
        params=[
            NodeParam("x", "Node", desc="节点")
        ]
    )
)

NodeRegistry.register(
    "zscore_ts",
    lambda x, window: ZScoreTS(x, window),
    NodeMeta(
        name="zscore",
        group="function",
        desc="基于TS的ZScore计算",
        params=[
            NodeParam("x", "Node", desc="节点"),
            NodeParam("window", "int", desc="窗口")
        ]
    )
)
