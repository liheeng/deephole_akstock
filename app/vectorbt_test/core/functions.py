from vectorbt_test.core.nodes import Node, FeatureNode, NodeType, NodeDType, ConstNode, to_args
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.core.base import Scope
from utils.group_func_registry import GroupFuncReg


class Function(FeatureNode):
    def __init__(self):
        super().__init__(type=NodeType.Factor)


class Cross(Function):
    dtype = NodeDType.BOOL

    def __init__(self, left, right):
        if left.dtype != NodeDType.NUMERIC or right.dtype != NodeDType.NUMERIC:
            raise TypeError("Cross requires numeric inputs")
        super().__init__()
        self.left = left
        self.right = right

    def _args(self):
        return [self.left.cache_key(), self.right.cache_key()] + super()._args()
    
    def compute(self, data, context: PortfolioContext):
        a = self.left.evaluate(data, context)
        b = self.right.evaluate(data, context)

        # return (a > b) & (a.shift(1) <= b.shift(1))
        return self.apply(
            a,
            lambda x: (x > b.loc[x.index]) & (x.shift(1) <= b.loc[x.index].shift(1)),
            context
        )
    

class Rank(Function):
    scope = Scope.CS
    dtype = NodeDType.NUMERIC

    def __init__(self, node):
        if node.dtype != NodeDType.NUMERIC:
            raise TypeError("rank() requires numeric input")
        super().__init__()
        self.node = node

    def _args(self):
        return [self.node.cache_key()] + super()._args()
    
    def compute(self, data, context: PortfolioContext):
        x = self.node.evaluate(data, context)

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
    dtype = NodeDType.BOOL

    def __init__(self, node, window):
        super().__init__()
        self.window = window
        self.node = node

    def _args(self):
        return [self.node.cache_key(), self.window.cache_key() if isinstance(self.window, ConstNode) else self.window] + super()._args()

    def compute(self, data, context: PortfolioContext):
        
        n = self.window.value if isinstance(self.window, ConstNode) else self.window
        x = get_value(self.node, data, context)

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
    dtype = NodeDType.ANY

    def __init__(self, node, window):
        if node.dtype != NodeDType.NUMERIC:
            raise TypeError("mean requires numeric input")
        super().__init__()
        self.node = node
        self.window = window

    def _args(self):
        return [self.node.cache_key(), self.window.cache_key() if isinstance(self.window, ConstNode) else self.window] + super()._args()

    def compute(self, data, context: PortfolioContext):
        x = self.node.evaluate(data, context)
        n = self.window.value if isinstance(self.window, ConstNode) else self.window
        return x.shift(n)


class Mean(Function):
    dtype = NodeDType.NUMERIC

    def __init__(self, node, window):
        if node.dtype != NodeDType.NUMERIC:
            raise TypeError("mean requires numeric input")
        super().__init__()
        self.node = node
        self.window = window

    def _args(self):
        return [self.node.cache_key(), self.window.cache_key() if isinstance(self.window, ConstNode) else self.window] + super()._args()

    def compute(self, data, context: PortfolioContext):
        x = self.node.evaluate(data, context)
        n = self.window.value if isinstance(self.window, ConstNode) else self.window
        return x.rolling(n).mean()


class ZScore(Function):
    scope = Scope.CS
    dtype = NodeDType.NUMERIC

    def __init__(self, node):
        if node.dtype != NodeDType.NUMERIC:
            raise TypeError("zscore() requires numeric input")
        super().__init__()
        self.node = node

    def _args(self):
        return [self.node.cache_key()] + super()._args()

    def compute(self, data, context: PortfolioContext):
        x = self.node.evaluate(data, context)
        
        return self.apply(
            x,
            lambda s: (s - s.mean()) / (s.std() + 1e-9),
            context
        )


class ZScoreTS(Function):
    scope = Scope.TS
    dtype = NodeDType.NUMERIC

    def __init__(self, node, window):
        if node.dtype != NodeDType.NUMERIC:
            raise TypeError("zscore() requires numeric input")
        super().__init__()
        self.node = node
        self.window = window

    def _args(self):
        return [self.node.cache_key(), self.window] + super()._args()

    def compute(self, data, context):
        x = self.node.evaluate(data, context)

        return self.apply(
            x,
            lambda s: (s - s.rolling(self.window).mean()) /
                      (s.rolling(self.window).std() + 1e-9),
            context
        )


@GroupFuncReg.register(group="nodes")
def register_functions():
    NodeRegistry.register(
        "cross",
        lambda left, right: Cross(left, right),
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
        lambda node: Rank(node),
        NodeMeta(
            name="rank",
            group="function",
            desc="Rank计算",
            params=[
                NodeParam("node", "Node", desc="节点")
            ]
        ))

    NodeRegistry.register(
        "top",
        lambda node, window: Top(node, window),
        NodeMeta(
            name="top",
            group="function",
            desc="Top计算",
            params=[
                NodeParam("node", "Node", desc="节点"),
                NodeParam("window", "int", desc="阈值")

            ]
        ))

    NodeRegistry.register(
        "delay",
        lambda node, window: Delay(node, window),
        NodeMeta(
            name="delay",
            group="function",
            desc="Delay计算",
            params=[
                NodeParam("node", "Node", desc="节点"),
                NodeParam("window", "int", desc="阈值")
            ]
        ))

    NodeRegistry.register(
        "mean",
        lambda node, window: Mean(node, window),
        NodeMeta(
            name="mean",
            group="function",
            desc="Mean计算",
            params=[
                NodeParam("node", "Node", desc="节点"),
                NodeParam("window", "int", desc="阈值")
            ]
        ))

    NodeRegistry.register(
        "zscore",
        lambda node: ZScore(node),
        NodeMeta(
            name="zscore",
            group="function",
            desc="ZScore计算",
            params=[
                NodeParam("node", "Node", desc="节点")
            ]
        )
    )

    NodeRegistry.register(
        "zscore_ts",
        lambda node, window: ZScoreTS(node, window),
        NodeMeta(
            name="zscore",
            group="function",
            desc="基于TS的ZScore计算",
            params=[
                NodeParam("node", "Node", desc="节点"),
                NodeParam("window", "int", desc="窗口")
            ]
        )
    )
