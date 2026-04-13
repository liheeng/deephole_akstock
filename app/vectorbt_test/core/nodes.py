from abc import ABC, abstractmethod
import enum
import pandas as pd
from vectorbt_test.core.context import PortfolioContext


class NodeType(enum.Enum):
    Unknown = "unknown"
    Indicator = "indicator"
    Factor = "factor"
    Signal = "signal"
    Function = "function"


class NodeDType(enum.Enum):
    Numeric = "numeric"
    Bool = "bool"
    Signal = "signal"


class Node(ABC):
    def __init__(self, type: NodeType = NodeType.Unknown, dtype: NodeDType = NodeDType.Numeric):
        self._type = type
        self._dtype = dtype

    # ===== 类型 =====
    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def dtype(self):
        return self._dtype

    @property
    def is_numeric(self):
        return self._dtype == NodeDType.Numeric

    @property
    def is_bool(self):
        return self._dtype == NodeDType.Bool

    @property
    def is_signal(self):
        return self._dtype == NodeDType.Signal

    # ===== cache key =====
    def cache_key(self):
        return (self.__class__.__name__, tuple(self._args()))

    def _args(self):
        return []

    # ===== evaluate（统一缓存入口）=====
    def evaluate(self, data, context: PortfolioContext = PortfolioContext()):
        assert context.data_provider is not None
        return context.data_provider.get(self, data, context)
        # key = self.cache_key()

        # if key in cache:
        #     return cache[key]

        # result = self.compute(data, context)
        # cache[key] = result
        # return result

    @abstractmethod
    def compute(self, data: pd.DataFrame, context: PortfolioContext):
        pass

    # =========================
    # DSL：算术
    # =========================
    def __add__(self, other):
        return BinaryOp(self, to_node(other), "add")

    def __sub__(self, other):
        return BinaryOp(self, to_node(other), "sub")

    def __mul__(self, other):
        return BinaryOp(self, to_node(other), "mul")

    def __truediv__(self, other):
        return BinaryOp(self, to_node(other), "div")

    # =========================
    # DSL：比较
    # =========================
    def __gt__(self, other):
        return BinaryOp(self, to_node(other), "gt")

    def __lt__(self, other):
        return BinaryOp(self, to_node(other), "lt")

    def __ge__(self, other):
        return BinaryOp(self, to_node(other), "ge")

    def __le__(self, other):
        return BinaryOp(self, to_node(other), "le")

    def __eq__(self, other):
        return BinaryOp(self, to_node(other), "eq")

    def __ne__(self, other):
        return BinaryOp(self, to_node(other), "ne")

    # =========================
    # DSL：布尔
    # =========================
    def __and__(self, other):
        return BinaryOp(self, to_node(other), "and")

    def __or__(self, other):
        return BinaryOp(self, to_node(other), "or")

    def __invert__(self):
        return UnaryOp(self, "not")

    # =========================
    # DSL：函数
    # =========================
    def slope(self):
        return Slope(self)


class ConstNode(Node):
    def __init__(self, value):
        super().__init__(NodeType.Indicator)
        self.value = value

    def _args(self):
        return [self.value]

    def compute(self, data, context: PortfolioContext):
        any_series = next(iter(context.data_provider.get_cache().values()))
        return any_series * 0 + self.value


def to_node(x):
    if isinstance(x, Node):
        return x
    return ConstNode(x)


class BinaryOp(Node):
    def __init__(self, left, right, op):

        if op in {"add", "sub", "mul", "div"}:
            dtype = NodeDType.Numeric

        elif op in {"gt", "lt", "ge", "le", "eq", "ne"}:
            dtype = NodeDType.Bool

        elif op in {"and", "or"}:
            dtype = NodeDType.Bool

        else:
            raise ValueError(op)

        super().__init__(NodeType.Factor, dtype)

        self.left = left
        self.right = right
        self.op = op

    def _args(self):
        return [self.left.cache_key(), self.right.cache_key(), self.op]

    def compute(self, data, context):
        assert context.data_provider is not None
        l = self.left.evaluate(data, context)
        r = self.right.evaluate(data, context)

        if self.op == "add":
            return l + r
        elif self.op == "sub":
            return l - r
        elif self.op == "mul":
            return l * r
        elif self.op == "div":
            return l / r.replace(0, 1e-9)

        elif self.op == "gt":
            return (l > r).astype(int)
        elif self.op == "lt":
            return (l < r).astype(int)
        elif self.op == "ge":
            return (l >= r).astype(int)
        elif self.op == "le":
            return (l <= r).astype(int)
        elif self.op == "eq":
            return (l == r).astype(int)
        elif self.op == "ne":
            return (l != r).astype(int)

        elif self.op == "and":
            return (l.astype(bool) & r.astype(bool)).astype(int)
        elif self.op == "or":
            return (l.astype(bool) | r.astype(bool)).astype(int)

        raise ValueError(f"Unknown op {self.op}")
   

class UnaryOp(Node):
    def __init__(self, node, op):
        super().__init__(NodeType.Factor, NodeDType.Bool)
        self.node = node
        self.op = op

    def _args(self):
        return [self.node.cache_key(), self.op]

    def compute(self, data, context):
        assert context.data_provider is not None
        x = self.node.evaluate(data, context)

        if self.op == "not":
            return (~x.astype(bool)).astype(int)

        raise ValueError(self.op)
   

class Slope(Node):
    def __init__(self, node):
        super().__init__(node.type)
        self.node = node

    def _args(self):
        return [self.node.cache_key()]

    def compute(self, data, context):
        assert context.data_provider is not None
        x = self.node.evaluate(data, context)
        return x.diff()


# =========================
# DB Node（优先用数据库）
# =========================
class DBNode(Node):
    def __init__(self, field_name: str):
        super().__init__()
        self.field_name = field_name

    @property
    def name(self):
        return self.field_name

    def compute(self, data: pd.DataFrame, context: PortfolioContext):
        df = context.get("db_df")

        if df is None:
            raise ValueError("DBNode requires db_df in context")

        if self.field_name not in df.columns:
            raise ValueError(f"{self.field_name} not found in db_df")

        return df[self.field_name]
