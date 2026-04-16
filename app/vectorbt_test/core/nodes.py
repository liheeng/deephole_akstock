from abc import ABC, abstractmethod
import enum
import pandas as pd
from vectorbt_test.core.context import PortfolioContext
from vectorbt_test.engine.execution_engine import Scope


class NodeType(enum.Enum):
    Unknown = "unknown"
    Indicator = "indicator"
    Factor = "factor"
    Signal = "signal"
    Function = "function"
    Data = "data"


class NodeDType(enum.Enum):
    ANY = "any"

    # 数值类
    NUMERIC = "numeric"     # Series (float/int)

    # 布尔类
    BOOL = "bool"           # 普通布尔（中间态）
    SIGNAL = "signal"       # 交易信号（最终态）

    # 结构类（你缺的）
    FRAME = "frame"         # DataFrame


class Node(ABC):
    _name: str = ""
    scope: Scope | None = None
    dtype: NodeDType | None = None

    def __init__(self, name: str | None = None, type: NodeType = NodeType.Unknown):
        self._type = type
        self._name = name or self.__class__.__name__

    def _args(self):
        return [self.name, self._type.value, self.dtype.value, self.scope]
    
    # ===== cache key =====
    def cache_key(self):
        return (self.__class__.__name__, tuple(self._args()))
    
    def apply(self, series, func, context: PortfolioContext):
        return context.execution_engine.apply(series, func, self.scope)

    # ===== 类型 =====
    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self._name

    @property
    def is_numeric(self):
        return self.dtype == NodeDType.NUMERIC

    @property
    def is_bool(self):
        return self.dtype == NodeDType.BOOL

    @property
    def is_signal(self):
        return self.dtype == NodeDType.SIGNAL

    @property
    def is_frame(self):
        return self.dtype == NodeDType.FRAME

    # ===== evaluate（统一缓存入口）=====
    def evaluate(self, data, context: PortfolioContext = PortfolioContext(), return_result=False):
        assert context.data_provider is not None
        return context.data_provider.get(self, data, context)

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


class FeatureNode(Node):
    def __init__(self, type=NodeType.Unknown):
        super().__init__(type=type)
    pass


class ConstNode(FeatureNode):
    dtype = NodeDType.NUMERIC

    def __init__(self, value):
        super().__init__(NodeType.Indicator)
        self.value = value

    def _args(self):
        return [self.value] + super()._args()

    def compute(self, data, context: PortfolioContext):
        any_series = next(iter(context.data_provider.get_cache().values()))
        return any_series * 0 + self.value


def to_node(x):
    if isinstance(x, Node):
        return x
    return ConstNode(x)


class ArgNode(ConstNode):
    dtype = NodeDType.ANY

    def __init__(self, value):
        super().__init__(value)
        
    def compute(self, data, context: PortfolioContext):
        raise NotImplementedError("DO NOT call this, the ArgNode does not have busisness usage, only for framework to handle argument.")


def to_args(x):
    if isinstance(x, enum.Enum):
        x = x.value
      
    if isinstance(x, str):
        return ArgNode(x)
    return to_node(x)


class BinaryOp(FeatureNode):
    def __init__(self, left, right, op):
        super().__init__()

        self.left = left
        self.right = right
        self.op = op
    
        self.dtype = self._infer_dtype()
        self.scope = self._infer_scope()
    
    def _args(self):
        return [self.left.cache_key(), self.right.cache_key(), self.op] + super()._args()
    
    def _infer_dtype(self):
        l = self.left.dtype
        r = self.right.dtype

        # ===== 数值运算 =====
        if self.op in {"add", "sub", "mul", "div"}:
            if l != NodeDType.NUMERIC or r != NodeDType.NUMERIC:
                raise TypeError("Arithmetic requires numeric inputs")
            return NodeDType.NUMERIC

        # ===== 比较 =====
        if self.op in {"gt", "lt", "ge", "le", "eq", "ne"}:
            return NodeDType.BOOL

        # ===== 布尔 =====
        if self.op in {"and", "or"}:
            if l not in {NodeDType.BOOL, NodeDType.SIGNAL}:
                raise TypeError("AND requires bool/signal")
            if r not in {NodeDType.BOOL, NodeDType.SIGNAL}:
                raise TypeError("AND requires bool/signal")

            return NodeDType.BOOL

        raise ValueError(self.op)

    def _infer_scope(self):
        if self.left.scope == Scope.CS or self.right.scope == Scope.CS:
            return Scope.CS
        return Scope.TS

    def compute(self, data, context: PortfolioContext):
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
   

class UnaryOp(FeatureNode):
    dtype = NodeDType.BOOL

    def __init__(self, node, op):
        super().__init__(NodeType.Factor)
        self.node = node
        self.op = op

    def _args(self):
        return [self.node.cache_key(), self.op] + super()._args()

    def compute(self, data, context: PortfolioContext):
        x = self.node.evaluate(data, context)

        if self.op == "not":
            return (~x.astype(bool)).astype(int)

        raise ValueError(self.op)
   

class Slope(FeatureNode):
    dtype = NodeDType.NUMERIC

    def __init__(self, node):
        super().__init__(node.type)
        self.node = node

    def _args(self):
        return [self.node.cache_key()] + super()._args()

    def compute(self, data, context: PortfolioContext):
        x = self.node.evaluate(data, context)
        return x.diff()

