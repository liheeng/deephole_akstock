from abc import ABC, abstractmethod
import enum
import pandas as pd


class NodeType(enum.Enum):
    Unknown = "unknown"
    Default = "default"
    Indicator = "indicator"
    Signal = "signal"
    Factor = "factor"


class Node(ABC):
    def __init__(self, type: NodeType = NodeType.Unknown):
        self._type = type

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def name(self):
        return self.__class__.__name__

    @abstractmethod
    def compute(self, data: pd.DataFrame, cache: dict, context: dict | None = None):
        raise NotImplementedError

    def slope(self):
        return Slope(self)


class Slope(Node):
    def __init__(self, node):
        super().__init__(node.type)
        self.node = node

    def compute(self, data, cache, context):
        x = self.node.compute(data, cache, context)
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

    def compute(self, data: pd.DataFrame, cache: dict, context: dict | None = None):
        df = context.get("db_df")

        if df is None:
            raise ValueError("DBNode requires db_df in context")

        if self.field_name not in df.columns:
            raise ValueError(f"{self.field_name} not found in db_df")

        return df[self.field_name]
