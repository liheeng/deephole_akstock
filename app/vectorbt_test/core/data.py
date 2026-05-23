from .context import PortfolioContext
import pandas as pd
from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from vectorbt_test.core.nodes import NodeDType, NodeType, Node
from utils.group_func_registry import GroupFuncReg


class DataNode(Node):
    dtype = NodeDType.ANY

    def __init__(self, type=NodeType.Data):
        super().__init__(type=type)
  
    pass


class RawDataNode(DataNode):
    pass


class Price(RawDataNode):
    dtype = NodeDType.NUMERIC

    def __init__(self, column="close"):
        super().__init__(type=NodeType.Data)
        self.column = column

    @property
    def name(self):
        return f"price_{self.column}"

    def _args(self):
        return [self.column] + super()._args()
  
    def compute(self, data, context: PortfolioContext):
        return data[self.column]


# =========================
# DB Node（优先用数据库）
# =========================
class DBNode(RawDataNode):
    dtype = NodeDType.ANY

    def __init__(self, field_name: str):
        super().__init__()
        self.field_name = field_name

    @property
    def name(self):
        return self.field_name

    def _args(self):
        return [self.field_name] + super()._args()
  
    def compute(self, data: pd.DataFrame, context: PortfolioContext):
        df = context.get("db_df")

        if df is None:
            raise ValueError("DBNode requires db_df in context")

        if self.field_name not in df.columns:
            raise ValueError(f"{self.field_name} not found in db_df")

        return df[self.field_name]
    

@GroupFuncReg.register(group="nodes")
def register_data():
    NodeRegistry.register(
        "Price",
        lambda column="close": Price(column),
        NodeMeta(
            name="Price",
            group="data",
            desc="price",
            params=[
                NodeParam("column", "str", 'close', "price column")
            ]
        )
    )
