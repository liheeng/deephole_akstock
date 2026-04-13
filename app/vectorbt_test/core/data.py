from .nodes import DataNode
from .context import PortfolioContext
import pandas as pd


class RawDataNode(DataNode):
    pass


class Price(RawDataNode):
    def __init__(self, column="close"):
        self.column = column

    @property
    def name(self):
        return f"price_{self.column}"

    def compute(self, data, context: PortfolioContext):
        return data[self.column]


# =========================
# DB Node（优先用数据库）
# =========================
class DBNode(RawDataNode):
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
    
