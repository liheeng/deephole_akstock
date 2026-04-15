from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd
# from vectorbt_test.core.context import PortfolioContext


class DataProvider:

    def __init__(self, adapter, db=None):
        self.adapter = adapter
        self.db = db
        self.cache = {}

    def get_cache(self):
        return self.cache
    
    def get(self, node, data, context):
        # Raw data node 直接计算, DO NOT cache!!!
        from vectorbt_test.core.data import DataNode
        if isinstance(node, DataNode):
            return node.compute(data, context)

        key = (
            node.cache_key(),
            context.data_scope.key()
        )
        # 1️⃣ cache
        if key in self.cache:
            return self.cache[key]

        # 2️⃣ DB
        if self.db is not None:
            db_val = self._get_from_db(node, context)
            if db_val is not None:
                self.cache[key] = db_val
                return db_val

        # 3️⃣ compute
        val = node.compute(data, context)

        # 4️⃣ 写缓存
        self.cache[key] = val

        return val
    
    def _get_from_db(self, node, context):
        # 只允许某些 node 走 DB
        if not hasattr(node, "name"):
            return None

        df = context.get("db_df")

        if df is not None and node.name in df.columns:
            return df[node.name]

        return None


class DataBackend(ABC):

    @abstractmethod
    def get(self, key: tuple, context) -> pd.Series | None:
        pass

    @abstractmethod
    def set(self, key: tuple, value: pd.Series, context):
        pass


class DuckDBBackend(DataBackend):

    def get(self, key, context):
        # key → table / column mapping
        pass

    def set(self, key, value, context):
        pass
