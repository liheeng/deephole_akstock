import pandas as pd
import numpy as np


class DataView:
    def __init__(self, adapter):
        self.adapter = adapter

    def __getitem__(self, key):
        key = key.lower()
        if hasattr(self.adapter, key):
            df = getattr(self.adapter, key)
            return self.adapter.squeeze_if_single(df)
        raise KeyError(f"{key} not found in data")

    # 可选：兼容 data.close
    def __getattr__(self, key):
        if hasattr(self.adapter, key):
            df = getattr(self.adapter, key)
            return self.adapter.squeeze_if_single(df)
        raise AttributeError(key)
    
    # def __array__(self):
    #     # Use close as default
    #     return self.adapter.close.values

    # def to_frame(self):
    #     # Use close as default
    #     return self.adapter.close
    

class DataAdapter:
    def __init__(self, df: pd.DataFrame):
        self.raw = df.copy()
        self._init_data(df)

    def _init_data(self, df: pd.DataFrame):
        df = df.copy()
        
        # Recommended: Clean the dataframe immediately after loading
        df = df.replace([np.inf, -np.inf], np.nan)
        df['close'] = df['close'].ffill() # Forward fill gaps (e.g. trading suspensions)
        df = df[df['close'] > 0]         # Drop any remaining unpriced rows

        df['date'] = pd.to_datetime(df['date'])

        # 🔥 单股票补 symbol（关键）
        if 'symbol' not in df.columns:
            df['symbol'] = 'asset'

        is_multi = df["symbol"].nunique() > 1

        df = df.set_index(["date", "symbol"]).sort_index()

        # 🔥 每个字段独立 unstack
        self.open   = df["open"].unstack("symbol")
        self.high   = df["high"].unstack("symbol")
        self.low    = df["low"].unstack("symbol")
        self.close  = df["close"].unstack("symbol")
        self.volume = df["volume"].unstack("symbol")

        # 🔥 统一排序 & 对齐
        self._align_all()
        self._fillna()

        self.data = self.close
        self._is_cross = is_multi

    def _align_all(self):
        base = self.close

        self.open   = self.open.reindex_like(base)
        self.high   = self.high.reindex_like(base)
        self.low    = self.low.reindex_like(base)
        self.volume = self.volume.reindex_like(base)

    def _fillna(self):
        self.open   = self.open.ffill()
        self.high   = self.high.ffill()
        self.low    = self.low.ffill()
        self.close  = self.close.ffill()
        self.volume = self.volume.fillna(0)

    def is_single(self):
        return self.close.shape[1] == 1

    def squeeze_if_single(self, df):
        if self.is_single():
            return df.iloc[:, 0]
        return df
    
    def data_view(self) -> DataView:
        return DataView(self)
    
    # ========================
    # 核心：数据标准化
    # ========================
    # def _normalize(self, data):
    #     # MultiIndex: (date, asset)
    #     if isinstance(data.index, pd.MultiIndex):
    #         data = data.sort_index()
    #         return data, True

    #     # DataFrame: 多列 = 多标
    #     if isinstance(data, pd.DataFrame) and data.shape[1] > 1:
    #         data = data.sort_index()
    #         return data, True

    #     # 单标（Series 或 单列 DataFrame）
    #     data = data.sort_index()
    #     return data.squeeze(), False

    def _normalize(self, data):
        data = data.sort_index()

        # ✅ 强制保证 DataFrame
        if isinstance(data, pd.Series):
            data = data.to_frame()

        # ✅ 单列也保持二维（很关键！）
        return data, data.shape[1] > 1

    # ========================
    # 判断
    # ========================
    @property
    def is_cross_section(self):
        return self._is_cross

    # ========================
    # 横截面 rank
    # ========================
    def cs_rank(self, x: pd.DataFrame | pd.Series, ascending=False):
        if isinstance(x.index, pd.MultiIndex):
            return x.groupby(level=0).rank(ascending=ascending)

        if isinstance(x, pd.DataFrame):
            return x.rank(axis=1, ascending=ascending)

        return x  # 单标不处理

    # ========================
    # 横截面 normalize
    # ========================
    def cs_normalize(self, x):
        if isinstance(x.index, pd.MultiIndex):
            denom = x.abs().groupby(level=0).sum()
            return x / denom.replace(0, 1)

        if isinstance(x, pd.DataFrame):
            return x.div(x.abs().sum(axis=1).replace(0, 1), axis=0)

        return x

    # ========================
    # 横截面 zscore
    # ========================
    def cs_zscore(self, x):
        if isinstance(x.index, pd.MultiIndex):
            return x.groupby(level=0).transform(
                lambda v: (v - v.mean()) / (v.std() + 1e-9)
            )

        if isinstance(x, pd.DataFrame):
            return x.sub(x.mean(axis=1), axis=0).div(x.std(axis=1) + 1e-9, axis=0)

        return (x - x.mean()) / (x.std() + 1e-9)

    # ========================
    # 转 vectorbt 格式
    # ========================
    def to_vbt(self, x):
        # MultiIndex → DataFrame
        if isinstance(x.index, pd.MultiIndex):
            return x.unstack().sort_index().ffill()

        # Series → DataFrame
        if isinstance(x, pd.Series):
            return x.to_frame()

        return x

    # ========================
    # 获取资产列表
    # ========================
    def get_assets(self):
        if isinstance(self.data.index, pd.MultiIndex):
            return self.data.index.get_level_values(1).unique()

        if isinstance(self.data, pd.DataFrame):
            return self.data.columns

        return ["SINGLE"]

    # ========================
    # 获取时间索引
    # ========================
    def get_dates(self):
        if isinstance(self.data.index, pd.MultiIndex):
            return self.data.index.get_level_values(0).unique()

        return self.data.index

    # ========================
    # Debug（强烈推荐）
    # ========================
    def debug(self):
        print("=== DataAdapter Debug ===")
        print("is_cross_section:", self.is_cross_section)
        print("type:", type(self.data))
        print("shape:", getattr(self.data, "shape", None))
        print("index:", type(self.data.index))
        print("=========================")