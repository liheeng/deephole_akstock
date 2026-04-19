import pandas as pd


class DataView:
    def __init__(self, adapter):
        self.adapter = adapter

    def __getitem__(self, key):
        key = key.lower()
        if hasattr(self.adapter, key):
            return getattr(self.adapter, key)
        raise KeyError(f"{key} not found in data")

    # 可选：兼容 data.close
    def __getattr__(self, key):
        if hasattr(self.adapter, key):
            return getattr(self.adapter, key)
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
        df['date'] = pd.to_datetime(df['date'])

        is_multi = df["symbol"].nunique() > 1

        if is_multi:
            df = df.set_index(["date", "symbol"]).sort_index()

            # 🔥 核心：每个字段单独拆
            self.open   = df["open"].unstack("symbol")
            self.high   = df["high"].unstack("symbol")
            self.low    = df["low"].unstack("symbol")
            self.close  = df["close"].unstack("symbol")
            self.volume = df["volume"].unstack("symbol")

        else:
            df = df.set_index("date").sort_index()

            # 单股票也统一成 DataFrame（关键！）
            self.open   = df[["open"]]
            self.high   = df[["high"]]
            self.low    = df[["low"]]
            self.close  = df[["close"]]
            self.volume = df[["volume"]]

        self.data = df
        # 🔥 默认 data = close（统一入口）
        self.default_data = self.close

        self._is_cross = is_multi

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