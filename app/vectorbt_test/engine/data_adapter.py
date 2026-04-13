import pandas as pd


class DataAdapter:
    def __init__(self, data: pd.DataFrame | pd.Series):
        self.raw = data
        _data, _ = self.adjust_data_index(data)
        self.data, self._is_cross = self._normalize(_data)

    def adjust_data_index(self, df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
        df['date'] = pd.to_datetime(df['date'])
        # ===== 多股票处理 =====
        is_multi = df["symbol"].nunique() > 1
        if is_multi:
            df = df.set_index(["date", "symbol"]).sort_index()
        else:
            df = df.set_index("date").sort_index()
        return df, is_multi

    # ========================
    # 核心：数据标准化
    # ========================
    def _normalize(self, data):
        # MultiIndex: (date, asset)
        if isinstance(data.index, pd.MultiIndex):
            data = data.sort_index()
            return data, True

        # DataFrame: 多列 = 多标
        if isinstance(data, pd.DataFrame) and data.shape[1] > 1:
            data = data.sort_index()
            return data, True

        # 单标（Series 或 单列 DataFrame）
        data = data.sort_index()
        return data.squeeze(), False

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