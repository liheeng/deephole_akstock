import pandas as pd


def handle_multi_index(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    df['date'] = pd.to_datetime(df['date'])
    # ===== 多股票处理 =====
    is_multi = df["symbol"].nunique() > 1
    if is_multi:
        df = df.set_index(["date", "symbol"]).sort_index()
    else:
        df = df.set_index("date").sort_index()
    return df, is_multi


def normalize_cross_section(alpha: pd.Series) -> pd.Series:
    return alpha.groupby(level=0).transform(
        lambda x: (x - x.mean()) / (x.std() + 1e-9)
    )


def maybe_normalize(alpha: pd.Series) -> pd.Series:
    # 判断是不是 multi-index
    if isinstance(alpha.index, pd.MultiIndex):
        if alpha.index.get_level_values(0).nunique() > 1 and \
           alpha.index.get_level_values(1).nunique() > 1:
            return normalize_cross_section(alpha)

    return alpha



def top_n(alpha: pd.Series, n: int) -> pd.Series:
    rank = alpha.groupby(level=0).rank(ascending=False)
    return rank <= n