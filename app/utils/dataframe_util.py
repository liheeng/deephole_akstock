import pandas as pd
from typing import List, Dict, Any


def df_to_dict(df: pd.DataFrame, key_col: str, value_cols: list[str]) -> Dict[str, List[Any]]:
    """
    :param df: DataFrame
    :param key_col: 作为key的列名
    :param value_cols: 按顺序放入value的列列表
    :return: {key: [col1_val, col2_val, col3_val...]}
    """
    result = {}
    for _, row in df.iterrows():
        key = row[key_col]
        # 只保留你指定的列，并按顺序生成 list
        value = [row[col] for col in value_cols]
        result[key] = value
    return result
