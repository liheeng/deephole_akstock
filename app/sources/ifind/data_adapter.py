import json
from typing import Tuple, Dict
from collections import defaultdict
import pandas as pd
from utils.log_manager import get_default_logger
# 你的原始数据（已转字符串）
# data_json = '''{"errorcode":0,"errmsg":"","tables":[{"thscode":"300033.SZ",...}''' # 粘贴你完整数据


def convert_to_df(data_json) -> Tuple[bool, Dict[str, pd.DataFrame]|None]:
    data = json.loads(data_json)

    if data['errorcode'] != 0:
        get_default_logger().warning(f"errorcode: {data['errorcode']}, errmsg: {data['errmsg']}")
        return False, None
    
    # 遍历每只股票
    his_data = dict()
    for table in data["tables"]:
        thscode = table["thscode"]
        time_list = table["time"]
        ohlcv = table["table"]

        # 转成 DataFrame（最实用）
        df = pd.DataFrame({
            "date": time_list,
            "open": ohlcv["open"],
            "close": ohlcv["close"],
            "high": ohlcv["high"],
            "low": ohlcv["low"],
            "volume": ohlcv["volume"],
            "amount": ohlcv["amount"],
            "turnover": ohlcv["turnover"]
        })

        his_data[thscode] = df

    return True, his_data