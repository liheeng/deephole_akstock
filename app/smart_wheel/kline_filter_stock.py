import pandas as pd
import numpy as np


def select_stock_by_pattern(
    stock_data,
    box_days=20,
    box_shrink_ratio=0.02,
    first_surge_ratio=0.06,
    retrace_range=(2, 4),
    volume_ratio=1.5,
):
    """
    基于K线形态筛选股票，返回各个关键点的完整K线信息（时间、OCHL、量额）

    :param stock_data: DataFrame，包含 'open','high','low','close','volume','amount'，按日期升序
    :return: dict 或 None，字典结构见下方示例
    """
    # 确保数据按时间升序，且足够长
    stock_data = stock_data.sort_index()
    if len(stock_data) < 200:
        return None

    # ========== 阶段1：股价大幅下跌，距半年以上高位跌超60% ==========
    lookback = min(250, len(stock_data))
    recent_data = stock_data.tail(lookback)
    high_price = recent_data["high"].max()
    high_idx = recent_data["high"].idxmax()
    current_price = stock_data["close"].iloc[-1]
    current_date = stock_data.index[-1]

    # 获取高位点的完整K线
    high_row = stock_data.loc[high_idx]
    high_point = {
        "date": high_idx,
        "open": high_row["open"],
        "close": high_row["close"],
        "high": high_row["high"],
        "low": high_row["low"],
        "volume": high_row["volume"],
        "amount": high_row["amount"],
    }

    # 时间间隔判断（半年约120个交易日）
    if isinstance(stock_data.index, pd.DatetimeIndex):
        days_from_high = (current_date - high_idx).days
    else:
        days_from_high = len(stock_data) - stock_data.index.get_loc(high_idx) - 1
    if days_from_high < 120:
        return None
    if current_price > 0.4 * high_price:
        return None

    # 从高位到当前区间的整体最低价点
    since_high = stock_data.loc[high_idx:]
    lowest_price = since_high["low"].min()
    lowest_date = since_high["low"].idxmin()
    lowest_row = stock_data.loc[lowest_date]
    lowest_point = {
        "date": lowest_date,
        "open": lowest_row["open"],
        "close": lowest_row["close"],
        "high": lowest_row["high"],
        "low": lowest_row["low"],
        "volume": lowest_row["volume"],
        "amount": lowest_row["amount"],
    }

    # ========== 阶段2：近期窄幅箱体整理（20~60日） ==========
    box_length = None
    box_high = None
    box_low = None
    box_start_idx = None
    box_end_idx = None
    box_high_point = None  # 箱体内最高价关键点
    box_low_point = None  # 箱体内最低价关键点

    for days in range(box_days, min(61, len(stock_data) - 5)):
        box = stock_data.tail(days + 5).head(days)
        if len(box) < days:
            continue

        # 每日振幅 < 2%
        daily_amp = box["high"] / box["low"] - 1
        if (daily_amp >= box_shrink_ratio).any():
            continue

        # 区间整体涨跌幅在 ±10% 内
        start_close = box["close"].iloc[0]
        end_close = box["close"].iloc[-1]
        overall_change = (end_close - start_close) / start_close
        if abs(overall_change) > 0.10:
            continue

        # 箱体高度不超过20%
        box_high_candidate = box["high"].max()
        box_low_candidate = box["low"].min()
        if (box_high_candidate / box_low_candidate - 1) > 0.20:
            continue

        # 记录箱体信息
        box_length = days
        box_high = box_high_candidate
        box_low = box_low_candidate
        box_start_idx = box.index[0]
        box_end_idx = box.index[-1]

        # 找出箱体内最高价日期（取第一次出现）
        high_date_in_box = box[box["high"] == box_high_candidate].index[0]
        high_row_box = stock_data.loc[high_date_in_box]
        box_high_point = {
            "date": high_date_in_box,
            "open": high_row_box["open"],
            "close": high_row_box["close"],
            "high": high_row_box["high"],
            "low": high_row_box["low"],
            "volume": high_row_box["volume"],
            "amount": high_row_box["amount"],
        }

        # 找出箱体内最低价日期（取第一次出现）
        low_date_in_box = box[box["low"] == box_low_candidate].index[0]
        low_row_box = stock_data.loc[low_date_in_box]
        box_low_point = {
            "date": low_date_in_box,
            "open": low_row_box["open"],
            "close": low_row_box["close"],
            "high": low_row_box["high"],
            "low": low_row_box["low"],
            "volume": low_row_box["volume"],
            "amount": low_row_box["amount"],
        }
        break

    if box_length is None:
        return None

    # ========== 阶段3：20日内首次放量大阳线突破箱体 ==========
    after_box = stock_data.loc[box_end_idx:].iloc[1:]
    if len(after_box) < 5:
        return None

    recent_20 = after_box.tail(20)
    volume_ma20 = stock_data["volume"].rolling(20, min_periods=10).mean()

    breakout_idx = None
    breakout_high_price = None
    breakout_row = None
    breakout_gain = None

    for idx in recent_20.index:
        current = stock_data.loc[idx]
        prev_close = stock_data.loc[
            stock_data.index[stock_data.index.get_loc(idx) - 1], "close"
        ]
        gain = (current["close"] - prev_close) / prev_close

        if not (
            current["close"] > current["open"]
            and gain >= first_surge_ratio
            and current["close"] > box_high
        ):
            continue

        current_vol = current["volume"]
        avg_vol = volume_ma20.loc[idx] if idx in volume_ma20.index else current_vol
        if pd.isna(avg_vol) or current_vol < avg_vol * volume_ratio:
            continue

        breakout_idx = idx
        breakout_high_price = current["high"]
        breakout_row = current
        breakout_gain = gain
        break

    if breakout_idx is None:
        return None

    first_break_point = {
        "date": breakout_idx,
        "open": breakout_row["open"],
        "close": breakout_row["close"],
        "high": breakout_row["high"],
        "low": breakout_row["low"],
        "volume": breakout_row["volume"],
        "amount": breakout_row["amount"],
        "gain": breakout_gain,
    }

    # ========== 阶段4：回调2~4日不破首阳收盘价，再出大阳线突破首阳最高价 ==========
    pos_break = stock_data.index.get_loc(breakout_idx)
    total_len = len(stock_data)

    for retrace_days in range(retrace_range[0], retrace_range[1] + 1):
        if pos_break + retrace_days + 1 >= total_len:
            continue

        retrace_ok = True
        for i in range(1, retrace_days + 1):
            if stock_data.iloc[pos_break + i]["low"] < breakout_row["close"]:
                retrace_ok = False
                break
        if not retrace_ok:
            continue

        second_idx_pos = pos_break + retrace_days + 1
        second_row = stock_data.iloc[second_idx_pos]
        prev_close_second = stock_data.iloc[second_idx_pos - 1]["close"]
        gain_second = (second_row["close"] - prev_close_second) / prev_close_second

        if (
            second_row["close"] > second_row["open"]
            and gain_second >= first_surge_ratio
            and second_row["close"] > breakout_high_price
        ):
            second_break_point = {
                "date": second_row.name,
                "open": second_row["open"],
                "close": second_row["close"],
                "high": second_row["high"],
                "low": second_row["low"],
                "volume": second_row["volume"],
                "amount": second_row["amount"],
                "gain": gain_second,
            }
            # 构建最终返回字典
            result = {
                "high_point": high_point,
                "lowest_point": lowest_point,
                "box_start_date": box_start_idx,
                "box_end_date": box_end_idx,
                "box_high_price": box_high,
                "box_low_price": box_low,
                "box_high_point": box_high_point,  # 箱体最高价关键点
                "box_low_point": box_low_point,  # 箱体最低价关键点
                "first_break_point": first_break_point,
                "second_break_point": second_break_point,
                "retrace_days_used": retrace_days,
            }
            return result

    return None
