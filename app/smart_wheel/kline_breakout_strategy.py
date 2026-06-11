import pandas as pd


# ----------------------------- 阶段1：大幅下跌信号 -----------------------------
def _price_drop_signal_single(df, half_year_days, drop_threshold, trace_days):
    """
    单只股票内部函数。

    参数:
        df: 单只股票的 DataFrame，需包含 open/close/high/low/volume/amount 列
        half_year_days: 从最高点到当前的最少天数 (默认120)
        drop_threshold: 当前价 ≤ 最高价 × drop_threshold 才触发 (默认0.4, 即下跌60%)
        trace_days: 追溯最高点的最大天数 (默认500)
    """
    """
    单只股票：从高位日期（满足时间间隔且现价低于高位 threshold）开始，到数据末尾标记为 True。
    新增列：signal_drop
    """
    df = df.copy()
    df["signal_drop"] = False

    if len(df) < 200:
        return df, None

    lookback = min(trace_days, len(df))
    recent_data = df.tail(lookback)
    high_price = recent_data["high"].max()
    high_idx = recent_data["high"].idxmax()
    current_price = df["close"].iloc[-1]

    # 时间间隔判断
    if isinstance(df.index, pd.DatetimeIndex):
        days_from_high = (df.index[-1] - high_idx).days
    else:
        days_from_high = len(df) - df.index.get_loc(high_idx) - 1

    if days_from_high < half_year_days:
        return df, None
    if current_price > drop_threshold * high_price:
        return df, None

    # 从高位日期到末尾标记为 True
    high_pos = df.index.get_loc(high_idx)
    df.iloc[high_pos:, df.columns.get_loc("signal_drop")] = True

    highest_row = df.loc[high_idx]
    since_high = df.loc[high_idx:]
    lowest_date = since_high["low"].idxmin()
    lowest_row = df.loc[lowest_date]

    info = {
        "highest_point": {
            "date": high_idx,
            "open": highest_row["open"],
            "close": highest_row["close"],
            "high": highest_row["high"],
            "low": highest_row["low"],
            "volume": highest_row["volume"],
            "amount": highest_row["amount"],
        },
        "lowest_point": {
            "date": lowest_date,
            "open": lowest_row["open"],
            "close": lowest_row["close"],
            "high": lowest_row["high"],
            "low": lowest_row["low"],
            "volume": lowest_row["volume"],
            "amount": lowest_row["amount"],
        },
        "high_idx": high_idx,
    }
    return df, info


def price_drop_signal(
    stock_data,
    half_year_days=120,
    drop_threshold=0.4,
    trace_days=500
):
    """
    在 stock_data 上标注"大幅下跌"信号列。按 symbol 分组计算。
    新增列：signal_drop

    参数:
        stock_data: 原始 DataFrame，应含 open/close/high/low/volume/amount，可选 symbol 列
        half_year_days: 从最高点到当前的最少天数 (默认120)
        drop_threshold: 当前价 ≤ 最高价 × drop_threshold 才触发 (默认0.4, 即下跌60%)
        trace_days: 追溯最高点的最大天数 (默认500)

    返回:
        (df_with_signal, info_dict): df 增加 signal_drop 列；info_dict 为 {symbol: info} 或单只时直接返回 info
    """
    df = stock_data.copy()
    df["signal_drop"] = False

    if "symbol" not in df.columns:
        # 无 symbol 列，当作单只股票处理
        return _price_drop_signal_single(df, half_year_days, drop_threshold, trace_days)

    all_info = {}
    result_parts = []
    for sym, group in df.groupby("symbol", sort=False):
        group_sorted = group.sort_index()
        sub_df, info = _price_drop_signal_single(
            group_sorted, half_year_days, drop_threshold, trace_days
        )
        result_parts.append(sub_df)
        if info is not None:
            all_info[sym] = info

    if not all_info:
        return pd.concat(result_parts).loc[df.index], None

    result_df = pd.concat(result_parts).loc[df.index]
    return result_df, all_info


# ----------------------------- 阶段2：窄幅箱体整理信号 -----------------------------
def _box_consolidation_signal_single(
    df, min_box_days, max_box_days, box_shrink_ratio, overall_change_limit, box_height_ratio
):
    """
    单只股票内部函数。

    参数:
        df: 单只股票 DataFrame
        min_box_days: 箱体最短天数 (默认20)
        max_box_days: 箱体最长大天数 (默认60)
        box_shrink_ratio: 每日振幅上限 (默认0.02, 即单日振幅不超过2%)
        overall_change_limit: 箱体区间整体涨跌幅上限 (默认0.10, 即±10%)
        box_height_ratio: 箱体高度上限 (默认0.20, 即最高/最低不超过20%)

    新增列：signal_box
    """
    df = df.copy()
    df["signal_box"] = False

    if len(df) < (max_box_days + 5):
        return df, None

    for days in range(min_box_days, min(max_box_days + 1, len(df) - 5)):
        box = df.tail(days + 5).head(days)
        if len(box) < days:
            continue

        # 每日振幅条件
        daily_amp = box["high"] / box["low"] - 1
        if (daily_amp >= box_shrink_ratio).any():
            continue

        # 区间整体涨跌幅
        start_close = box["close"].iloc[0]
        end_close = box["close"].iloc[-1]
        overall_change = (end_close - start_close) / start_close
        if abs(overall_change) > overall_change_limit:
            continue

        # 箱体高度
        box_high_candidate = box["high"].max()
        box_low_candidate = box["low"].min()
        if (box_high_candidate / box_low_candidate - 1) > box_height_ratio:
            continue

        box_start_idx = box.index[0]
        box_end_idx = box.index[-1]

        # 在箱体区间内标记为 True
        start_pos = df.index.get_loc(box_start_idx)
        end_pos = df.index.get_loc(box_end_idx)
        df.iloc[start_pos:end_pos + 1, df.columns.get_loc("signal_box")] = True

        high_date_in_box = box[box["high"] == box_high_candidate].index[0]
        high_row_box = df.loc[high_date_in_box]

        low_date_in_box = box[box["low"] == box_low_candidate].index[0]
        low_row_box = df.loc[low_date_in_box]

        info = {
            "box_start_date": box_start_idx,
            "box_end_date": box_end_idx,
            "box_high_price": box_high_candidate,
            "box_low_price": box_low_candidate,
            "box_high_point": {
                "date": high_date_in_box,
                "open": high_row_box["open"],
                "close": high_row_box["close"],
                "high": high_row_box["high"],
                "low": high_row_box["low"],
                "volume": high_row_box["volume"],
                "amount": high_row_box["amount"],
            },
            "box_low_point": {
                "date": low_date_in_box,
                "open": low_row_box["open"],
                "close": low_row_box["close"],
                "high": low_row_box["high"],
                "low": low_row_box["low"],
                "volume": low_row_box["volume"],
                "amount": low_row_box["amount"],
            },
        }
        return df, info

    return df, None


def box_consolidation_signal(
    stock_data,
    min_box_days=20,
    max_box_days=60,
    box_shrink_ratio=0.02,
    overall_change_limit=0.10,
    box_height_ratio=0.20,
):
    """
    在 stock_data 上标注"箱体整理"信号列。按 symbol 分组计算。
    新增列：signal_box

    参数:
        stock_data: 原始 DataFrame
        min_box_days: 箱体最短天数 (默认20)
        max_box_days: 箱体最长大天数 (默认60)
        box_shrink_ratio: 每日振幅上限 (默认0.02)
        overall_change_limit: 箱体区间整体涨跌幅上限 (默认0.10, 即±10%)
        box_height_ratio: 箱体高度上限 (默认0.20, 即最高/最低不超过20%)

    返回:
        (df_with_signal, info_dict)
    """
    df = stock_data.copy()
    df["signal_box"] = False

    if "symbol" not in df.columns:
        return _box_consolidation_signal_single(
            df, min_box_days, max_box_days, box_shrink_ratio, overall_change_limit, box_height_ratio
        )

    all_info = {}
    result_parts = []
    for sym, group in df.groupby("symbol", sort=False):
        group_sorted = group.sort_index()
        sub_df, info = _box_consolidation_signal_single(
            group_sorted, min_box_days, max_box_days, box_shrink_ratio, overall_change_limit, box_height_ratio
        )
        result_parts.append(sub_df)
        if info is not None:
            all_info[sym] = info

    if not all_info:
        return pd.concat(result_parts).loc[df.index], None

    result_df = pd.concat(result_parts).loc[df.index]
    return result_df, all_info


# ----------------------------- 阶段3：首次放量大阳线突破信号 -----------------------------
def _first_breakout_signal_single(
    df, box_end_idx, box_high, first_surge_ratio, volume_ratio, lookback_days
):
    """
    单只股票内部函数。

    参数:
        df: 单只股票 DataFrame
        box_end_idx: 箱体结束日期 (index 值)
        box_high: 箱体最高价
        first_surge_ratio: 突破日最低涨幅 (默认0.06, 即6%)
        volume_ratio: 成交量需为20日均量的倍数 (默认1.5)
        lookback_days: 箱体结束后回溯天数 (默认20)

    新增列：signal_first_break
    """
    df = df.copy()
    df["signal_first_break"] = False

    after_box = df.loc[box_end_idx:].iloc[1:]
    if len(after_box) < 5:
        return df, None

    recent = after_box.tail(lookback_days)
    volume_ma20 = df["volume"].rolling(20, min_periods=10).mean()

    for idx in recent.index:
        current = df.loc[idx]
        prev_close = df.loc[df.index[df.index.get_loc(idx) - 1], "close"]
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

        pos = df.index.get_loc(idx)
        df.iloc[pos, df.columns.get_loc("signal_first_break")] = True

        info = {
            "first_break_point": {
                "date": idx,
                "open": current["open"],
                "close": current["close"],
                "high": current["high"],
                "low": current["low"],
                "volume": current_vol,
                "amount": current["amount"],
                "gain": gain,
            },
            "breakout_high": current["high"],
            "breakout_close": current["close"],
        }
        return df, info

    return df, None


def first_breakout_signal(
    stock_data, box_end_idx, box_high,
    first_surge_ratio=0.06, volume_ratio=1.5, lookback_days=20
):
    """
    在 stock_data 上标注"首次放量大阳线突破"信号列。按 symbol 分组计算。
    新增列：signal_first_break

    参数:
        stock_data: 原始 DataFrame
        box_end_idx: 箱体结束日期 (index 值)
        box_high: 箱体最高价
        first_surge_ratio: 突破日最低涨幅 (默认0.06)
        volume_ratio: 成交量需为20日均量的倍数 (默认1.5)
        lookback_days: 箱体结束后回溯天数 (默认20)

    返回:
        (df_with_signal, info_dict)
    """
    df = stock_data.copy()
    df["signal_first_break"] = False

    if "symbol" not in df.columns:
        return _first_breakout_signal_single(
            df, box_end_idx, box_high, first_surge_ratio, volume_ratio, lookback_days
        )

    all_info = {}
    result_parts = []
    for sym, group in df.groupby("symbol", sort=False):
        group_sorted = group.sort_index()
        sub_df, info = _first_breakout_signal_single(
            group_sorted, box_end_idx, box_high, first_surge_ratio, volume_ratio, lookback_days
        )
        result_parts.append(sub_df)
        if info is not None:
            all_info[sym] = info

    if not all_info:
        return pd.concat(result_parts).loc[df.index], None

    result_df = pd.concat(result_parts).loc[df.index]
    return result_df, all_info


# ----------------------------- 阶段4：回调后再次突破信号 -----------------------------
def _retrace_second_breakout_signal_single(
    df, breakout_idx, breakout_close, breakout_high,
    first_surge_ratio, retrace_range
):
    """
    单只股票内部函数。

    参数:
        df: 单只股票 DataFrame
        breakout_idx: 首次突破日 index
        breakout_close: 首次突破日收盘价 (回调不破此价)
        breakout_high: 首次突破日最高价 (二次突破须超过此价)
        first_surge_ratio: 二次突破日最低涨幅 (默认0.06)
        retrace_range: 回调天数范围 (默认(2,4), 即2~4天)

    新增列：signal_second_break
    """
    df = df.copy()
    df["signal_second_break"] = False

    pos_break = df.index.get_loc(breakout_idx)
    total_len = len(df)

    for retrace_days in range(retrace_range[0], retrace_range[1] + 1):
        if pos_break + retrace_days + 1 >= total_len:
            continue

        retrace_ok = True
        for i in range(1, retrace_days + 1):
            if df.iloc[pos_break + i]["low"] < breakout_close:
                retrace_ok = False
                break
        if not retrace_ok:
            continue

        second_idx_pos = pos_break + retrace_days + 1
        second_row = df.iloc[second_idx_pos]
        prev_close_second = df.iloc[second_idx_pos - 1]["close"]
        gain_second = (second_row["close"] - prev_close_second) / prev_close_second

        if (
            second_row["close"] > second_row["open"]
            and gain_second >= first_surge_ratio
            and second_row["close"] > breakout_high
        ):
            df.iloc[second_idx_pos, df.columns.get_loc("signal_second_break")] = True

            info = {
                "second_break_point": {
                    "date": second_row.name,
                    "open": second_row["open"],
                    "close": second_row["close"],
                    "high": second_row["high"],
                    "low": second_row["low"],
                    "volume": second_row["volume"],
                    "amount": second_row["amount"],
                    "gain": gain_second,
                },
                "retrace_days_used": retrace_days,
            }
            return df, info

    return df, None


def retrace_second_breakout_signal(
    stock_data, breakout_idx, breakout_close, breakout_high,
    first_surge_ratio=0.06, retrace_range=(2, 4)
):
    """
    在 stock_data 上标注"回调后再次突破"信号列。按 symbol 分组计算。
    新增列：signal_second_break

    参数:
        stock_data: 原始 DataFrame
        breakout_idx: 首次突破日 index
        breakout_close: 首次突破日收盘价 (回调不破此价)
        breakout_high: 首次突破日最高价 (二次突破须超过此价)
        first_surge_ratio: 二次突破日最低涨幅 (默认0.06)
        retrace_range: 回调天数范围 (默认(2,4), 即2~4天)

    返回:
        (df_with_signal, info_dict)
    """
    df = stock_data.copy()
    df["signal_second_break"] = False

    if "symbol" not in df.columns:
        return _retrace_second_breakout_signal_single(
            df, breakout_idx, breakout_close, breakout_high,
            first_surge_ratio, retrace_range
        )

    all_info = {}
    result_parts = []
    for sym, group in df.groupby("symbol", sort=False):
        group_sorted = group.sort_index()
        sub_df, info = _retrace_second_breakout_signal_single(
            group_sorted, breakout_idx, breakout_close, breakout_high,
            first_surge_ratio, retrace_range
        )
        result_parts.append(sub_df)
        if info is not None:
            all_info[sym] = info

    if not all_info:
        return pd.concat(result_parts).loc[df.index], None

    result_df = pd.concat(result_parts).loc[df.index]
    return result_df, all_info


# ----------------------------- 主函数：串联四个阶段，逐列标注信号 -----------------------------
def select_stock_by_pattern(
    stock_data,
    min_box_days=20,
    max_box_days=60,
    box_shrink_ratio=0.02,
    overall_change_limit=0.10,
    box_height_ratio=0.20,
    first_surge_ratio=0.06,
    retrace_range=(2, 4),
    volume_ratio=1.5,
    half_year_days=120,
    drop_threshold=0.4,
    trace_days=500,
    lookback_days=20,
):
    """
    综合四个阶段在 stock_data 上逐列标注信号，返回标注后的 DataFrame 和按 symbol 组织的汇总信息。

    参数:
        stock_data: 原始 DataFrame，应含 open/close/high/low/volume/amount，可选 symbol 列
        min_box_days: 箱体最短天数 (默认20)
        max_box_days: 箱体最长大天数 (默认60)
        box_shrink_ratio: 每日振幅上限 (默认0.02)
        overall_change_limit: 箱体区间整体涨跌幅上限 (默认0.10, 即±10%)
        box_height_ratio: 箱体高度上限 (默认0.20, 即最高/最低不超过20%)
        first_surge_ratio: 突破日最低涨幅 (默认0.06)
        retrace_range: 回调天数范围 (默认(2,4))
        volume_ratio: 成交量需为20日均量的倍数 (默认1.5)
        half_year_days: 从最高点到当前的最少天数 (默认120)
        drop_threshold: 当前价 ≤ 最高价 × drop_threshold 才触发 (默认0.4)
        trace_days: 追溯最高点的最大天数 (默认500)
        lookback_days: 箱体结束后寻找突破的回溯天数 (默认20)

    新增列：
      - signal_drop: 从高位下跌至今的区间
      - signal_box: 箱体整理区间
      - signal_first_break: 首次放量大阳线突破日
      - signal_second_break: 回调后第二次突破日

    返回:
        (df, summary):
          df: 标注了所有信号列的完整 DataFrame
          summary: dict {symbol: {阶段信息}}，不满足时返回 (df, None)
    """
    df = stock_data.copy()

    # 阶段1：大幅下跌信号
    df, stage1_info = price_drop_signal(df, half_year_days, drop_threshold, trace_days)
    if stage1_info is None:
        return df, None

    # 阶段2：窄幅箱体整理信号
    df, stage2_info = box_consolidation_signal(
        df, min_box_days, max_box_days,
        box_shrink_ratio, overall_change_limit, box_height_ratio,
    )
    if stage2_info is None:
        return df, None

    # --- 以下阶段需要依赖前序阶段的 info，需要按 symbol 处理 ---
    has_symbol = "symbol" in df.columns

    # 收集所有通过阶段1+2的 symbol
    if has_symbol:
        common_syms = set(stage1_info.keys()) & set(stage2_info.keys())
    else:
        common_syms = {None}

    if not common_syms:
        return df, None

    merged_info = {}
    result_df = df.copy()

    for sym in common_syms:
        if has_symbol:
            mask = result_df["symbol"] == sym
            sub_df = result_df[mask].sort_index()
            s1 = stage1_info[sym]
            s2 = stage2_info[sym]
        else:
            sub_df = result_df.sort_index()
            s1 = stage1_info
            s2 = stage2_info

        # 阶段3
        sub_df3, s3 = _first_breakout_signal_single(
            sub_df, s2["box_end_date"], s2["box_high_price"],
            first_surge_ratio, volume_ratio, lookback_days,
        )
        if s3 is None:
            continue

        # 阶段4
        sub_df4, s4 = _retrace_second_breakout_signal_single(
            sub_df3, s3["first_break_point"]["date"],
            s3["breakout_close"], s3["breakout_high"],
            first_surge_ratio, retrace_range,
        )
        if s4 is None:
            continue

        # 写回结果
        result_df.loc[sub_df4.index, sub_df4.columns] = sub_df4

        # 合并 info
        merged_info[sym] = {**s1, **s2, **s3, **s4}

    if not merged_info:
        return result_df, None

    return result_df, merged_info
