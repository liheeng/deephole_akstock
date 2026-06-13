"""
选股策略 — Filter Chain 模式，支持跨阶段上下文引用。

每个 FilterStage 的 signal_expr 可含 {ctx.上阶段名.key} 占位符：
  stage1: "大跌", "HeavyDrop() & BoxConsolidation()"
  stage2: "突破", "VolumeBreakout(from_date={ctx.大跌.trigger_date})"
          →  自动替换为 VolumeBreakout(from_date=2024-03-15)
"""

from __future__ import annotations

import re
from typing import List, Dict, Any
import pandas as pd

from vectorbt_test.core.strategy import Strategy
from vectorbt_test.picker.pick import PickResult, StageResult, FilterStage
from vectorbt_test.picker.picker_signal import PickerSignal
from vectorbt_test.core.signals import Signal


class PickStrategy(Strategy):
    """
    选股策略（Filter Chain + 跨阶段上下文模式）。

    用法:
      strategy = PickStrategy("my_pick", stages=[
          FilterStage("大跌", "HeavyDrop() & BoxConsolidation()"),
          FilterStage("突破", "VolumeBreakout(from_date={ctx.大跌.trigger_date})",
                      time_scope="from_last"),
      ])
      result = strategy.generate(original_df, data_provider)
      print(result.summary())
    """

    def __init__(
        self,
        name: str,
        stages: List[FilterStage] | None = None,
    ):
        self.name = name
        self.stages = stages or []

    # ── 占位符解析 ──────────────────────────────────────────

    @staticmethod
    def _resolve_expr(expr: str, stage_context: Dict[str, Dict[str, Any]]) -> str:
        """
        将 signal_expr 中的 {ctx.阶段名.key} 替换为实际值。

        如 "VolumeBreakout(from_date={ctx.大跌.trigger_date})"
          → "VolumeBreakout(from_date=2024-03-15)"
        """

        def _replace(m: re.Match) -> str:
            ref = m.group(1)  # "大跌.trigger_date"
            parts = ref.split(".")
            if len(parts) < 2:
                return m.group(0)
            stage_name = parts[0]
            key = ".".join(parts[1:])
            val = stage_context.get(stage_name, {}).get(key)
            if val is None:
                return m.group(0)
            # 日期转字符串
            if isinstance(val, pd.Timestamp):
                return val.strftime("%Y-%m-%d")
            return str(val)

        return re.sub(r"\{ctx\.([^}]+)\}", _replace, expr)

    # ── 从 DataFrame 提取触发日价格等信息 ────────────────────

    @staticmethod
    def _extract_computed(
        stage_df: pd.DataFrame,
        signals: pd.DataFrame,
        selected: List[str],
        first_trigger: pd.Timestamp | None,
        stock_count_before: int,
    ) -> Dict[str, Any]:
        """
        从阶段执行结果中提取可被后续阶段引用的计算值。

        提取字段:
          - stock_count_before: 本阶段开始前的候选股票数
          - stock_count_after:  本阶段筛选后剩余的股票数
          - trigger_date:       首次触发日期（Timestamp 对象）
          - trigger_date_str:   首次触发日期（字符串 "YYYY-MM-DD"）
          - trigger_close/high/low/volume: 触发日被选中股票的均值

        这些值可通过 {ctx.阶段名.xxx} 在后续阶段的 signal_expr 中引用。
        """
        computed: Dict[str, Any] = {}
        computed["stock_count_before"] = stock_count_before
        computed["stock_count_after"] = len(selected)
        computed["trigger_date"] = first_trigger
        computed["trigger_date_str"] = (
            str(first_trigger.date()) if first_trigger else ""
        )

        # 若 DataFrame 有 OHLCV 列，提取触发日的价格/成交量
        if first_trigger is not None and not stage_df.empty:
            day_data = stage_df[stage_df["date"] == first_trigger]
            if not day_data.empty and "close" in day_data.columns:
                trigger_data = day_data[day_data["symbol"].isin(selected)]
                if not trigger_data.empty:
                    computed["trigger_close"] = trigger_data["close"].mean()
                    if "volume" in trigger_data.columns:
                        computed["trigger_volume"] = trigger_data["volume"].mean()
                    if "high" in trigger_data.columns:
                        computed["trigger_high"] = trigger_data["high"].mean()
                    if "low" in trigger_data.columns:
                        computed["trigger_low"] = trigger_data["low"].mean()

        return computed

    # ── 提取 per-stock 参考值（用于 set_refs）──────────────

    @staticmethod
    def _extract_ref_data(
        stage_df: pd.DataFrame,
        signals: pd.DataFrame,
        first_trigger: pd.Timestamp | None,
    ) -> Dict[str, pd.Series]:
        """
        从阶段执行结果中提取每只股票的触发日 OHLCV 值。

        返回格式: {"high": Series(index=symbols, values=...), "low": Series(...), ...}

        用途: 该数据通过 signal.set_refs() 注入给下一阶段的 PickerSignal 子类。
              例如 ShortBoxConsolidationSignal 需要 ref_high/ref_low 来判断
              股价是否在箱体内震荡。

        提取方式:
          1. 定位触发日 (first_trigger) 在 stage_df 中的行
          2. 对每只被选中的股票，提取当天 open/high/low/close/volume
          3. 按字段组织成 Series，key = 字段名，value = Series(symbol → 值)
        """
        if first_trigger is None or stage_df.empty:
            return {}
        day_data = stage_df[stage_df["date"] == first_trigger]
        if day_data.empty:
            return {}
        refs: Dict[str, Dict[str, float]] = {}
        for col in signals.columns:
            row = day_data[day_data["symbol"] == col]
            if not row.empty:
                r = row.iloc[0]
                refs[col] = {
                    "high": r["high"],
                    "low": r["low"],
                    "close": r["close"],
                    "volume": r["volume"],
                    "open": r["open"],
                }
        result: Dict[str, pd.Series] = {}
        for field in ["high", "low", "close", "volume", "open"]:
            values = {sym: data[field] for sym, data in refs.items()}
            result[field] = pd.Series(values)
        return result

    def generate(self, original_df: pd.DataFrame, data_provider) -> PickResult:
        """
        执行 Filter Chain 选股。

        stage_context 会在每个阶段结束后自动更新，
        后续阶段的 signal_expr 可通过 {ctx.阶段名.key} 引用。

        Args:
            original_df: 原始长格式 DataFrame
            data_provider: DataProvider

        Returns:
            PickResult（含 stage_context）
        """
        from vectorbt_test.engine.context_builder import create_context

        # ── 初始化 ────────────────────────────────────────────
        # remaining_symbols: 当前阶段剩余候选股票，初始为全量
        # current_df:        原始数据的副本，后续不做修改，仅用于按 symbol 过滤
        # stage_context:     跨阶段上下文，{阶段名: {key: value}}，供 {ctx.xxx} 占位符引用
        # last_trigger_date: 上一阶段首次触发日期，用于 from_last 时间裁剪
        # last_stage_name:   上一阶段名称，用于查找 ref_data
        remaining_symbols: List[str] = list(original_df["symbol"].unique())
        current_df = original_df.copy()
        stage_results: List[StageResult] = []
        stage_context: Dict[str, Dict[str, Any]] = {}
        last_trigger_date: pd.Timestamp | None = None
        last_stage_name: str | None = None

        for stage in self.stages:
            stock_count_before = len(remaining_symbols)

            # ═══════════════════════════════════════════════════
            #  Step 1: 用剩余股票过滤
            #  只保留上一阶段筛选出的股票，缩小数据范围
            # ═══════════════════════════════════════════════════
            stage_df = current_df[current_df["symbol"].isin(remaining_symbols)].copy()
            if stage_df.empty:
                # 无剩余股票 → 终止链，记录空结果
                sr = StageResult(
                    stage=stage, signals=None, remaining_symbols=[], trigger_date=None
                )
                stage_results.append(sr)
                stage_context[stage.name] = {
                    "stock_count_before": stock_count_before,
                    "stock_count_after": 0,
                }
                remaining_symbols = []
                break

            # ═══════════════════════════════════════════════════
            #  Step 2: 解析 signal_expr 中的跨阶段占位符
            #  将 {ctx.阶段名.key} 替换为 stage_context 中的实际值
            #  如 "VolumeBreakout(from_date={ctx.大跌.trigger_date})"
            #    → "VolumeBreakout(from_date=2024-03-15)"
            #  此步骤在数据裁剪前执行，以便提前解析出信号类获取 lookback_buffer。
            # ═══════════════════════════════════════════════════
            resolved_expr = self._resolve_expr(stage.signal_expr, stage_context)
            if stage.params:
                for key, val_expr in stage.params.items():
                    resolved_val = self._resolve_expr(val_expr, stage_context)
                    resolved_expr = resolved_expr.replace("{" + key + "}", resolved_val)

            # ═══════════════════════════════════════════════════
            #  Step 3: 预构建 Signal 对象获取 lookback_buffer
            #  提前解析信号以获取其 lookback_buffer 属性，与 stage.lookback_buffer
            #  取最大值，确保数据裁剪时保留足够的回溯历史（如均量计算需要的前置窗口）。
            # ═══════════════════════════════════════════════════
            signal: Signal | None = Signal.build(resolved_expr)
            if signal is None:
                raise ValueError(f"无法解析信号: {resolved_expr}")

            # 取 stage 和 signal 各自 lookback_buffer 的较大值
            signal_lb = getattr(signal, 'lookback_buffer', 0) or 0
            effective_lb = max(stage.lookback_buffer, signal_lb)

            # ═══════════════════════════════════════════════════
            #  Step 4: 根据 time_scope 裁剪时间范围
            #  "from_last": 只保留从上阶段触发日到现在的数据
            #  "full":      保留全部时间范围（不需要裁剪）
            #  lookback_buffer 确保信号有足够的回溯历史数据。
            # ═══════════════════════════════════════════════════
            if stage.time_scope == "from_last" and last_trigger_date is not None:
                from_date = last_trigger_date - pd.Timedelta(days=effective_lb)
                stage_df = stage_df[stage_df["date"] >= from_date].copy()
                if stage_df.empty:
                    sr = StageResult(
                        stage=stage,
                        signals=None,
                        remaining_symbols=[],
                        trigger_date=None,
                    )
                    stage_results.append(sr)
                    stage_context[stage.name] = {
                        "stock_count_before": stock_count_before,
                        "stock_count_after": 0,
                    }
                    remaining_symbols = []
                    break

            # ═══════════════════════════════════════════════════
            #  Step 5: 构建 PortfolioContext 并评估信号
            #  1) create_context 将长格式 DataFrame 转为 DataAdapter/DataView
            #  2) 若信号继承 PickerSignal，注入上一阶段的 per-stock ref_data
            #  3) signal.evaluate 执行实际计算，返回 bool DataFrame
            # ═══════════════════════════════════════════════════
            ctx = create_context(stage_df, data_provider)
            data = ctx.data_adapter.data_view()

            # 注入 per-stock ref_values（仅在上一阶段有 ref_data 时触发）
            # ref_data 中包含每只股票在上一阶段触发日的 OHLCV（high/low/close/volume/open）
            if (
                isinstance(signal, PickerSignal)
                and last_stage_name
                and "ref_data" in stage_context.get(last_stage_name, {})
            ):
                prev_refs = stage_context[last_stage_name]["ref_data"]
                signal.set_refs(prev_refs)

            signals = signal.evaluate(data, ctx)

            # 单只股票时，signal.evaluate 返回 Series → 统一转为 DataFrame
            if isinstance(signals, pd.Series):
                signals = signals.to_frame(stage.name)

            # ═══════════════════════════════════════════════════
            #  Step 5: 获取最新一天的触发结果
            #  signals 是 bool DataFrame (dates × symbols)
            #  取最后一行（最新交易日），找出被选中的股票
            # ═══════════════════════════════════════════════════
            latest_date = signals.index[-1]
            row = signals.loc[latest_date]
            selected = row[row].index.tolist()

            # ═══════════════════════════════════════════════════
            #  Step 6: 查找首次触发日期（从前往后扫 → 最早触发日）
            #  用于 {ctx.阶段名.trigger_date} 引用和 ref_data 提取。
            #
            #  注意: 由于信号输出通常带 cummax (一旦触发永久 True)，
            #  必须从前往后扫才能找到真正的首次触发日期。
            #  从后往前扫会因 cummax 特性而永远返回最后一天。
            # ═══════════════════════════════════════════════════
            first_trigger = None
            for dt in signals.index:
                if signals.loc[dt].any():
                    first_trigger = dt
                    break

            # ═══════════════════════════════════════════════════
            #  Step 7: 计算阶段截断日期（stage_cutoff）
            #  该日期是当前阶段的有效终点，下一阶段 from_last 以此为起点。
            #
            #  规则:
            #    如果信号有 recent_days 属性（如 BoxConsolidation、VolumeBreakout），
            #      cutoff = 最后一天 - recent_days + 1 个交易日
            #      因为信号在最后一天 i=n-1 时，实际只处理到 i - recent_days 位置。
            #    否则（如 HeavyDrop 无窗口末尾参数），
            #      cutoff = first_trigger（最早触发日）
            # ═══════════════════════════════════════════════════
            signal_recent = getattr(signal, 'recent_days', None)
            if signal_recent and not stage_df.empty:
                last_dt = stage_df["date"].max()
                # 找到 last_dt 前第 (recent_days - 1) 个交易日
                sorted_dates = sorted(stage_df["date"].unique())
                last_idx = sorted_dates.index(last_dt)
                cutoff_idx = max(0, last_idx - (signal_recent - 1))
                stage_cutoff = pd.Timestamp(sorted_dates[cutoff_idx])
            else:
                stage_cutoff = first_trigger

            # ═══════════════════════════════════════════════════
            #  Step 8: 提取计算值与 per-stock ref_data
            #  computed: 全局统计（均值），用于 {ctx} 占位符
            #  ref_data: 每只股票触发日的 OHLCV，供后续 PickerSignal 子类使用
            # ═══════════════════════════════════════════════════
            computed = self._extract_computed(
                stage_df, signals, selected, first_trigger, stock_count_before
            )
            ref_data = self._extract_ref_data(stage_df, signals, first_trigger)
            computed["ref_data"] = ref_data
            computed["stage_cutoff"] = stage_cutoff

            # ── 保存阶段执行结果 ──────────────────────────────
            stage_results.append(
                StageResult(
                    stage=stage,
                    signals=signals,          # bool DataFrame (dates × symbols)
                    remaining_symbols=selected, # 通过该阶段筛选的股票
                    trigger_date=first_trigger, # 首次触发日期
                    computed=computed,         # 统计值 + ref_data
                )
            )

            # ── 更新跨阶段上下文 ──────────────────────────────
            # 将 computed 存入 stage_context，下一阶段可通过 {ctx.阶段名.key} 引用
            stage_context[stage.name] = computed
            # 记录上一阶段名称，供下次迭代注入 ref_data
            last_stage_name = stage.name
            # 更新剩余股票列表，传给下一阶段
            remaining_symbols = selected
            # 更新阶段截断日期，用于下一阶段的 from_last 时间裁剪
            # from_last = 本阶段的 stage_cutoff（信号处理窗口的终点 + 1 个交易日）
            # 这样下一阶段的数据从 stage_cutoff 开始，不会重复处理已覆盖的时间段
            if stage_cutoff is not None:
                last_trigger_date = stage_cutoff

            # ── 终止条件 ──────────────────────────────────────
            # 若当前阶段没有股票通过筛选，后续阶段无数据可算，终止链
            if not remaining_symbols:
                break

        # ═══════════════════════════════════════════════════════
        #  返回 PickResult
        #  包含: 每阶段结果 + 最终剩余股票 + 跨阶段上下文
        # ═══════════════════════════════════════════════════════
        return PickResult(
            type="pick",
            name=self.name,
            stage_results=stage_results,
            remaining_symbols=remaining_symbols,
            stage_context=stage_context,
        )
