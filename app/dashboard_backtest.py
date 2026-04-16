import streamlit as st
import requests
from vectorbt_test.core.portfolio import PortfolioType
from vectorbt_test.core.expr_parser import ExprParser
from vectorbt_test.engine.init import load_register_nodes
import re


@st.cache_data
def load_nodes(api_base: str):
    return requests.get(f"{api_base}/nodes").json()


class BacktestPage:

    def __init__(self, api_base: str):
        self.api_base = api_base
        self.nodes = load_nodes(api_base)
        # 👇 放这里（紧跟 nodes 后面）
        self.nodes_flat = [
            n["name"]
            for group in self.nodes.values()
            for n in group
        ]
        self.node_meta_map = {
            n["name"]: n
            for group in self.nodes.values()
            for n in group
        }
        load_register_nodes()
        self.parser = ExprParser()

    def _get_last_token(self, expr: str):
        if not expr:
            return ""

        tokens = re.findall(r"[A-Za-z_]+", expr)
        return tokens[-1] if tokens else ""

    def _suggest_nodes(self, prefix: str):
        # prefix = prefix.upper()
        return [
            name for name in self.nodes_flat
            if name.startswith(prefix)
        ]

    def _build_snippet(self, name: str):
        meta = self.node_meta_map.get(name)

        if not meta:
            return name

        if not meta["params"]:
            return name

        args = []

        for p in meta["params"]:
            default = p.get("default")

            # ✅ 没默认值就给安全值
            if default is None:
                if p["type"] == "int":
                    default = 1
                elif p["type"] == "float":
                    default = 0.0
                else:
                    default = ""

            # 字符串加引号
            if isinstance(default, str) and not default.isdigit():
                default = f"'{default}'"

            args.append(str(default))

        return f"{name}({', '.join(args)})"

    def _extract_functions(self, expr: str):
        import re

        pattern = r"([A-Za-z_][A-Za-z0-9_]*)\((.*?)\)"
        matches = re.finditer(pattern, expr)

        results = []

        for m in matches:
            name = m.group(1)
            args = m.group(2)
            full = m.group(0)

            results.append({
                "name": name,
                "args": args,
                "full": full,
                "start": m.start(),
                "end": m.end()
            })

        return results

    def _parse_args(self, args_str: str):
        args = args_str.split(",")
        return [a.strip() for a in args if a.strip()]

    def _edit_function_ui(self, expr: str, key: str):

        funcs = self._extract_functions(expr)

        if not funcs:
            return expr

        st.markdown("### 🛠 编辑函数参数")

        labels = [f["full"] for f in funcs]

        selected = st.selectbox(
            "选择函数",
            labels,
            key=f"{key}_func_select"
        )

        func = next(f for f in funcs if f["full"] == selected)

        meta = self.node_meta_map.get(func["name"])

        if not meta:
            return expr

        args = self._parse_args(func["args"])

        new_params = {}

        for i, p in enumerate(meta["params"]):
            default = args[i] if i < len(args) else p.get("default")

            new_params[p["name"]] = st.text_input(
                p["name"],
                value=str(default),
                key=f"{key}_param_{p['name']}"
            )

        if st.button("✅ 更新函数", key=f"{key}_update_func"):

            new_expr_part = self._build_expr(func["name"], new_params)

            # 替换原表达式
            new_expr = (
                expr[:func["start"]] +
                new_expr_part +
                expr[func["end"]:]
            )

            st.session_state[key] = new_expr
            st.rerun()

        return expr

    def _expr_with_autocomplete(self, label: str, key: str, default=""):
        if key not in st.session_state or st.session_state[key] is None:
            st.session_state[key] = default or ""

        expr = st.text_input(label, value=st.session_state[key], key=f"{key}_input")

        st.session_state[key] = expr

        # ===== 提取 token =====
        token = self._get_last_token(expr)

        suggestions = self._suggest_nodes(token) if token else []

        if suggestions:
            selected = st.selectbox(
                "🔍 自动补全",
                suggestions,
                key=f"{key}_suggest"
            )

            col1, col2 = st.columns(2)

            with col1:
                if st.button("插入名称", key=f"{key}_insert_name"):
                    # new_expr = expr[: -len(token)] + selected
                    snippet = self._build_snippet(selected)
                    new_expr = expr[: -len(token)] + snippet    
                    st.session_state[key] = new_expr
                    st.rerun()

            with col2:
                # if st.button("插入函数", key=f"{key}_insert_func"):
                #     snippet = self._build_snippet(selected)
                #     new_expr = expr[: -len(token)] + snippet
                #     st.session_state[key] = new_expr
                #     st.rerun()

                if st.button("插入函数", key=f"{key}_insert_func"):
                    snippet = self._build_snippet(selected)

                    if token:
                        new_expr = expr[: -len(token)] + snippet
                    else:
                        new_expr = expr + snippet

                    st.session_state[key] = new_expr
                    st.rerun()
                    
            # ===== 参数提示 =====
            if selected in self.node_meta_map:
                meta = self.node_meta_map[selected]

                if meta["params"]:
                    params_str = ", ".join(
                        f"{p['name']}={p['default']}"
                        for p in meta["params"]
                    )
                    st.caption(f"👉 {selected}({params_str})")

            expr = self._edit_function_ui(expr, key)

        return st.session_state[key]

    def _format_error_pointer(self, expr: str, error: Exception):
        """
        返回：
        RSI(14 >
            ^
        """

        if hasattr(error, "offset") and error.offset:
            pos = error.offset - 1  # Python offset 从1开始
        else:
            return expr  # 无法定位

        pointer = " " * pos + "^"
        return f"{expr}\n{pointer}"

    def _validate_expr(self, expr: str, label: str = ""):
        if not expr or not expr.strip():
            return False
        
        # =========================
        # ❗ 裸函数名（如 MA）
        # =========================
        if expr.strip() in self.node_meta_map:
            meta = self.node_meta_map[expr.strip()]

            if meta["params"]:
                st.warning(f"⚠️ {label} 需要参数，例如: {expr}(...)")
                return False
    
        try:
            self.parser.parse(expr)
            st.success(f"✅ {label} 表达式合法")
            return True

        except Exception as e:
            msg = str(e)

            # =========================
            # 🟡 未完成输入（忽略）
            # =========================
            incomplete_patterns = [
                "was never closed",
                "unexpected EOF",
                "EOF while parsing",
            ]

            if any(p in msg for p in incomplete_patterns):
                st.info(f"⌛ {label} 输入中...")
                return False

            # =========================
            # ❗ 参数缺失
            # =========================
            if "missing 1 required positional argument" in msg:
                st.error(f"❌ {label} 参数缺失，例如: MA(5)")
                return False

            # =========================
            # 🔥 语法错误定位
            # =========================
            if isinstance(e, SyntaxError):
                pointer_text = self._format_error_pointer(expr, e)
                st.error(f"❌ {label} 语法错误")
                st.code(pointer_text)
                return False

            # =========================
            # ❌ 其他错误
            # =========================
            st.error(f"❌ {label} 错误: {msg}")
            return False
        
    def _build_expr(self, name, params):
        if not params:
            return name
        args = ", ".join(str(v) for v in params.values())
        return f"{name}({args})"

    def _render_params(self, node_meta, prefix=""):
        values = {}

        for p in node_meta["params"]:
            key = f"{prefix}_{p['name']}"

            if p["type"] == "int":
                values[p["name"]] = st.number_input(
                    p["name"],
                    value=p["default"] or 1,
                    key=key
                )
            elif p["type"] == "float":
                values[p["name"]] = st.number_input(
                    p["name"],
                    value=float(p["default"] or 0.0),
                    key=key
                )
            else:
                values[p["name"]] = st.text_input(
                    p["name"],
                    value=str(p["default"] or ""),
                    key=key
                )

        return values

    # =========================
    # Strategy UI
    # =========================
    def _render_strategy(self, idx):
        with st.expander(f"Strategy {idx}", expanded=True):

            name = st.text_input("Strategy Name", value=f"strategy_{idx}", key=f"name_{idx}")

            # =========================
            # FACTORS
            # =========================
            st.markdown("#### Factors")

            factor_key = f"factors_{idx}"
            if factor_key not in st.session_state:
                st.session_state[factor_key] = []

            factors = st.session_state[factor_key]

            for i, f in enumerate(factors):
                col1, col2 = st.columns([8, 1])

                with col1:
                    st.code(f)

                with col2:
                    if st.button("❌", key=f"del_factor_{idx}_{i}"):
                        factors.pop(i)
                        st.rerun()

            if st.button("➕ 添加 Factor", key=f"add_factor_{idx}"):
                # st.session_state[f"show_factor_dialog_{idx}"] = True
                st.session_state["active_dialog"] = ("factor", idx)

            # =========================
            # SIGNAL
            # =========================
            st.markdown("#### Signal")

            signal_val = st.session_state.get(f"signal_{idx}", "")

            if signal_val:
                st.code(signal_val)

            if st.button("✏️ 编辑 Signal", key=f"edit_signal_{idx}"):
                # st.session_state[f"show_signal_dialog_{idx}"] = True
                st.session_state["active_dialog"] = ("signal", idx)

            # =========================
            # Dialog 触发（关键）
            # =========================
            # if st.session_state.get(f"show_factor_dialog_{idx}"):
            #     self.factor_dialog(idx)

            # if st.session_state.get(f"show_signal_dialog_{idx}"):
            #     self.signal_dialog(idx)

            # =========================
            # 输出
            # =========================
            factor_exprs = [f for f in factors if f.strip()]

            return {
                "name": name,
                "factors": factor_exprs,
                "signal": signal_val or None
            }

    # =========================
    # Portfolio UI（核心修正）
    # =========================
    def _render_portfolio(self, strategies):
        st.subheader("📦 Portfolio 配置")

        # =========================
        # Mode
        # =========================
        portfolio_mode = st.selectbox(
            "Portfolio Mode",
            [PortfolioType.SIGNAL_STRATEGY.value, 
             PortfolioType.WEIGHT_STRATEGY.value])

        # =========================
        # Schedule Signal（展示 + 弹窗）
        # =========================
        st.markdown("### ⏱ Schedule Signal")
    
        # 初始化
        if "schedule_signal" not in st.session_state:
            # st.session_state["schedule_signal"] = "RSI(14) > 70"
            st.session_state["schedule_signal"] = None

        schedule_s_enabled = st.checkbox(
            "启用 Schedule Signal",
            value=st.session_state["schedule_signal"] is not None,
            key="schedule_signal_enabled"
        )

        if schedule_s_enabled:
            schedule_signal = st.session_state.get("schedule_signal") or ""

            if schedule_signal:
                st.code(schedule_signal)
                self._validate_expr(schedule_signal, "Schedule Signal")

            if st.button("✏️ 编辑 Schedule Signal", key="edit_schedule_signal"):
                st.session_state["active_dialog"] = ("schedule", None)
        else:
            st.info("未启用 Schedule Signal")
            st.session_state["schedule_signal"] = None
            schedule_signal = None

        # 按钮
        # if st.button("✏️ 编辑 Schedule Signal", key="edit_schedule_signal"):
            # st.session_state["show_schedule_dialog"] = True
            # st.session_state["active_dialog"] = ("schedule", None)

        # 弹窗触发（关键）
        # if st.session_state.get("show_schedule_dialog"):
        #     self.schedule_signal_dialog()

        # =========================
        # TS / CS 配置
        # =========================
        extra = {}

        if portfolio_mode == PortfolioType.SIGNAL_STRATEGY.value:
            st.markdown("### Signal Strategy 配置")

            strategy_op = st.selectbox("Strategy Op", ["AND", "OR"])

            # 初始化vote_weights
            if "vote_weights" not in st.session_state:
                st.session_state["vote_weights"] = None

            vote_weights_enabled = st.checkbox(
                "启用 Vote Weights",
                value=st.session_state["vote_weights"] is not None,
                key="vote_weights_enabled"
            )

            if vote_weights_enabled:
                vote_weights = st.text_input("Vote Weights（逗号分隔）", value="1,1")
                vote_weights_list = [float(x) for x in vote_weights.split(",") if x]
            else:
                st.info("未启用 Vote Weights")
                vote_weights_list = None
                vote_weights = None
                
            if vote_weights_list is not None and len(vote_weights_list) != len(strategies):
                st.error("❗ vote_weights 数量必须等于 strategy 数量")

            extra = {
                "strategy_op": strategy_op,
                "vote_weights": vote_weights_list
            }

        else:
            st.markdown("### Weight Strategy 配置")

            strategy_weights = st.text_input("Strategy Weights（逗号分隔）", value="1,1")

            strategy_weights_list = [float(x) for x in strategy_weights.split(",") if x]

            if len(strategy_weights_list) != len(strategies):
                st.error("❗ strategy_weights 数量必须等于 strategy 数量")

            extra = {
                "strategy_weights": strategy_weights_list
            }

        return portfolio_mode, schedule_signal, extra

    # =========================
    # 参数
    # =========================
    def _render_params_panel(self):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            init_cash = st.number_input("初始资金", value=10000)
        with col2:
            top_n = st.number_input("top_n", value=10)
        with col3:
            hold_days = st.number_input("hold_days", value=5)
        with col4:
            freq = st.selectbox("freq", ["1D", "1H"])

        return {
            "freq": freq,
            "init_cash": init_cash,
            "top_n": top_n,
            "hold_days": hold_days
        }

    # =========================
    # 回测
    # =========================
    def _run_backtest(self, payload):
        res = requests.post(f"{self.api_base}/backtest", json=payload)
        if res.status_code != 200:
            st.error(res.text)
            return None
        return res.json()

    def _validate_strategies(self, strategies):
        errors = []

        for s in strategies:
            if not any(f.strip() for f in s["factors"]):
                errors.append(f"{s['name']} 没有 factor")

        return errors

    # =========================
    # 主入口
    # =========================
    def render(self):

        st.title("📈 Backtest System")

        # ===== Strategy 数量 =====
        n = st.number_input("Strategy 数量", 1, 10, 1)

        strategies = []
        for i in range(n):
            strategies.append(self._render_strategy(i))

        # ===== factor 非空校验 =====
        # has_error = False
        # for s in strategies:
        #     if not any(f.strip() for f in s["factors"]):
        #         st.error(f"❗ {s['name']} 至少需要一个有效 factor")
        #         has_error = True
        errors = self._validate_strategies(strategies)
        for e in errors:
            st.error(e)

        # ===== Portfolio =====
        portfolio_mode, schedule_signal, extra = self._render_portfolio(strategies)

        # ===== 参数 =====
        params = self._render_params_panel()

        payload = {
            "name": "SP1",
            "mode": portfolio_mode,
            "strategies": strategies,
            "schedule_signal": schedule_signal,
            "params": params,
            **extra
        }

        st.markdown("### 📦 Payload")
        st.json(payload)

        if st.button("🚀 Run Backtest"):
            if errors and len(errors) > 0:
                st.error("❌ 请先修复策略配置")
                return
            
            all_valid = True

            # 校验 factors
            for s in strategies:
                for f in s["factors"]:
                    if not self._validate_expr(f, f"{s['name']} Factor"):
                        all_valid = False

            # 校验 strategy signal
            for s in strategies:
                if s["signal"] and not self._validate_expr(s["signal"], f"{s['name']} Signal"):
                    all_valid = False

            # 校验 schedule signal
            if schedule_signal and not self._validate_expr(schedule_signal, "Schedule Signal"):
                all_valid = False

            if not all_valid:
                st.error("❌ 存在非法表达式，无法回测")
                return
            
            with st.spinner("Running..."):
                data = self._run_backtest(payload)

                if not data:
                    return

                st.success("Done")

                st.subheader("📊 Equity")
                st.line_chart(data["equity"])

                st.subheader("📈 Stats")
                st.json(data["stats"])

                st.subheader("📋 Trades")
                st.dataframe(data["trades"])

        self._render_dialogs()

    def _render_dialogs(self):

        dialog = st.session_state.get("active_dialog")

        if not dialog:
            return

        dialog_type, idx = dialog

        if dialog_type == "factor":
            self.factor_dialog(idx)

        elif dialog_type == "signal":
            self.signal_dialog(idx)

        elif dialog_type == "schedule":
            self.schedule_signal_dialog()

    def factor_dialog(self, idx):
        @st.dialog("添加 Factor")
        def _dialog():

            dialog_key = f"factor_dialog_expr_{idx}"

            # 初始化
            if dialog_key not in st.session_state:
                st.session_state[dialog_key] = ""

            mode = st.radio("模式", ["dsl", "visual"])

            if mode == "dsl":

                # ❗ 不用返回值，直接用 session_state
                self._expr_with_autocomplete(
                    "表达式",
                    key=dialog_key
                )

                expr = st.session_state[dialog_key] or ""

            else:
                col1, col2 = st.columns(2)

                with col1:
                    # 1️⃣ 选择 group
                    group = st.selectbox(
                        "类型",
                        list(self.nodes.keys()),
                        key=f"factor_group_{idx}"
                    )

                    # 2️⃣ 选择 node
                    meta = st.selectbox(
                        "节点",
                        self.nodes[group],
                        format_func=lambda x: x["name"],
                        key=f"factor_node_{idx}"
                    )
                    params = self._render_params(meta, f"factor_dialog_p_{idx}")

                with col2:
                    op = st.selectbox("运算符", ["", "+", "-", "*", "/"])

                expr = self._build_expr(meta["name"], params)

            # 展示
            if expr:
                st.code(expr)

            # ===== 按钮 =====
            col1, col2 = st.columns(2)

            with col1:
                if st.button("取消"):
                    # st.session_state[f"show_factor_dialog_{idx}"] = False
                    st.session_state["active_dialog"] = None
                    
                    st.rerun()

            with col2:
                if st.button("确定"):

                    # ❗ 从 session_state 拿最终值
                    final_expr = expr.strip()

                    if not final_expr:
                        st.error("表达式不能为空")
                        return

                    key = f"factors_{idx}"
                    if key not in st.session_state:
                        st.session_state[key] = []

                    st.session_state[key].append(final_expr)

                    # 清理 dialog state（非常重要）
                    del st.session_state[dialog_key]

                    # st.session_state[f"show_factor_dialog_{idx}"] = False
                    st.session_state["active_dialog"] = None
                    st.rerun()

        _dialog()

    def signal_dialog(self, idx):

        @st.dialog("编辑 Signal")
        def _dialog():

            mode = st.radio("模式", ["dsl", "visual"], key=f"signal_mode_{idx}")

            if mode == "dsl":
                # expr = self._expr_with_autocomplete(
                #     "Signal 表达式",
                #     key=f"signal_dialog_expr_{idx}"
                # )

                dialog_key = f"signal_dialog_expr_{idx}"

                if dialog_key not in st.session_state:
                    st.session_state[dialog_key] = ""

                self._expr_with_autocomplete(
                    "Signal 表达式",
                    key=dialog_key
                )

                expr = st.session_state[dialog_key] or ""

            else:
                col1, col2 = st.columns(2)

                with col1:
                    # 1️⃣ 选择 group
                    group = st.selectbox(
                        "类型",
                        list(self.nodes.keys()),
                        key=f"factor_group_{idx}"
                    )

                    # 2️⃣ 选择 node
                    meta = st.selectbox(
                        "节点",
                        self.nodes[group],
                        format_func=lambda x: x["name"],
                        key=f"factor_node_{idx}"
                    )
                    params = self._render_params(meta, f"signal_dialog_p_{idx}")

                with col2:
                    op = st.selectbox("比较符", [">", "<", ">=", "<="])
                    val = st.number_input("阈值", value=0.0)

                expr = f"{self._build_expr(meta['name'], params)} {op} {val}"
                st.code(expr)

            col1, col2 = st.columns(2)

            with col1:
                if st.button("取消"):
                    # st.session_state[f"show_signal_dialog_{idx}"] = False
                    st.session_state["active_dialog"] = None
                    st.rerun()

            with col2:
                if st.button("确定"):
                    # st.session_state[f"show_signal_dialog_{idx}"] = False
                    st.session_state["active_dialog"] = None
                    st.rerun()
            
        _dialog()

    def schedule_signal_dialog(self):
        @st.dialog("编辑 Schedule Signal")
        def _dialog():

            mode = st.radio(
                "模式",
                ["dsl", "visual"],
                key="schedule_signal_mode_dialog"
            )

            # ===== DSL =====
            if mode == "dsl":
                expr = self._expr_with_autocomplete(
                    "表达式",
                    key="schedule_signal_dialog_expr",
                    default=st.session_state.get("schedule_signal", "RSI(14) > 70")
                )

            # ===== 可视化 =====
            else:
                col1, col2 = st.columns(2)

                with col1:
                    # 1️⃣ 选择 group
                    group = st.selectbox(
                        "类型",
                        list(self.nodes.keys()),
                        key=f"factor_group_{idx}"
                    )

                    # 2️⃣ 选择 node
                    meta = st.selectbox(
                        "节点",
                        self.nodes[group],
                        format_func=lambda x: x["name"],
                        key=f"factor_node_{idx}"
                    )
                    params = self._render_params(meta, "schedule_signal_p_dialog")

                with col2:
                    op = st.selectbox("比较符", [">", "<", ">=", "<="])
                    val = st.number_input("阈值", value=0.0)

                expr = f"{self._build_expr(meta['name'], params)} {op} {val}"
                st.code(expr)

            # ===== 校验 =====
            if expr:
                self._validate_expr(expr, "Schedule Signal")

            # ===== 按钮 =====
            col1, col2 = st.columns(2)

            with col1:
                if st.button("取消"):
                    # st.session_state["show_schedule_dialog"] = False
                    st.session_state["active_dialog"] = None
                    st.rerun()

            with col2:
                if st.button("确定"):

                    final_expr = expr.strip()

                    if not final_expr:
                        st.session_state["schedule_signal"] = None
                    else:
                        st.session_state["schedule_signal"] = final_expr

                    st.session_state["active_dialog"] = None
                    st.rerun()

        _dialog()