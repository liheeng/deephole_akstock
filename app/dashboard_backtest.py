import streamlit as st
import requests
from vectorbt_test.core.expr_parser import ExprParser
import traceback
from vectorbt_test.engine.init import load_register_nodes

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
        import re
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

        if not meta or not meta["params"]:
            return name

        # 生成参数占位
        placeholders = ", ".join(" " for _ in meta["params"])

        return f"{name}({placeholders})"

    def _expr_with_autocomplete(self, label: str, key: str, default=""):

        if key not in st.session_state:
            st.session_state[key] = default

        expr = st.text_input(label, value=st.session_state[key], key=f"{key}_input")

        st.session_state[key] = expr

        # ===== 获取当前 token =====
        token = self._get_last_token(expr)

        # == show parameters
        if token in self.node_meta_map:
            meta = self.node_meta_map[token]

            if meta["params"]:
                params_str = ", ".join(
                    f"{p['name']}={p['default']}"
                    for p in meta["params"]
                )

                st.caption(f"👉 参数: {token}({params_str})")

        suggestions = self._suggest_nodes(token) if token else []

        if suggestions:
            selected = st.selectbox(
                "🔍 自动补全",
                suggestions,
                key=f"{key}_suggest"
            )

            if st.button("插入", key=f"{key}_insert"):
                # 替换最后一个 token
                new_expr = expr[: -len(token)] + selected
                st.session_state[key] = new_expr
                st.rerun()

        return st.session_state[key]

    def _validate_expr(self, expr: str, label: str = ""):
        if not expr or not expr.strip():
            return False

        try:
            self.parser.parse(expr)
            st.success(f"✅ {label} 表达式合法")
            return True
        except Exception as e:
            e.__traceback__
            st.error(f"❌ {label} 错误: {str(e)}\n{traceback.format_exc()}")
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
                st.session_state[factor_key] = [{"mode": "dsl", "value": ""}]

            factors = st.session_state[factor_key]
            new_factors = []

            for i, f in enumerate(factors):

                st.markdown(f"##### Factor {i}")

                col_mode, col_del = st.columns([6, 1])

                with col_mode:
                    mode = st.selectbox(
                        "模式",
                        ["dsl", "visual"],
                        index=0 if f["mode"] == "dsl" else 1,
                        key=f"{factor_key}_mode_{i}"
                    )

                with col_del:
                    if st.button("❌", key=f"del_factor_{idx}_{i}"):
                        continue

                # ===== DSL 模式 =====
                if mode == "dsl":
                    # val = st.text_input(
                    #     "表达式",
                    #     value=f["value"],
                    #     key=f"{factor_key}_dsl_{i}"
                    # )
                    val = self._expr_with_autocomplete(
                        f"Factor {i}",
                        key=f"{factor_key}_dsl_{i}",
                        default=f["value"]
                    )

                # ===== 可视化模式 =====
                else:
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        group = st.selectbox(
                            "类型",
                            list(self.nodes.keys()),
                            key=f"{factor_key}_g_{i}"
                        )
                        meta = st.selectbox(
                            "节点",
                            self.nodes[group],
                            format_func=lambda x: x["name"],
                            key=f"{factor_key}_n_{i}"
                        )

                    with col2:
                        params = self._render_params(meta, f"{factor_key}_p_{i}")

                    with col3:
                        op = st.selectbox(
                            "运算符",
                            ["", "+", "-", "*", "/", "&", "|"],
                            key=f"{factor_key}_op_{i}"
                        )

                    val = self._build_expr(meta["name"], params)

                    # 可扩展：简单二元
                    if op:
                        val = f"{val} {op} {val}"

                new_factors.append({
                    "mode": mode,
                    "value": val
                })

                st.code(val)
                self._validate_expr(val, f"Factor {i}")

            if st.button("➕ 添加 Factor", key=f"add_factor_{idx}"):
                new_factors.append({"mode": "dsl", "value": ""})

            st.session_state[factor_key] = new_factors

            # =========================
            # SIGNAL
            # =========================
            st.markdown("#### Signal")

            signal_mode = st.selectbox(
                "Signal 模式",
                ["dsl", "visual"],
                key=f"signal_mode_{idx}"
            )

            if signal_mode == "dsl":
                # signal_val = st.text_input("Signal 表达式", key=f"signal_{idx}")
                signal_val = self._expr_with_autocomplete(
                    "Signal 表达式",
                    key=f"signal_{idx}",
                    default=""
                )

            else:
                col1, col2 = st.columns(2)

                with col1:
                    meta = st.selectbox(
                        "节点",
                        self.nodes["indicator"],
                        format_func=lambda x: x["name"],
                        key=f"signal_node_{idx}"
                    )
                    params = self._render_params(meta, f"signal_p_{idx}")

                with col2:
                    cmp_op = st.selectbox("比较符", [">", "<", ">=", "<="])
                    cmp_val = st.number_input("阈值", value=0.0)

                signal_val = f"{self._build_expr(meta['name'], params)} {cmp_op} {cmp_val}"

            if signal_val:
                st.code(signal_val)
                self._validate_expr(signal_val, "Strategy Signal")

            # =========================
            # 输出（重要）
            # =========================
            factor_exprs = [f["value"] for f in new_factors if f["value"].strip()]

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

        portfolio_mode = st.selectbox("Portfolio Mode", ["ts", "cs"])

        st.markdown("### ⏱ Schedule Signal")

        signal_mode = st.selectbox(
            "Schedule Signal 模式",
            ["dsl", "visual"],
            key="schedule_signal_mode"
        )

        # ===== DSL 模式 =====
        if signal_mode == "dsl":
            # schedule_signal = st.text_input(
            #     "表达式",
            #     value=st.session_state.get("schedule_signal", "RSI(14) > 70"),
            #     key="schedule_signal_input"
            # )
            schedule_signal = self._expr_with_autocomplete(
                "Schedule Signal",
                key="schedule_signal",
                default="RSI(14) > 70"
            )

        # ===== 可视化模式 =====
        else:
            col1, col2 = st.columns(2)

            with col1:
                meta = st.selectbox(
                    "节点",
                    self.nodes["indicator"],
                    format_func=lambda x: x["name"],
                    key="schedule_signal_node"
                )
                params = self._render_params(meta, "schedule_signal_p")

            with col2:
                cmp_op = st.selectbox(
                    "比较符",
                    [">", "<", ">=", "<=", "=="],
                    key="schedule_signal_op"
                )
                cmp_val = st.number_input(
                    "阈值",
                    value=0.0,
                    key="schedule_signal_val"
                )

            schedule_signal = f"{self._build_expr(meta['name'], params)} {cmp_op} {cmp_val}"

        # ===== 展示 =====
        if schedule_signal:
            st.code(schedule_signal)
            self._validate_expr(schedule_signal, "Schedule Signal")

        st.session_state["schedule_signal"] = schedule_signal

        extra = {}

        # ===== TS =====
        if portfolio_mode == "ts":
            st.markdown("### TS 配置")

            strategy_op = st.selectbox("Strategy Op", ["AND", "OR"])

            vote_weights = st.text_input("Vote Weights（逗号分隔）", value="1,1")

            vote_weights_list = [float(x) for x in vote_weights.split(",") if x]

            if len(vote_weights_list) != len(strategies):
                st.error("❗ vote_weights 数量必须等于 strategy 数量")

            extra = {
                "strategy_op": strategy_op,
                "vote_weights": vote_weights_list
            }

        # ===== CS =====
        else:
            st.markdown("### CS 配置")

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
        for s in strategies:
            if not any(f.strip() for f in s["factors"]):
                st.error(f"❗ {s['name']} 至少需要一个有效 factor")
                return

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