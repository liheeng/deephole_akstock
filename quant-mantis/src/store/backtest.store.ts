import { create } from "zustand"

// =========================
// Types
// =========================
type PortfolioMode = "signal_strategy" | "weight_strategy"
type StrategyMode = "ts" | "cs"

interface Factor {
    expr: string
}

interface EnabledField<T> {
    enabled: boolean
    value: T
}

interface Strategy {
    name: string
    strategy_mode: StrategyMode

    factors: Factor[]

    signal: EnabledField<string>

    threshold: EnabledField<number>
    top_n: EnabledField<number>

    collapsed?: boolean
}

interface BacktestState {

    name: string
    portfolio_mode: PortfolioMode

    params: {
        freq: string
        init_cash: number
    }

    schedule_signal: EnabledField<string>

    strategy_op: EnabledField<"AND" | "OR">
    vote_weights: EnabledField<number[]>
    strategy_weights: EnabledField<number[]>

    strategies: Strategy[]

    dialog: {
        open: boolean
        type: "factor" | "signal" | "schedule_signal"
        strategyIndex?: number
        factorIndex?: number
    }

    // ===== Actions =====
    setPortfolioMode: (mode: PortfolioMode) => void

    setParams: (patch: Partial<{ freq: string; init_cash: number }>) => void

    setScheduleSignal: (patch: Partial<EnabledField<string>>) => void
    toggleScheduleSignal: () => void

    setStrategyOp: (patch: Partial<EnabledField<"AND" | "OR">>) => void
    toggleStrategyOp: () => void

    setVoteWeights: (patch: Partial<EnabledField<number[]>>) => void
    toggleVoteWeights: () => void

    setStrategyWeights: (patch: Partial<EnabledField<number[]>>) => void
    toggleStrategyWeights: () => void

    addStrategy: () => void
    removeStrategy: (index: number) => void
    updateStrategy: (index: number, patch: Partial<Strategy>) => void

    setStrategyMode: (index: number, mode: StrategyMode) => void
    toggleStrategyCollapse: (index: number) => void

    setStrategySignal: (index: number, patch: Partial<EnabledField<string>>) => void
    toggleStrategySignal: (index: number) => void

    setThreshold: (index: number, patch: Partial<EnabledField<number>>) => void
    toggleThreshold: (index: number) => void

    setTopN: (index: number, patch: Partial<EnabledField<number>>) => void
    toggleTopN: (index: number) => void

    addFactor: (strategyIndex: number, afterIndex?: number) => void
    updateFactor: (strategyIndex: number, factorIndex: number, expr: string) => void
    deleteFactor: (strategyIndex: number, factorIndex: number) => void

    openDialog: (payload: Partial<BacktestState["dialog"]>) => void
    closeDialog: () => void

    buildPayload: () => any
}

// =========================
// Store
// =========================
export const useBacktestStore = create<BacktestState>((set, get) => ({

    name: "MyPortfolio",

    portfolio_mode: "signal_strategy",

    params: {
        freq: "1D",
        init_cash: 100000
    },

    schedule_signal: {
        enabled: false,
        value: ""
    },

    strategy_op: {
        enabled: true,
        value: "OR"
    },

    vote_weights: {
        enabled: false,
        value: []
    },

    strategy_weights: {
        enabled: false,
        value: []
    },

    strategies: [],

    dialog: {
        open: false,
        type: "factor"
    },

    // =========================
    // Portfolio
    // =========================
    setPortfolioMode: (mode) => {
        set(state => {
            if (mode === "signal_strategy") {
                return {
                    portfolio_mode: mode,
                    strategy_weights: { ...state.strategy_weights, enabled: false }
                }
            }
            if (mode === "weight_strategy") {
                return {
                    portfolio_mode: mode,
                    strategy_op: { ...state.strategy_op, enabled: false },
                    vote_weights: { ...state.vote_weights, enabled: false }
                }
            }
            return { portfolio_mode: mode }
        })
    },

    setParams: (patch) =>
        set(state => ({
            params: { ...state.params, ...patch }
        })),

    setScheduleSignal: (patch) =>
        set(state => ({
            schedule_signal: { ...state.schedule_signal, ...patch }
        })),

    toggleScheduleSignal: () =>
        set(state => ({
            schedule_signal: {
                ...state.schedule_signal,
                enabled: !state.schedule_signal.enabled
            }
        })),

    setStrategyOp: (patch) =>
        set(state => ({
            strategy_op: { ...state.strategy_op, ...patch }
        })),

    toggleStrategyOp: () =>
        set(state => ({
            strategy_op: {
                ...state.strategy_op,
                enabled: !state.strategy_op.enabled
            }
        })),

    setVoteWeights: (patch) =>
        set(state => ({
            vote_weights: { ...state.vote_weights, ...patch }
        })),

    toggleVoteWeights: () =>
        set(state => ({
            vote_weights: {
                ...state.vote_weights,
                enabled: !state.vote_weights.enabled
            }
        })),

    setStrategyWeights: (patch) =>
        set(state => ({
            strategy_weights: { ...state.strategy_weights, ...patch }
        })),

    toggleStrategyWeights: () =>
        set(state => ({
            strategy_weights: {
                ...state.strategy_weights,
                enabled: !state.strategy_weights.enabled
            }
        })),

    // =========================
    // Strategy
    // =========================
    addStrategy: () =>
        set((state) => {
            // 先定义变量
            let newStrategy: Strategy;

            // 根据模式判断生成策略
            if (state.portfolio_mode === "signal_strategy") {
            newStrategy = {
                name: `strategy_${state.strategies.length + 1}`,
                strategy_mode: "ts",
                factors: [{ expr: "" }],
                signal: { enabled: false, value: "" },
                threshold: { enabled: false, value: 0.5 },
                top_n: { enabled: false, value: 10 },
                collapsed: false,
            };
            } else {
            newStrategy = {
                name: `strategy_${state.strategies.length + 1}`,
                strategy_mode: "cs",
                factors: [{ expr: "" }],
                signal: { enabled: false, value: "" },
                threshold: { enabled: false, value: 0.5 },
                top_n: { enabled: false, value: 10 },
                collapsed: false,
            };
            }

            // 必须返回新状态
            return {
                strategies: [
                    ...state.strategies, 
                    newStrategy
                ],
            };
        }),

    removeStrategy: (index) =>
        set(state => ({
            strategies: state.strategies.filter((_, i) => i !== index)
        })),

    updateStrategy: (index, patch) =>
        set(state => {
            const next = [...state.strategies]
            next[index] = { ...next[index], ...patch }
            return { strategies: next }
        }),

    setStrategyMode: (index, mode) =>
        set(state => {
            const next = [...state.strategies]
            next[index].strategy_mode = mode
            return { strategies: next }
        }),

    toggleStrategyCollapse: (index) =>
        set(state => {
            const next = [...state.strategies]
            next[index].collapsed = !next[index].collapsed
            return { strategies: next }
        }),

    setStrategySignal: (index, patch) =>
        set(state => {
            const next = [...state.strategies]
            next[index].signal = { ...next[index].signal, ...patch }
            return { strategies: next }
        }),

    toggleStrategySignal: (index) =>
        set(state => {
            const next = [...state.strategies]
            next[index].signal.enabled = !next[index].signal.enabled
            return { strategies: next }
        }),

    setThreshold: (index, patch) =>
        set(state => {
            const next = [...state.strategies]
            next[index].threshold = { ...next[index].threshold, ...patch }
            return { strategies: next }
        }),

    toggleThreshold: (index) =>
        set(state => {
            const next = [...state.strategies]
            next[index].threshold.enabled = !next[index].threshold.enabled
            return { strategies: next }
        }),

    setTopN: (index, patch) =>
        set(state => {
            const next = [...state.strategies]
            next[index].top_n = { ...next[index].top_n, ...patch }
            return { strategies: next }
        }),

    toggleTopN: (index) =>
        set(state => {
            const next = [...state.strategies]
            next[index].top_n.enabled = !next[index].top_n.enabled
            return { strategies: next }
        }),

    // =========================
    // Factor
    // =========================
    addFactor: (strategyIndex, afterIndex) =>
        set(state => {
            const next = [...state.strategies]
            const factors = next[strategyIndex].factors

            if (afterIndex === undefined) {
                factors.push({ expr: "" })
            } else {
                factors.splice(afterIndex + 1, 0, { expr: "" })
            }

            return { strategies: next }
        }),

    updateFactor: (strategyIndex, factorIndex, expr) =>
        set(state => {
            const next = [...state.strategies]
            next[strategyIndex].factors[factorIndex].expr = expr
            return { strategies: next }
        }),

    deleteFactor: (strategyIndex, factorIndex) =>
        set(state => {
            const next = [...state.strategies]
            next[strategyIndex].factors.splice(factorIndex, 1)
            return { strategies: next }
        }),

    // =========================
    // Dialog
    // =========================
    openDialog: (payload) =>
        set({
            dialog: {
                open: true,
                type: "factor",
                ...payload
            }
        }),

    closeDialog: () =>
        set({
            dialog: { open: false, type: "factor" }
        }),

    // =========================
    // Payload
    // =========================
    buildPayload: () => {
        const s = get()

        return {
            name: s.name,
            portfolio_mode: s.portfolio_mode,
            params: s.params,

            schedule_signal:
                s.schedule_signal.enabled && s.schedule_signal.value
                    ? s.schedule_signal.value
                    : null,

            strategies: s.strategies.map(st => ({
                name: st.name,
                strategy_mode: st.strategy_mode,
                factors: st.factors.map(f => f.expr).filter(Boolean),

                signal:
                    st.signal.enabled && st.signal.value
                        ? st.signal.value
                        : null,

                threshold:
                    s.portfolio_mode === "signal_strategy" && st.threshold.enabled
                        ? st.threshold.value
                        : null,

                top_n:
                    s.portfolio_mode === "weight_strategy" && st.top_n.enabled
                        ? st.top_n.value
                        : null
            })),

            strategy_op:
                s.portfolio_mode === "signal_strategy" && s.strategy_op.enabled
                    ? s.strategy_op.value
                    : null,

            vote_weights:
                s.portfolio_mode === "signal_strategy" && s.vote_weights.enabled
                    ? s.vote_weights.value
                    : null,

            strategy_weights:
                s.portfolio_mode === "weight_strategy" && s.strategy_weights.enabled
                    ? s.strategy_weights.value
                    : null
        }
    }

}))