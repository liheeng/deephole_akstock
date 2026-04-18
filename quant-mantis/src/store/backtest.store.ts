import { create } from "zustand"
import { v4 as uuidv4 } from 'uuid';

// =========================
// Types
// =========================
type PortfolioMode = "signal_strategy" | "weight_strategy"
type StrategyMode = "ts" | "cs"

interface Factor {
    id: string
    name?: string
    added: boolean
    expr: string
}

interface EnabledField<T> {
    enabled: boolean
    value: T
}

interface Strategy {
    id: string

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

    backtestResult: any; // or specify the type of backtestResult

    // ===== Actions =====
    setBacktestResult: (result: any) => void

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

    backtestResult: null,

    setBacktestResult: (result: any) => {
        set({
            backtestResult: result
        })
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
                    id: `signal_strategy_${uuidv4()}`,
                    name: `strategy_${state.strategies.length + 1}`,
                    strategy_mode: "ts",
                    factors: [{ id: 'factor_' +uuidv4(), added: false, expr: "" }],
                    signal: { enabled: false, value: "" },
                    threshold: { enabled: false, value: 0.5 },
                    top_n: { enabled: false, value: 10 },
                    collapsed: false,
                };
            } else {
                newStrategy = {
                    id: `weight_strategy_${uuidv4()}`,
                    name: `strategy_${state.strategies.length + 1}`,
                    strategy_mode: "cs",
                    factors: [{ id: 'factor_' +uuidv4(), added: false, expr: "" }],
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
                factors.push({ id: 'factor_' +uuidv4(), added: true, expr: "" })
            } else {
                factors.splice(afterIndex + 1, 0, { id: 'factor_' +uuidv4(), added: true, expr: "" })
            }

            return { strategies: next }
        }),

    updateFactor: (strategyIndex, factorIndex, expr) =>
        set(state => {
            const next = [...state.strategies]
            const factor = next[strategyIndex].factors[factorIndex];
            factor.expr = expr
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

        const mode =
            s.portfolio_mode === "signal_strategy"
                ? "SIGNAL_STRATEGY"
                : "WEIGHT_STRATEGY"

        const strategies = s.strategies.map(st => ({
            name: st.name,

            factors: st.factors
                .map(f => f.expr)
                .filter(f => f && f.trim()),

            signal:
                st.signal.enabled && st.signal.value
                    ? st.signal.value
                    : null,

            // ✅ 核心：按模式输出
            threshold:
                mode === "SIGNAL_STRATEGY" && st.threshold.enabled
                    ? st.threshold.value
                    : null,

            top_n:
                mode === "WEIGHT_STRATEGY" && st.top_n.enabled
                    ? st.top_n.value
                    : null
        }))

        const payload: any = {
            name: s.name,
            mode,
            strategies,

            schedule_signal:
                s.schedule_signal.enabled && s.schedule_signal.value
                    ? s.schedule_signal.value
                    : null,

            params: {
                freq: s.params.freq,
                init_cash: s.params.init_cash
            }
        }

        // ===== SIGNAL_STRATEGY =====
        if (mode === "SIGNAL_STRATEGY") {
            if (s.strategy_op.enabled) {
                payload.strategy_op = s.strategy_op.value
            }

            if (s.vote_weights.enabled) {
                payload.vote_weights = s.vote_weights.value
            }
        }

        // ===== WEIGHT_STRATEGY =====
        if (mode === "WEIGHT_STRATEGY") {
            if (s.strategy_weights.enabled) {
                payload.strategy_weights = s.strategy_weights.value
            }
        }

        return payload
    }

}))