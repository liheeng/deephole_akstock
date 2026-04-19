import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"

// =========================
// Types
// =========================
type PortfolioMode = "signal_strategy" | "weight_strategy"
type StrategyMode = "ts" | "cs"

interface Factor {
    id: string
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

    factors: Record<string, Factor>
    factorIds: string[]

    signal: EnabledField<string>

    threshold?: EnabledField<number>
    top_n?: EnabledField<number>

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

    strategies: Record<string, Strategy>
    strategyIds: string[]

    // ===== actions =====
    setPortfolioMode: (mode: PortfolioMode) => void
    setParams: (patch: Partial<{ freq: string; init_cash: number }>) => void

    setScheduleSignal: (patch: Partial<EnabledField<string>>) => void
    toggleScheduleSignal: () => void

    addStrategy: () => void
    removeStrategy: (strategyId: string) => void
    updateStrategy: (strategyId: string, patch: Partial<Strategy>) => void

    updateStrategyMode: (strategyId: string, mode: StrategyMode) => void

    updateStrategySignal: (strategyId: string, patch: Partial<EnabledField<string>>) => void

    setStrategyThreshold: (strategyId: string, patch: Partial<EnabledField<number>>) => void
    setStrategyTopN: (strategyId: string, patch: Partial<EnabledField<number>>) => void

    addFactor: (strategyId: string, afterId?: string) => void
    updateFactor: (strategyId: string, factorId: string, expr: string) => void
    deleteFactor: (strategyId: string, factorId: string) => void

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

    strategies: {},
    strategyIds: [],

    // =========================
    // Portfolio
    // =========================
    setPortfolioMode: (mode) => set({ portfolio_mode: mode }),

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

    // =========================
    // Strategy
    // =========================
    addStrategy: () =>
        set(state => {
            const id = `strategy_${uuidv4()}`
            const fid = `factor_${uuidv4()}`

            const base = {
                id,
                name: `strategy_${state.strategyIds.length + 1}`,
                factors: {
                    [fid]: { id: fid, added: false, expr: "" }
                },
                factorIds: [fid],
                signal: { enabled: false, value: "" },
                collapsed: false
            }

            const strategy: Strategy =
                state.portfolio_mode === "signal_strategy"
                    ? {
                        ...base,
                        strategy_mode: "ts",
                        threshold: { enabled: false, value: 0.5 }
                    }
                    : {
                        ...base,
                        strategy_mode: "cs",
                        top_n: { enabled: false, value: 10 }
                    }

            return {
                strategies: {
                    ...state.strategies,
                    [id]: strategy
                },
                strategyIds: [...state.strategyIds, id]
            }
        }),

    removeStrategy: (strategyId) =>
        set(state => {
            const next = { ...state.strategies }
            delete next[strategyId]

            return {
                strategies: next,
                strategyIds: state.strategyIds.filter(x => x !== strategyId)
            }
        }),

    updateStrategy: (strategyId, patch) =>
        set(state => {
            const prev = state.strategies[strategyId]
            if (!prev) return state

            // ❗禁止覆盖嵌套对象
            const { signal, threshold, top_n, ...safePatch } = patch

            return {
                strategies: {
                    ...state.strategies,
                    [strategyId]: {
                        ...prev,
                        ...safePatch
                    }
                }
            }
        }),

    updateStrategyMode: (id, mode) =>
        set(state => {
            const st = state.strategies[id]

            if (mode === "ts") {
                return {
                    strategies: {
                        ...state.strategies,
                        [id]: {
                            ...st,
                            strategy_mode: mode,
                            threshold: st.threshold ?? { enabled: false, value: 0.5 },
                            top_n: undefined
                        }
                    }
                }
            }

            if (mode === "cs") {
                return {
                    strategies: {
                        ...state.strategies,
                        [id]: {
                            ...st,
                            strategy_mode: mode,
                            top_n: st.top_n ?? { enabled: false, value: 10 },
                            threshold: undefined
                        }
                    }
                }
            }

            return state
        }),

    updateStrategySignal: (strategyId, patch) =>
        set(state => {
            const st = state.strategies[strategyId]

            return {
                strategies: {
                    ...state.strategies,
                    [strategyId]: {
                        ...st,
                        signal: {
                            ...st.signal,
                            ...patch
                        }
                    }
                }
            }
        }),

    setStrategyThreshold: (strategyId, patch) =>
        set(state => {
            const st = state.strategies[strategyId]
            if (!st.threshold) return state

            return {
                strategies: {
                    ...state.strategies,
                    [strategyId]: {
                        ...st,
                        threshold: {
                            ...st.threshold,
                            ...patch
                        }
                    }
                }
            }
        }),

    setStrategyTopN: (strategyId, patch) =>
        set(state => {
            const st = state.strategies[strategyId]
            if (!st.top_n) return state

            return {
                strategies: {
                    ...state.strategies,
                    [strategyId]: {
                        ...st,
                        top_n: {
                            ...st.top_n,
                            ...patch
                        }
                    }
                }
            }
        }),

    // =========================
    // Factor
    // =========================
    addFactor: (strategyId, afterId) =>
        set(state => {
            const st = state.strategies[strategyId]
            const id = `factor_${uuidv4()}`

            const factorIds = [...st.factorIds]

            if (!afterId) {
                factorIds.push(id)
            } else {
                const idx = factorIds.indexOf(afterId)
                factorIds.splice(idx + 1, 0, id)
            }

            return {
                strategies: {
                    ...state.strategies,
                    [strategyId]: {
                        ...st,
                        factors: {
                            ...st.factors,
                            [id]: { id, added: true, expr: "" }
                        },
                        factorIds
                    }
                }
            }
        }),

    updateFactor: (strategyId, factorId, expr) =>
        set(state => ({
            strategies: {
                ...state.strategies,
                [strategyId]: {
                    ...state.strategies[strategyId],
                    factors: {
                        ...state.strategies[strategyId].factors,
                        [factorId]: {
                            ...state.strategies[strategyId].factors[factorId],
                            expr
                        }
                    }
                }
            }
        })),

    deleteFactor: (strategyId, factorId) =>
        set(state => {
            const st = state.strategies[strategyId]
            const nextFactors = { ...st.factors }
            delete nextFactors[factorId]

            return {
                strategies: {
                    ...state.strategies,
                    [strategyId]: {
                        ...st,
                        factors: nextFactors,
                        factorIds: st.factorIds.filter(id => id !== factorId)
                    }
                }
            }
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

        const strategies = s.strategyIds.map(id => {
            const st = s.strategies[id]

            return {
                name: st.name,

                factors: st.factorIds
                    .map(fid => st.factors[fid].expr)
                    .filter(f => f && f.trim()),

                signal:
                    st.signal.enabled && st.signal.value
                        ? st.signal.value
                        : null,

                threshold:
                    mode === "SIGNAL_STRATEGY" && st.threshold?.enabled
                        ? st.threshold.value
                        : null,

                top_n:
                    mode === "WEIGHT_STRATEGY" && st.top_n?.enabled
                        ? st.top_n.value
                        : null
            }
        })

        return {
            name: s.name,
            mode,
            strategies,

            schedule_signal:
                s.schedule_signal.enabled && s.schedule_signal.value
                    ? s.schedule_signal.value
                    : null,

            params: s.params
        }
    }

}))