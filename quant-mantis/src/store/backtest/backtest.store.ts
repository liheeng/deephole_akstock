import { create } from "zustand"
import { useStrategyStore } from "./strategy.store"
import { useFactorStore } from "./factor.store"
import { useSignalStore } from "./signal.store"

type PortfolioMode = "signal_strategy" | "weight_strategy"

interface EnabledField<T> {
    enabled: boolean
    value: T
}

interface BacktestState {

    name: string
    portfolio_mode: PortfolioMode

    params: {
        freq: string
        init_cash: number
    }

    // ✅ schedule 使用 signalId
    schedule_signal: {
        enabled: boolean
        signalId?: string
    }

    setPortfolioMode: (mode: PortfolioMode) => void

    setScheduleSignal: (patch: Partial<{ enabled: boolean; signalId?: string }>) => void

    // =========================
    // Portfolio Config（你漏掉的部分）
    // =========================

    strategy_op: EnabledField<"AND" | "OR">
    vote_weights: EnabledField<number[]>
    strategy_weights: EnabledField<number[]>

    setStrategyOp: (patch: Partial<EnabledField<"AND" | "OR">>) => void
    setVoteWeights: (patch: Partial<EnabledField<number[]>>) => void
    setStrategyWeights: (patch: Partial<EnabledField<number[]>>) => void

    updatePortfolioParams: (patch: Partial<{ freq: string; init_cash: number }>) => void

    buildPayload: () => any
}

export const useBacktestStore = create<BacktestState>((set, get) => ({

    name: "MyPortfolio",

    portfolio_mode: "signal_strategy",

    params: {
        freq: "1D",
        init_cash: 100000
    },

    schedule_signal: {
        enabled: false,
        signalId: undefined
    },

    setPortfolioMode: (mode: PortfolioMode) => set({ portfolio_mode: mode }),

    setScheduleSignal: (patch) =>
        set(state => ({
            schedule_signal: { ...state.schedule_signal, ...patch }
        })),

        // =========================
    // portfolio config（补齐）
    // =========================
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

    setStrategyOp: (patch) =>
        set(state => ({
            strategy_op: {
                ...state.strategy_op,
                ...patch
            }
        })),

    setVoteWeights: (patch) =>
        set(state => ({
            vote_weights: {
                ...state.vote_weights,
                ...patch
            }
        })),

    setStrategyWeights: (patch) =>
        set(state => ({
            strategy_weights: {
                ...state.strategy_weights,
                ...patch
            }
        })),


    updatePortfolioParams: (patch) =>
        set(state => ({
            params: {
                ...state.params,
                ...patch
            }   
    })),

    // =========================
    // Payload
    // =========================
    buildPayload: () => {

        const s = get()

        const { strategies, strategyIds } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()

        const mode =
            s.portfolio_mode === "signal_strategy"
                ? "SIGNAL_STRATEGY"
                : "WEIGHT_STRATEGY"

        return {
            name: s.name,
            mode,

            strategies: strategyIds.map(id => {
                const st = strategies[id]

                return {
                    name: st.name,

                    factors: st.factorIds
                        .map(fid => factors[fid]?.expr)
                        .filter(Boolean),

                    signal: st.signalId
                        ? signals[st.signalId]?.expr
                        : null,

                    threshold:
                        mode === "SIGNAL_STRATEGY"
                            ? st.config.threshold ?? null
                            : null,

                    top_n:
                        mode === "WEIGHT_STRATEGY"
                            ? st.config.top_n ?? null
                            : null
                }
            }),

            schedule_signal:
                s.schedule_signal.enabled && s.schedule_signal.signalId
                    ? signals[s.schedule_signal.signalId]?.expr
                    : null,

            // =========================
            // portfolio config（终于补回来了）
            // =========================
            strategy_op: s.strategy_op.enabled ? s.strategy_op.value : null,

            vote_weights:
                s.vote_weights.enabled ? s.vote_weights.value : null,

            strategy_weights:
                s.strategy_weights.enabled ? s.strategy_weights.value : null,

            params: s.params
        }
    }

}))