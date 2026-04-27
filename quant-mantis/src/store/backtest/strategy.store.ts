import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"
import { useFactorStore } from "./factor.store"

type StrategyMode = "ts" | "cs"

export interface Strategy {
    id: string
    name: string

    factorIds: string[]
    factor_ids?: string
    signalId?: string
    signal_id?: string

    config: {
        mode: StrategyMode
        threshold?: number
        top_n?: number
    }

    collapsed?: boolean
}

interface StrategyState {
    strategies: Record<string, Strategy>
    strategyIds: string[]

    // initialized: boolean

    init: (strategies: Strategy[]) => void

    createStrategy: (mode: StrategyMode) => string
    deleteStrategy: (id: string) => void

    updateStrategyMeta: (id: string, patch: Partial<Strategy>) => void

    updateStrategyMode: (id: string, mode: StrategyMode) => void

    setStrategySignal: (id: string, signalId: string) => void

    addFactorToStrategy: (id: string, factorId: string) => void
    removeFactorFromStrategy: (id: string, factorId: string) => void

    updateStrategyConfig: (id: string, patch: Partial<Strategy["config"]>) => void
}

export const useStrategyStore = create<StrategyState>((set, get) => ({

    strategies: {},
    strategyIds: [],

    // initialized: false,

    // init: (strategies: Strategy[] = []) => {

    //     if (!Array.isArray(strategies)) {
    //         console.warn("init strategies is not array", strategies)
    //         return
    //     }

    //     const valid = strategies.filter(s => {
    //         if (!s?.id) {
    //             console.warn("invalid strategy", s)
    //             return false
    //         }
    //         return true
    //     })

    //     set(() => ({
    //         strategies: Object.fromEntries(
    //             valid.map(s => [s.id, { ...s }])
    //         ),
    //         // strategies: Object.fromEntries(
    //         //     valid.map(s => [s.id, JSON.parse(JSON.stringify(s))])
    //         // ),
    //         strategyIds: valid.map(s => s.id)
    //     }))
    // },

    init: (strategies: Strategy[] = []) => {

        // const state = get()

        // if (state.initialized) return

        if (!Array.isArray(strategies)) {
            console.warn("init strategies is not array", strategies)
            return
        }

        const valid = strategies.filter(s => s?.id)
        

        set(() => ({
            strategies: Object.fromEntries(
                valid.map(s => [s.id, { ...s, factorIds: JSON.parse(s.factor_ids || "[]") || [] , signalId: s.signal_id }])
            ),
            strategyIds: valid.map(s => s.id),
            // initialized: true
        }))
    },
    createStrategy: (mode: StrategyMode = "ts") => {
        const { createFactor } = useFactorStore.getState()
        const factorId = createFactor()
        const id = `strategy_${uuidv4()}`
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    id,
                    name: `strategy_${id}`,
                    factorIds: [factorId],
                    config: {
                        mode: mode,
                        threshold: 0.5
                    }
                }
            },
            strategyIds: [...state.strategyIds, id]
        }))

        return id
    },

    deleteStrategy: (id) =>
        set(state => {
            const next = { ...state.strategies }
            delete next[id]

            return {
                strategies: next,
                strategyIds: state.strategyIds.filter(x => x !== id)
            }
        }),

    updateStrategyMeta: (id, patch) =>
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    ...state.strategies[id],
                    ...patch
                }
            }
        })),

    updateStrategyMode: (id: string, mode: StrategyMode) => {
        const currentState = get()
        if (!id || id === "") {
            if (currentState.strategies) {
                for (const key in currentState.strategies) {
                    set(state => ({
                        strategies: {
                            ...state.strategies,
                            [key]: {
                                ...state.strategies[key],
                                config: {
                                    ...state.strategies[key].config,
                                    mode
                                }
                            }
                        }
                    }))
                }
            }
        } else {
            set(state => ({
                strategies: {
                    ...state.strategies,
                    [id]: {
                        ...state.strategies[id],
                        config: {
                            ...state.strategies[id].config,
                            mode
                        }
                    }
                }
            }))
        }
    },

    setStrategySignal: (id, signalId) =>
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    ...state.strategies[id],
                    signalId
                }
            }
        })),

    addFactorToStrategy: (id, factorId) =>
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    ...state.strategies[id],
                    factorIds: [...state.strategies[id].factorIds, factorId]
                }
            }
        })),

    removeFactorFromStrategy: (id, factorId) =>
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    ...state.strategies[id],
                    factorIds: state.strategies[id].factorIds.filter(f => f !== factorId)
                }
            }
        })),

    updateStrategyConfig: (id, patch) =>
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    ...state.strategies[id],
                    config: {
                        ...state.strategies[id].config,
                        ...patch
                    }
                }
            }
        }))

}))