import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"
import { useFactorStore } from "./factor.store"

type StrategyMode = "ts" | "cs"

export interface Strategy {
    id: string
    name: string

    factorIds: string[]
    signalId?: string

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



    createStrategy: (mode: StrategyMode = "ts") => {
        const { createFactor } = useFactorStore.getState()
        const factorId = createFactor()
        const id = `strategy_${uuidv4()}`
        set(state => ({
            strategies: {
                ...state.strategies,
                [id]: {
                    id,
                    name: `strategy_${state.strategyIds.length + 1}`,
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