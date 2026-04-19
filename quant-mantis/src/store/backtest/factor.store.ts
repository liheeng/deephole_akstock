import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"

export interface Factor {
    id: string
    expr: string
}

interface FactorState {
    factors: Record<string, Factor>

    createFactor: () => string
    updateFactor: (id: string, expr: string) => void
    deleteFactor: (id: string) => void
}

export const useFactorStore = create<FactorState>((set) => ({

    factors: {},

    createFactor: () => {
        const id = `factor_${uuidv4()}`
        set(state => ({
            factors: {
                ...state.factors,
                [id]: { id, expr: "" }
            }
        }))
        return id
    },

    updateFactor: (id, expr) =>
        set(state => ({
            factors: {
                ...state.factors,
                [id]: {
                    ...state.factors[id],
                    expr
                }
            }
        })),

    deleteFactor: (id) =>
        set(state => {
            const next = { ...state.factors }
            delete next[id]
            return { factors: next }
        })

}))