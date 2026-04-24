import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"

export interface Factor {
    id: string
    name: string
    expr: string
}

interface FactorState {
    factors: Record<string, Factor>

    init: (factors: Factor[]) => void
    createFactor: () => string
    updateFactor: (id: string, expr: string) => void
    deleteFactor: (id: string) => void
}

export const useFactorStore = create<FactorState>((set) => ({

    factors: {},
    init(factors: Factor[] = []) {
        const valid = factors
            .filter(f => f && f.id) // ✅ 过滤非法
            .map(f => ({
                ...f,                // ✅ 断开引用
                name: f.name ?? f.id // ✅ 不修改原对象
            }))

        set(() => ({
            factors: Object.fromEntries(
                valid.map(f => [f.id, f])
            )
        }))
    },

    createFactor: () => {
        const id = `factor_${uuidv4()}`
        set(state => ({
            factors: {
                ...state.factors,
                [id]: { id, name: id, expr: "" }
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