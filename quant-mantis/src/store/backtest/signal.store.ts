import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"

export interface Signal {
    id: string
    expr: string
}

interface SignalState {
    signals: Record<string, Signal>

    createSignal: (expr: string) => string
    updateSignal: (id: string, expr: string) => void
    deleteSignal: (id: string) => void
}

export const useSignalStore = create<SignalState>((set) => ({

    signals: {},

    createSignal: (expr: string) => {
        const id = `signal_${uuidv4()}`
        set(state => ({
            signals: {
                ...state.signals,
                [id]: { id, expr: expr || "" }
            }
        }))
        return id
    },

    updateSignal: (id, expr) =>
        set(state => ({
            signals: {
                ...state.signals,
                [id]: {
                    ...state.signals[id],
                    expr
                }
            }
        })),

    deleteSignal: (id) =>
        set(state => {
            const next = { ...state.signals }
            delete next[id]
            return { signals: next }
        })

}))