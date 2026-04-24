import { create } from "zustand"
import { v4 as uuidv4 } from "uuid"

export interface Signal {
    id: string
    name: string
    expr: string
}

interface SignalState {
    signals: Record<string, Signal>

    init: (signals: Signal[]) => void
    createSignal: (expr: string) => string
    updateSignal: (id: string, expr: string) => void
    deleteSignal: (id: string) => void
}

export const useSignalStore = create<SignalState>((set) => ({

    signals: {},

    init: (signals: Signal[] = []) => {
        const valid = signals
            .filter(s => s && s.id)
            .map(s => ({
                ...s,
                name: s.name ?? s.id
            }))

        set(() => ({
            signals: Object.fromEntries(
                valid.map(s => [s.id, s])
            )
        }))
    },

    createSignal: (expr: string) => {
        const id = `signal_${uuidv4()}`
        set(state => ({
            signals: {
                ...state.signals,
                [id]: { id, name: id, expr: expr || "" }
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