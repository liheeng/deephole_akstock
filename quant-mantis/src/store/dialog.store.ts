import { create } from "zustand"
import type { BacktestDataSourceDef } from "./dataset.store"

// =========================
// Types
// =========================
export type DialogType =
    | "factor"
    | "signal"
    | "schedule_signal"
    | "strategy_graph"   // 🚀 预留
    | "node_editor"      // 🚀 预留
    | 'backtest_data'
    | 'backtest_wizard'

interface DialogState {
    open: boolean
    type: DialogType | null
    payload?: {
        dataSource?: BacktestDataSourceDef
    } | any
    strategyId?: string
    factorId?: string
}

interface DialogStore {

    dialog: DialogState

    openDialog: (type: DialogType, payload?: any) => void
    closeDialog: () => void

    // 可选：直接更新 payload（高级用法）
    setDialogPayload: (payload: any) => void
}

// =========================
// Store
// =========================
export const useDialogStore = create<DialogStore>((set) => ({

    dialog: {
        open: false,
        type: null,
        payload: undefined
    },

    openDialog: (type, payload) =>
        set({
            dialog: {
                open: true,
                type,
                payload
            }
        }),

    closeDialog: () =>
        set({
            dialog: {
                open: false,
                type: null,
                payload: undefined
            }
        }),

    setDialogPayload: (payload) =>
        set(state => ({
            dialog: {
                ...state.dialog,
                payload
            }
        }))

}))