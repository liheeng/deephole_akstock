import { create } from "zustand"

interface BacktestResultState {

    equity: any[]          // 时间序列
    stats: Record<string, any>
    trades: any[]

    loading: boolean
    error: string | null

    // ===== actions =====
    setBacktestResult: (result: any) => void
    setLoading: (loading: boolean) => void
    setError: (error: string | null) => void

    reset: () => void
}

// =========================
// Store
// =========================
export const useBacktestResultStore = create<BacktestResultState>((set) => ({

    equity: [],
    stats: {},
    trades: [],

    loading: false,
    error: null,

    setEquity: (equity: any[]) => set({ equity }),
    setStats: (stats: Record<string, any>) => set({ stats }),
    setTrades: (trades: any[]) => set({ trades }),


    setBacktestResult: (r) => set({
        equity: r.equity,
        stats: r.stats,
        trades: r.trades
    }),

    setLoading: (loading) =>
        set({ loading }),

    setError: (error) =>
        set({
            error,
            loading: false
        }),

    reset: () =>
        set({
            equity: [],
            stats: {},
            trades: [],
            loading: false,
            error: null
        })

}))