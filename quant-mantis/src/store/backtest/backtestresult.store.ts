import { create } from "zustand"

interface StatsData {
    average: Record<string, any>
    details: Record<string, Record<string, any>>
}

interface EquityData {
    times: string[]
    average: number[]
    details: Record<string, number[]>
    best_sharpe?: number[]
    best_return?: number[]
    meta?: {
        best_sharpe_column?: string
        best_return_column?: string
        freq?: string
        count?: string
    }
}

export interface BacktestResultState {

    equity: EquityData | null
    stats: StatsData | null   // 🔥 改这里
    trades: any[]

    loading: boolean
    error: string | null

    selectedSymbol: string | null   // ⭐ 新增

    activeTradeId: string | null
    
    setActiveTradeId: (id: string | null) => void

    setSelectedSymbol: (s: string | null) => void

    setBacktestResult: (result: any) => void
    setLoading: (loading: boolean) => void
    setError: (error: string | null) => void

    reset: () => void
}

// =========================
// Store
// =========================
export const useBacktestResultStore = create<BacktestResultState>((set) => ({

    equity: null,
    stats: {
        average: {},
        details: {}
    },
    trades: [],

    loading: false,
    error: null,

    selectedSymbol: null,

    activeTradeId: null as string | null,
    
    setActiveTradeId: (id: string | null) => set({ activeTradeId: id }),

    setSelectedSymbol: (s) => set({ selectedSymbol: s }),

    setEquity: (equity: EquityData | null) => set({ equity }),
    setStats: (stats: StatsData) => set({ stats }),
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
            equity: null,
            stats: {
                average: {},
                details: {}
            },
            trades: [],
            loading: false,
            error: null
        })

}))