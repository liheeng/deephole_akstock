import { create } from "zustand"
import { useStrategyStore } from "./strategy.store"
import { useFactorStore } from "./factor.store"
import { useSignalStore } from "./signal.store"
import { SimpleCheckResult, type CheckResult } from "../../common/Types"
import { nanoid } from "nanoid"
import { type BacktestConfig } from "../../api/Client"

type PortfolioMode = "signal_strategy" | "weight_strategy"

interface EnabledField<T> {
    enabled: boolean
    value: T
}

interface BacktestSnapshot {
    backtest: any
    strategies: any
    strategyIds: string[]
    factors: any
    signals: any
}

export interface BacktestState {
    id: string
    name: string
    portfolio_mode: PortfolioMode

    params: {
        freq: string
        init_cash: number
    }

    schedule_signal: {
        enabled: boolean
        signalId?: string
    }

    strategy_op: EnabledField<"AND" | "OR">
    vote_weights: EnabledField<number[]>
    strategy_weights: EnabledField<number[]>

    // =========================
    // ✅ snapshot
    // =========================
    originalSnapshot?: BacktestSnapshot

    applyBacktestConfig: (config: BacktestConfig | undefined) => void
    // =========================
    // setters
    // =========================
    setPortfolioName: (name: string) => void
    setPortfolioMode: (mode: PortfolioMode) => void
    setScheduleSignal: (patch: Partial<{ enabled: boolean; signalId?: string }>) => void

    setStrategyOp: (patch: Partial<EnabledField<"AND" | "OR">>) => void
    setVoteWeights: (patch: Partial<EnabledField<number[]>>) => void
    setStrategyWeights: (patch: Partial<EnabledField<number[]>>) => void

    updatePortfolioParams: (patch: Partial<{ freq: string; init_cash: number }>) => void

    // =========================
    // snapshot & dirty
    // =========================
    setOriginalSnapshot: () => void
    isDirty: () => boolean

    // =========================
    // logic
    // =========================
    validate: () => CheckResult
    buildPayload: () => any
}

export const useBacktestStore = create<BacktestState>((set, get) => ({
    // id: "_id_backtest_" + nanoid(),
    // name: "_name-backtest-" + nanoid(),
    id: "",
    name: "",

    portfolio_mode: "signal_strategy",

    params: {
        freq: "1D",
        init_cash: 100000
    },

    schedule_signal: {
        enabled: false,
        signalId: undefined
    },

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

    applyBacktestConfig(_config: BacktestConfig | undefined) {
        let config: BacktestConfig | undefined = _config
        if (!config && get().id !== "") {
            // Do not apply if id is not null and config is null
            return
        }

        if (!config) {
            // Create default config
            get().setOriginalSnapshot()
            const strategyId = "strategy-" + nanoid()
            const factorId= "factor-" + nanoid()
            config = {
                id: "bt_" + nanoid(),
                name: "MyPortfolio-" + nanoid(),
                portfolio_mode: "signal_strategy",
                params: {
                    freq: "1D",
                    init_cash: 100000
                },
                schedule_signal: {
                    enabled: false,
                    signalId: undefined
                },
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
                strategies: {
                    [strategyId]: {
                        id: strategyId,
                        name: "Strategy-1",
                        config: {
                            mode: "ts",
                            threshold: 0.5
                        },
                        factorIds: [factorId],
                        signalId: undefined, 
                    }
                },
                factors: {
                    [factorId]: {
                        id: factorId,
                        name: "Factor-1",
                        expr: "(MA(5) - MA(20)) / MA(20)"
                    }
                },
                signals: {}
            }
        }

        // 初始化子 store（注意格式转换）
        useSignalStore.getState().init(Object.values(config.signals))
        useFactorStore.getState().init(Object.values(config.factors))
        useStrategyStore.getState().init(Object.values(config.strategies))

        // 2. 写入 backtest
        set(state => ({
            ...state,
            id: config.id,
            name: config.name,
            portfolio_mode: config.portfolio_mode as PortfolioMode,
            params: config.params as any,
            schedule_signal: config.schedule_signal as any,
            strategy_op: config.strategy_op as any,
            vote_weights: config.vote_weights as any,
            strategy_weights: config.strategy_weights as any
        }))
    },

    // =========================
    // setters
    // =========================
    setPortfolioName: (name) => set({ name }),

    setPortfolioMode: (mode) => set({ portfolio_mode: mode }),

    setScheduleSignal: (patch) =>
        set(state => ({
            schedule_signal: { ...state.schedule_signal, ...patch }
        })),

    setStrategyOp: (patch) =>
        set(state => ({
            strategy_op: { ...state.strategy_op, ...patch }
        })),

    setVoteWeights: (patch) =>
        set(state => ({
            vote_weights: { ...state.vote_weights, ...patch }
        })),

    setStrategyWeights: (patch) =>
        set(state => ({
            strategy_weights: { ...state.strategy_weights, ...patch }
        })),

    updatePortfolioParams: (patch) =>
        set(state => ({
            params: { ...state.params, ...patch }
        })),

    // =========================
    // ✅ snapshot
    // =========================
    setOriginalSnapshot: () => {
        const s = get()

        const { strategies, strategyIds } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()

        const snapshot: BacktestSnapshot = {
            backtest: {
                id: s.id,
                name: s.name,
                portfolio_mode: s.portfolio_mode,
                params: structuredClone(s.params),
                schedule_signal: structuredClone(s.schedule_signal),
                strategy_op: structuredClone(s.strategy_op),
                vote_weights: structuredClone(s.vote_weights),
                strategy_weights: structuredClone(s.strategy_weights)
            },
            strategies: structuredClone(strategies),
            strategyIds: [...strategyIds],
            factors: structuredClone(factors),
            signals: structuredClone(signals)
        }

        set({ originalSnapshot: snapshot })
    },

    isDirty: () => {
        const s = get()
        if (!s.originalSnapshot) return false

        const { strategies, strategyIds } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()

        const current = {
            backtest: {
                id: s.id,
                name: s.name,
                portfolio_mode: s.portfolio_mode,
                params: s.params,
                schedule_signal: s.schedule_signal,
                strategy_op: s.strategy_op,
                vote_weights: s.vote_weights,
                strategy_weights: s.strategy_weights
            },
            strategies,
            strategyIds,
            factors,
            signals
        }

        return JSON.stringify(current) !== JSON.stringify(s.originalSnapshot)
    },

    // =========================
    // validate（原样保留）
    // =========================
    validate: (): CheckResult => {
        const currentState = get()
        const { strategyIds, strategies } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()

        const errors: string[] = []

        if (strategyIds.length === 0) {
            errors.push("请至少添加一个策略")
        }

        strategyIds.forEach((strategyId, idx) => {
            const strategy = strategies[strategyId]
            if (!strategy) {
                errors.push(`第 ${idx + 1} 个策略不存在`)
                return
            }

            if (!strategy.name?.trim()) {
                errors.push(`策略名称不能为空`)
            }

            if (!strategy.factorIds || strategy.factorIds.length === 0) {
                errors.push(`策略【${strategy.name}】必须至少选择一个因子`)
            }

            if (currentState.portfolio_mode === "signal_strategy") {
                if (strategy.signalId) {
                    const signal = signals[strategy.signalId]
                    if (!signal || !signal.expr?.trim()) {
                        errors.push(`策略信号无效`)
                    }
                }
            }
        })

        return new SimpleCheckResult(...errors)
    },

    // =========================
    // payload
    // =========================
    buildPayload: () => {
        const s = get()
        const { strategies, strategyIds } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()

        return {
            id: s.id,
            name: s.name,
            portfolio_mode: s.portfolio_mode,
            params: s.params,
            schedule_signal: s.schedule_signal,
            strategy_op: s.strategy_op,
            vote_weights: s.vote_weights,
            strategy_weights: s.strategy_weights,
            strategies,
            factors,
            signals
        }
    }
}))