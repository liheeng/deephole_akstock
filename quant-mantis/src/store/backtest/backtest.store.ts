import { create } from "zustand"
import { useStrategyStore } from "./strategy.store"
import { useFactorStore } from "./factor.store"
import { useSignalStore } from "./signal.store"
import { SimpleCheckResult, type CheckResult } from "../../common/Types"

type PortfolioMode = "signal_strategy" | "weight_strategy"

interface EnabledField<T> {
    enabled: boolean
    value: T
}

interface BacktestState {

    name: string
    portfolio_mode: PortfolioMode

    params: {
        freq: string
        init_cash: number
    }

    // ✅ schedule 使用 signalId
    schedule_signal: {
        enabled: boolean
        signalId?: string
    }

    // datasetId?: string

    // setDatasetId: (id: string) => void

    setPortfolioMode: (mode: PortfolioMode) => void

    setScheduleSignal: (patch: Partial<{ enabled: boolean; signalId?: string }>) => void

    // =========================
    // Portfolio Config（你漏掉的部分）
    // =========================

    strategy_op: EnabledField<"AND" | "OR">
    vote_weights: EnabledField<number[]>
    strategy_weights: EnabledField<number[]>

    setStrategyOp: (patch: Partial<EnabledField<"AND" | "OR">>) => void
    setVoteWeights: (patch: Partial<EnabledField<number[]>>) => void
    setStrategyWeights: (patch: Partial<EnabledField<number[]>>) => void

    updatePortfolioParams: (patch: Partial<{ freq: string; init_cash: number }>) => void
    validate: () => CheckResult
    buildPayload: () => any
}

export const useBacktestStore = create<BacktestState>((set, get) => ({

    name: "MyPortfolio",

    portfolio_mode: "signal_strategy",

    params: {
        freq: "1D",
        init_cash: 100000
    },

    schedule_signal: {
        enabled: false,
        signalId: undefined
    },

    setPortfolioMode: (mode: PortfolioMode) => set({ portfolio_mode: mode }),

    setScheduleSignal: (patch) =>
        set(state => ({
            schedule_signal: { ...state.schedule_signal, ...patch }
        })),

    // =========================
    // portfolio config（补齐）
    // =========================
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

    setStrategyOp: (patch) =>
        set(state => ({
            strategy_op: {
                ...state.strategy_op,
                ...patch
            }
        })),

    setVoteWeights: (patch) =>
        set(state => ({
            vote_weights: {
                ...state.vote_weights,
                ...patch
            }
        })),

    setStrategyWeights: (patch) =>
        set(state => ({
            strategy_weights: {
                ...state.strategy_weights,
                ...patch
            }
        })),


    updatePortfolioParams: (patch) =>
        set(state => ({
            params: {
                ...state.params,
                ...patch
            }
        })),

    // ==============================================
    // ✅ 标准校验：返回 CheckResult
    // ==============================================
    validate: (): CheckResult => {
        const currentState = get()
        const { strategyIds, strategies } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()
        const errors: string[] = []

        // 1. 策略列表校验：至少有一个策略
        if (strategyIds.length === 0) {
            errors.push("请至少添加一个策略")
        }

        // 2. 逐个校验策略
        strategyIds.forEach((strategyId, idx) => {
            const strategy = strategies[strategyId]
            if (!strategy) {
                errors.push(`第 ${idx + 1} 个策略不存在（ID: ${strategyId}）`)
                return // 跳过不存在的策略的后续校验
            }

            // 2.1 策略名称（非空）
            if (!strategy.name?.trim()) {
                errors.push(`第 ${idx + 1} 个策略名称不能为空`)
            }

            // 2.2 核心规则：策略必须至少有一个 Factor（强制校验，无例外）
            if (!strategy.factorIds || strategy.factorIds.length === 0) {
                errors.push(`策略【${strategy.name || `ID:${strategyId}`}】必须至少选择一个因子`)
            } else {
                // 2.3 校验策略下的 Factor 有效性（存在且 expr 非空）
                strategy.factorIds.forEach((factorId) => {
                    const factor = factors[factorId]
                    if (!factor) {
                        errors.push(`策略【${strategy.name || `ID:${strategyId}`}】包含不存在的因子（ID: ${factorId}）`)
                    } else if (!factor.expr?.trim()) {
                        errors.push(`策略【${strategy.name || `ID:${strategyId}`}】中的因子（ID: ${factorId}）表达式不能为空`)
                    }
                })
            }

            // 2.4 Signal 校验：仅当 portfolio 模式为 signal_strategy 时，才校验策略的 signal
            if (currentState.portfolio_mode === "signal_strategy") {
                // signalId 是可选字段
                if (strategy.signalId) {
                    const signal = signals[strategy.signalId]
                    if (!signal || !signal.expr?.trim()) {
                        errors.push(`策略【${strategy.name || `ID:${strategyId}`}】的信号（ID: ${strategy.signalId}）无效或表达式为空`)
                    }
                }
            }

            // 2.5 策略配置校验（可选字段，仅校验存在的配置）
            if (strategy.config) {
                if (strategy.config.mode === "ts" && (strategy.config.threshold === undefined || strategy.config.threshold < 0)) {
                    errors.push(`策略【${strategy.name || `ID:${strategyId}`}】的 threshold 必须大于等于 0`)
                }
                if (strategy.config.mode === "cs" && (strategy.config.top_n === undefined || strategy.config.top_n <= 0)) {
                    errors.push(`策略【${strategy.name || `ID:${strategyId}`}】的 top_n 必须大于 0`)
                }
            }
        })

        // 3. 调度信号校验：仅当 enabled 为 true 时，才校验 signalId 有效性
        if (currentState.schedule_signal.enabled) {
            const scheduleSignalId = currentState.schedule_signal.signalId
            if (!scheduleSignalId) {
                errors.push("调度信号已启用，但未选择信号（signalId 为空）")
            } else {
                const scheduleSignal = signals[scheduleSignalId]
                if (!scheduleSignal || !scheduleSignal.expr?.trim()) {
                    errors.push(`调度信号（ID: ${scheduleSignalId}）无效或表达式为空`)
                }
            }
        }

        // 4. 回测参数校验
        if (currentState.params.init_cash <= 0) {
            errors.push("初始资金必须大于 0")
        }
        if (!currentState.params.freq?.trim()) {
            errors.push("调仓频率不能为空")
        }

        // 5. Portfolio 配置校验（仅校验启用的字段）
        const { strategy_op, vote_weights, strategy_weights } = currentState
        // 5.1 strategy_op 启用时，值必须是 AND/OR
        if (strategy_op.enabled && !["AND", "OR"].includes(strategy_op.value)) {
            errors.push("策略运算符（strategy_op）值必须是 AND 或 OR")
        }
        // 5.2 vote_weights 启用时，数组不能为空且元素需为正数
        if (vote_weights.enabled) {
            if (vote_weights.value.length === 0) {
                errors.push("投票权重（vote_weights）已启用，但权重数组为空")
            } else if (vote_weights.value.some(w => w <= 0)) {
                errors.push("投票权重（vote_weights）中的值必须大于 0")
            }
        }
        // 5.3 strategy_weights 启用时，数组不能为空且元素需为正数
        if (strategy_weights.enabled) {
            if (strategy_weights.value.length === 0) {
                errors.push("策略权重（strategy_weights）已启用，但权重数组为空")
            } else if (strategy_weights.value.some(w => w <= 0)) {
                errors.push("策略权重（strategy_weights）中的值必须大于 0")
            }
        }

        // 返回校验结果（SimpleCheckResult 需支持接收错误数组，通常包含 isValid 和 errors 属性）
        return new SimpleCheckResult(...errors)
    },

    // =========================
    // Payload
    // =========================
    buildPayload: () => {

        const s = get()

        const { strategies, strategyIds } = useStrategyStore.getState()
        const { factors } = useFactorStore.getState()
        const { signals } = useSignalStore.getState()

        const mode =
            s.portfolio_mode === "signal_strategy"
                ? "SIGNAL_STRATEGY"
                : "WEIGHT_STRATEGY"

        return {
            name: s.name,
            mode,

            strategies: strategyIds.map(id => {
                const st = strategies[id]

                return {
                    name: st.name,

                    factors: st.factorIds
                        .map(fid => factors[fid]?.expr)
                        .filter(Boolean),

                    signal: st.signalId
                        ? signals[st.signalId]?.expr
                        : null,

                    threshold:
                        mode === "SIGNAL_STRATEGY"
                            ? st.config.threshold ?? null
                            : null,

                    top_n:
                        mode === "WEIGHT_STRATEGY"
                            ? st.config.top_n ?? null
                            : null
                }
            }),

            schedule_signal:
                s.schedule_signal.enabled && s.schedule_signal.signalId
                    ? signals[s.schedule_signal.signalId]?.expr
                    : null,

            // =========================
            // portfolio config（终于补回来了）
            // =========================
            strategy_op: s.strategy_op.enabled ? s.strategy_op.value : null,

            vote_weights:
                s.vote_weights.enabled ? s.vote_weights.value : null,

            strategy_weights:
                s.strategy_weights.enabled ? s.strategy_weights.value : null,

            params: s.params
        }
    }

}))