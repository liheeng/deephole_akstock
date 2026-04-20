import VisualEditorDialog from "../visual/VisualEditorDialog"

import { useDialogStore } from "../../store/dialog.store"

import { useFactorStore } from "../../store/backtest/factor.store"
import { useSignalStore } from "../../store/backtest/signal.store"
import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useBacktestStore } from "../../store/backtest/backtest.store"

export default function GlobalDialogs({ nodes }: any) {

    const dialog = useDialogStore(state => state.dialog)
    const closeDialog = useDialogStore(state => state.closeDialog)

    const updateFactor = useFactorStore(state => state.updateFactor)

    const createSignal = useSignalStore(state => state.createSignal)
    const updateSignal = useSignalStore(state => state.updateSignal)

    const setStrategySignal = useStrategyStore(state => state.setStrategySignal)

    const setScheduleSignal = useBacktestStore(state => state.setScheduleSignal)

    return (
        <VisualEditorDialog
            open={dialog.open}
            nodes={nodes}
            onClose={closeDialog}

            onConfirm={(expr: string) => {

                const payload = dialog.payload || {}

                // =========================
                // Factor
                // =========================
                if (dialog.type === "factor") {

                    const { factorId } = payload
                    if (!factorId) return

                    updateFactor(factorId, expr)
                }

                // =========================
                // Strategy Signal
                // =========================
                if (dialog.type === "signal") {

                    const { strategyId, signalId } = payload
                    if (!strategyId) return

                    let sid = signalId

                    // 没有 signal 就创建一个
                    if (!sid) {
                        sid = createSignal("")
                        setStrategySignal(strategyId, sid)
                    }

                    updateSignal(sid, expr)
                }

                // =========================
                // Schedule Signal（Portfolio）
                // =========================
                if (dialog.type === "schedule_signal") {

                    const { signalId } = payload

                    let sid = signalId

                    if (!sid) {
                        sid = createSignal("")
                    }

                    updateSignal(sid, expr)

                    setScheduleSignal({
                        enabled: true,
                        signalId: sid
                    })
                }

                closeDialog()
            }}
        />
    )
}