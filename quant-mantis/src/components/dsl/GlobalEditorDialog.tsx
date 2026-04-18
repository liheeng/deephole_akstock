import VisualEditorDialog from "../visual/VisualEditorDialog"
import { useBacktestStore } from "../../store/backtest.store"

export default function GlobalDialogs({ nodes }: any) {

    const {
        dialog,
        closeDialog,

        updateFactor,
        updateStrategy,

        strategies
    } = useBacktestStore()

    return (
        <VisualEditorDialog
            open={dialog.open}
            nodes={nodes}
            onClose={closeDialog}
            onConfirm={(expr: string) => {

                // =========================
                // Factor
                // =========================
                if (dialog.type === "factor") {
                    if (
                        dialog.strategyIndex === undefined ||
                        dialog.factorIndex === undefined
                    ) return

                    updateFactor(
                        dialog.strategyIndex,
                        dialog.factorIndex,
                        expr
                    )
                }

                // =========================
                // Strategy Signal
                // =========================
                if (dialog.type === "signal") {
                    if (dialog.strategyIndex === undefined) return

                    const s = strategies[dialog.strategyIndex]
                    if (!s) return

                    updateStrategy(dialog.strategyIndex, {
                        signal: {
                            ...s.signal,
                            enabled: true,
                            value: expr
                        }
                    })
                }

                // =========================
                // Schedule Signal（Portfolio）
                // =========================
                if (dialog.type === "schedule_signal") {
                    useBacktestStore.setState(state => ({
                        schedule_signal: {
                            ...state.schedule_signal,
                            enabled: true,
                            value: expr
                        }
                    }))
                }

                closeDialog()
            }}
        />
    )
}