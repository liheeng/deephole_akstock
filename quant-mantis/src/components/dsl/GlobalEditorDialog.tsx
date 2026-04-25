import VisualEditorDialog from "../visual/VisualEditorDialog"
import BacktestWizardDialog from "../backtest/BacktestWizardDialog"
import { useDialogStore } from "../../store/dialog.store"

import { useFactorStore } from "../../store/backtest/factor.store"
import { useSignalStore } from "../../store/backtest/signal.store"
import { useStrategyStore } from "../../store/backtest/strategy.store"
import { useBacktestStore } from "../../store/backtest/backtest.store"

import BacktestDataDialog from "../backtest/BacktestDataDialog"
import { useDatasetStore, type Dataset,type BacktestDataSourceDef } from "../../store/dataset.store"

export default function GlobalDialogs({ nodes }: any) {

    const dialog = useDialogStore(state => state.dialog)
    const closeDialog = useDialogStore(state => state.closeDialog)

    const updateFactor = useFactorStore(state => state.updateFactor)

    const createSignal = useSignalStore(state => state.createSignal)
    const updateSignal = useSignalStore(state => state.updateSignal)

    const setStrategySignal = useStrategyStore(state => state.setStrategySignal)

    const setScheduleSignal = useBacktestStore(state => state.setScheduleSignal)

    const selectDataset = useDatasetStore(s => s.setCurrentDataset)
    
    const { expr } = dialog.payload || {}

    return (
        <>
        
            {/* ========================= */}
            {/* Visual Editor */}
            {/* ========================= */}
            
            {["factor", "signal", "schedule_signal"].includes(dialog.type) && (
                <VisualEditorDialog
                    open={dialog.open}
                    value={expr}
                    nodes={nodes}
                    onClose={closeDialog}
                    onConfirm={(expr: string) => {
                        const payload = dialog.payload || {}

                        if (dialog.type === "factor") {
                            const { factorId } = payload
                            if (!factorId) return
                            updateFactor(factorId, expr)
                        }

                        if (dialog.type === "signal") {
                            const { strategyId, signalId } = payload
                            if (!strategyId) return

                            let sid = signalId
                            if (!sid) {
                                sid = createSignal("")
                                setStrategySignal(strategyId, sid)
                            }

                            updateSignal(sid, expr)
                        }

                        if (dialog.type === "schedule_signal") {
                            const { scheduleSignalId } = payload
                            let sid = scheduleSignalId

                            if (!sid) sid = createSignal("")
                            updateSignal(sid, expr)

                            setScheduleSignal({
                                enabled: true,
                                signalId: sid
                            })
                        }

                        closeDialog()
                    }}
                />
            )}

            {/* ========================= */}
            {/* Backtest Data Dialog */}
            {/* ========================= */}
            {dialog.type === "backtest_data" && (
                <BacktestDataDialog
                    open={dialog.open}
                    initialValues={
                        { dataset: dialog.payload?.dataset }
                    }
                    onClose={closeDialog}
                    onConfirm={(datasetId: any) => {
                        selectDataset(datasetId)
                        closeDialog()
                    }}
                />
            )}

            {dialog.type === "backtest_wizard" && (
                    <BacktestWizardDialog
                        open={dialog.open}
                        dataset={dialog.payload?.dataset}
                        onClose={closeDialog}
                        onConfirm={(dataset: Dataset) => {  
                            const { runBacktest } = dialog.payload
                            selectDataset(dataset.id)  
                            runBacktest(dataset.sourceDef)
                            closeDialog()
                        }}
                    />
                )
            }
        </>
    )
}