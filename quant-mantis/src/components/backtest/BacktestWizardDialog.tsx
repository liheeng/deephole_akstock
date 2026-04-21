import {
    Dialog, DialogTitle, DialogContent, DialogActions,
    Button, Tabs, Tab, Box
} from "@mui/material"
import { useRef, useState } from "react"

import StepPreview from "./StepPreview"
import { BacktestDataEditPanel, type BacktestDataEditPanelRef } from "./BacktestDataDialog"
// import { useDialogStore } from "../../store/dialog.store"

export default function BacktestWizardDialog({
    open,
    onClose,
    datasetSourceDef,
    onConfirm
}: any) {

    const [tab, setTab] = useState(0)
    const [localDSD, setLocalDSD] = useState(datasetSourceDef)
    const panelRef = useRef<BacktestDataEditPanelRef>(null)

    const handleNext = () => {
        const val = panelRef.current?.getValue()

        if (!val) return  // 校验失败（比如 SQL 未 validate）

        setLocalDSD(val)
        setTab(1)
    }

    const handlePrev = () => {
        setTab(0)
    }

    return (
        <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>

            <DialogTitle>Run Backtest</DialogTitle>

            <DialogContent>

                <Tabs value={tab} onChange={(_, v) => setTab(v)}>
                    <Tab label="Data" />
                    <Tab label="Preview" />
                </Tabs>

                <Box sx={{mt:2, display: "flex", height: "100%" }}>

                    {tab === 0 && (
                        <BacktestDataEditPanel
                            ref={panelRef}
                            initialValue={ { datasetSourceDef: localDSD } }
                        />
                    )}

                    {tab === 1 && (
                        <StepPreview ds={localDSD} />
                    )}

                </Box>

            </DialogContent>

            <DialogActions>
                <Button onClick={onClose}>Cancel</Button>

                {tab === 0 && (
                    <Button variant="contained" onClick={handleNext}>
                        Next
                    </Button>
                )}

                {tab === 1 && (
                    <>
                        <Button onClick={handlePrev}>Back</Button>
                        <Button
                            variant="contained"
                            onClick={() => onConfirm(localDSD)}
                        >
                            Run
                        </Button>
                    </>
                )}
            </DialogActions>

        </Dialog>
    )
}