import {
    Dialog, DialogTitle, DialogContent, DialogActions,
    Button, Tabs, Tab, Box
} from "@mui/material"
import { useRef, useState } from "react"

import StepPreview from "./StepPreview"
import { BacktestDataEditPanel, type BacktestDataEditPanelRef } from "./BacktestDataEditPanel"
import { useDatasetStore } from "../../store/dataset.store"


export default function BacktestWizardDialog({
    open,
    onClose,
    dataset,
    onConfirm
}: any) {

    const createDataset = useDatasetStore(s => s.createDataset)
    const updateSourceDef = useDatasetStore(s => s.updateSourceDef)
    const [tab, setTab] = useState(0)
    const [localDataset, setLocalDataset] = useState(dataset)
    const panelRef = useRef<BacktestDataEditPanelRef>(null)

    const handleNext = () => {
        const datasetSourceDef = panelRef.current?.getValue()

        if (!datasetSourceDef) return  // 校验失败（比如 SQL 未 validate）

        if (!dataset) {
            const ds = createDataset(datasetSourceDef)
            setLocalDataset(ds)
        } else {
            const ds = updateSourceDef(dataset.id, datasetSourceDef)
            setLocalDataset(ds)
        }
        
        setTab(1)
    }

    const handlePrev = () => {
        setTab(0)
    }

    return (
        <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>

            <DialogTitle>Run Backtest</DialogTitle>

            <DialogContent
                sx={{
                    height: "50vh",          // 👈 简化！不要 min/max 同时写
                    display: "flex",
                    flexDirection: "column",
                    overflow: "hidden",      // 🔥 必须
                    minHeight: 0             // 🔥 必须
                }}
            >

                <Tabs value={tab} onChange={(_, v) => setTab(v)}>
                    <Tab label="Data" />
                    <Tab label="Preview" />
                </Tabs>

                <Box 
                    sx={{
                        mt: 2,
                        display: "flex",
                        flex: 1,
                        minHeight: 0,        // 🔥 关键
                        width: "100%",
                        border: "1px solid rgba(186, 181, 181, 0.92)",
                        borderRadius: "8px",
                        overflow: "hidden",
                        p: 1,
                    }}
                >

                    {tab === 0 && (
                        <BacktestDataEditPanel
                            ref={panelRef}
                            initialValues={{ dataset: localDataset }}
                            sx={{ flex: 1, width: "100%" }}
                        />
                    )}

                    {tab === 1 && (
                        <StepPreview
                            ds={localDataset.sourceDef} 
                            sx={{ flex: 1, width: "100%" }}
                        />
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
                            onClick={() => onConfirm(localDataset)}
                        >
                            Run
                        </Button>
                    </>
                )}
            </DialogActions>

        </Dialog>
    )
}