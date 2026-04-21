import {
    Dialog, DialogTitle, DialogContent, DialogActions,
    Button, Tabs, Tab, Box
} from "@mui/material"
import { useState } from "react"

import StepPreview from "./StepPreview"
import BacktestDataDialog from "./BacktestDataDialog"
import { useDialogStore } from "../../store/dialog.store"

export default function BacktestWizardDialog({
    open,
    onClose,
    datasetSourceDef,
    onConfirm
}: any) {

    const [tab, setTab] = useState(0)
    const [localDSD, setLocalDSD] = useState(datasetSourceDef)
    const closeDialog = useDialogStore(state => state.closeDialog)

    return (
        <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>

            <DialogTitle>Run Backtest</DialogTitle>

            <DialogContent>

                <Tabs value={tab} onChange={(_, v) => setTab(v)}>
                    <Tab label="Data" />
                    <Tab label="Preview" />
                </Tabs>

                <Box sx={{ mt: 2 }}>

                    {tab === 0 && (
                        <BacktestDataDialog
                            open={true}
                            initialValue={localDSD}
                            onClose={() => {
                                closeDialog()
                            }}
                            onConfirm={(sourceDef: any) => {
                                setLocalDSD(sourceDef)
                                closeDialog()
                            }}
                        />
                    )}

                    {tab === 1 && (
                        <StepPreview ds={localDSD} />
                    )}

                </Box>

            </DialogContent>

            <DialogActions>
                <Button onClick={onClose}>Cancel</Button>
                <Button
                    variant="contained"
                    onClick={() => onConfirm(localDSD)}
                >
                    Run
                </Button>
            </DialogActions>

        </Dialog>
    )
}