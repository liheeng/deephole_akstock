import { use, useRef } from "react"
import {
    Dialog,
    DialogTitle,
    DialogContent,
    DialogActions,
    Button
} from "@mui/material"

import { type BacktestDataSourceDef, type Dataset, useDatasetStore } from "../../store/dataset.store"
import { BacktestDataEditPanel, type BacktestDataEditPanelRef } from "./BacktestDataEditPanel"
import { Light as SyntaxHighlighter } from "react-syntax-highlighter"
import sql from "react-syntax-highlighter/dist/esm/languages/hljs/sql"
SyntaxHighlighter.registerLanguage("sql", sql)


type BacktestDataDialogProps = {
    initialValues: { dataset: Dataset }
    [key: string]: any
}

export default function BacktestDataDialog(props: BacktestDataDialogProps) {
    // const datasetSourceDef: BacktestDataSourceDef = props.initialValues?.datasetSourceDef
    const panelRef = useRef<BacktestDataEditPanelRef>(null)
    const dataset = props.initialValues?.dataset
    const createDataset = useDatasetStore(s => s.createDataset)
    const updateSourceDef = useDatasetStore(s => s.updateSourceDef)

    return (
        <Dialog open={props.open} onClose={props.onClose} maxWidth="lg" fullWidth>

            <DialogTitle>Backtest Data</DialogTitle>

            <DialogContent sx={{
                minHeight: "40vh",     // 👈 加这一行！强制最小高度
                display: "flex",
                flexDirection: "column",
                height: "100%", // 让子元素能100%高度
            }}>
                <BacktestDataEditPanel
                    ref={panelRef}
                    // initialValue={datasetSourceDef}
                    {...props}
                />
                {/* <BacktestDataEditPanel {...props} /> */}
            </DialogContent>

            <DialogActions>
                <Button onClick={props.onClose}>Cancel</Button>
                <Button
                    onClick={
                        () => {
                            const datasetSourceDef: BacktestDataSourceDef = panelRef.current?.getValue()
                            if (!dataset) {
                                const ds = createDataset(datasetSourceDef)
                                props.onConfirm?.(ds.id)
                            } else {
                                updateSourceDef(dataset.id, datasetSourceDef)
                                props.onConfirm?.(dataset.id)
                            }
                        }
                    }
                >
                    Confirm
                </Button>
            </DialogActions>

        </Dialog>
    )
}


