import { useRef } from "react"
import {
    Dialog,
    DialogTitle,
    DialogContent,
    DialogActions,
    Button
} from "@mui/material"

import { type BacktestDataSourceDef } from "../../store/dataset.store"
import { BacktestDataEditPanel, type BacktestDataEditPanelRef } from "./BacktestDataEditPanel"
import { Light as SyntaxHighlighter } from "react-syntax-highlighter"
import sql from "react-syntax-highlighter/dist/esm/languages/hljs/sql"
SyntaxHighlighter.registerLanguage("sql", sql)


type BacktestDataDialogProps = {
    initialValues: { datasetSourceDef: BacktestDataSourceDef }
    [key: string]: any
}

export default function BacktestDataDialog(props: BacktestDataDialogProps) {
    // const datasetSourceDef: BacktestDataSourceDef = props.initialValues?.datasetSourceDef
    const panelRef = useRef<BacktestDataEditPanelRef>(null)

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
                    onClick={() => {
                        const datasetSourceDef = panelRef.current?.getValue()
                        props.onConfirm?.(datasetSourceDef)
                    }
                    }
                >
                    Confirm
                </Button>
            </DialogActions>

        </Dialog>
    )
}


