import {
    Dialog,
    DialogTitle,
    DialogContent,
    DialogActions,
    Button,
    Slider
} from "@mui/material"
import { useState } from "react"
// import ExplainPanel from "../explain/ExplainPanel"
import ExplainViewer from "../explain/ExplainViewer"

export default function ExplainDetailDialog({
    open,
    onClose,
    data
}: {
    open: boolean
    onClose: () => void
    data: any[]
}) {
    const [fontSize, setFontSize] = useState(14)

    return (
        <Dialog
            open={open}
            onClose={onClose}
            fullWidth
            maxWidth="xl"   // 👈 关键：大屏
        >
            <DialogTitle>Execution Plan (Explain)</DialogTitle>

            <DialogContent
                sx={{
                    height: "70vh",
                    display: "flex",
                    flexDirection: "column",
                    p: 0
                }}
            >
                <Slider
                    min={10}
                    max={20}
                    value={fontSize}
                    onChange={(_, v) => setFontSize(v as number)}
                />
                <ExplainViewer
                    text={data?.[0]}
                    fontSize={fontSize}
                    nodeFontSize={fontSize - 1}
                    detailFontSize={fontSize - 2}
                />
            </DialogContent>

            <DialogActions>
                <Button onClick={onClose}>Close</Button>
            </DialogActions>
        </Dialog>
    )
}