import { Box, Typography, Button } from "@mui/material"
import MainCard from "../visual/MainCard"
import KeyValueRow from "../misc/KeyValueRow"

import { useBacktestStore } from "../../store/backtest/backtest.store"
import { useDialogStore } from "../../store/dialog.store"
import { useDatasetStore } from "../../store/dataset.store"

export default function BacktestDataPanel() {


    const openDialog = useDialogStore(s => s.openDialog)
    const datasetId = useBacktestStore(s => s.datasetId)
    const datasets = useDatasetStore(s => s.datasets)

    const dataset = datasets.find(d => d.id === datasetId)

    return (
        <MainCard
            title={
                <Typography sx={{ textAlign: "left", width: "100%", mb: 1 }}>
                    📊 Backtest Data
                </Typography>
            }
            secondary={
                <Button
                    size="small"
                    onClick={() =>
                        openDialog("backtest_data", { datasetSourceDef: dataset?.sourceDef })
                    }
                >
                    Edit
                </Button>
            }
        >

            {dataset?.sourceDef?.type === "sql" ? (
                <Typography
                    sx={{ fontFamily: "Monaco, monospace", fontSize: 12 }}
                >
                    {dataset?.sourceDef?.sql.slice(0, 120)}...
                </Typography>
            ) : (
                <Box>

                    <KeyValueRow
                        label="Markets"
                        value={dataset?.sourceDef?.markets?.join(", ") || "None"}
                    />
                    <KeyValueRow label="Symbols" value={dataset?.sourceDef?.symbols?.join(", ") || "None"} />
                    <KeyValueRow
                        label="Sectors"
                        value={dataset?.sourceDef?.sectors?.join(", ") || "None"}
                    />

                    <KeyValueRow
                        label="Universe"
                        value={dataset?.sourceDef?.universe || "None"}
                    />
                    <KeyValueRow label="Range" value={`${dataset?.sourceDef?.start} ~ ${dataset?.sourceDef?.end}`} />

                </Box>
            )}

        </MainCard>
    )
}