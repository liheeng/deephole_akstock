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
                        openDialog("backtest_data", { dataSource: dataset?.source })
                    }
                >
                    Edit
                </Button>
            }
        >

            {dataset?.source.type === "sql" ? (
                <Typography
                    sx={{ fontFamily: "Monaco, monospace", fontSize: 12 }}
                >
                    {dataset?.source.sql.slice(0, 120)}...
                </Typography>
            ) : (
                <Box>

                    <KeyValueRow
                        label="Markets"
                        value={dataset?.source.markets?.join(", ") || "None"}
                    />
                    <KeyValueRow label="Symbols" value={dataset?.source.symbols?.join(", ") || "None"} />
                    <KeyValueRow
                        label="Sectors"
                        value={dataset?.source.sectors?.join(", ") || "None"}
                    />

                    <KeyValueRow
                        label="Universe"
                        value={dataset?.source.universe || "None"}
                    />
                    <KeyValueRow label="Range" value={`${dataset?.source.start} ~ ${dataset?.source.end}`} />

                </Box>
            )}

        </MainCard>
    )
}