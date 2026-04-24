import { useState } from "react"
import { Box, Typography, Button, Tooltip, Stack, TextField, Popover, List, ListItemButton, ListItemText } from "@mui/material"
import IconButton from "@mui/material/IconButton"
import StorageIcon from "@mui/icons-material/Storage"
import MainCard from "../visual/MainCard"
import KeyValueRow from "../misc/KeyValueRow"

import { useDialogStore } from "../../store/dialog.store"
import { useDatasetStore, type Dataset } from "../../store/dataset.store"
import { useMessageStore } from "../../store/message.store"

import { updateDataset as apiUpdateDataset, fetchDatasets } from "../../api/Client"
export default function BacktestDataPanel() {


    const openDialog = useDialogStore(s => s.openDialog)
    // const getDatasetName = useDatasetStore(s => s.getDatasetName)
    const setDatesetName = useDatasetStore(s => s.setDatasetName)
    const getDataset = useDatasetStore(s => s.getDataset)
    const validateDataset = useDatasetStore(s => s.validateDataset)
    
    const currentDatasetId = useDatasetStore(s => s.currentDatasetId)
    const dataset = getDataset(currentDatasetId)
    const datasetName = dataset?.name

    const setOriginalDataset = useDatasetStore(s => s.setOriginalDataset)
    const isDirty = useDatasetStore(s =>
                    s.isDatasetDirty(s.currentDatasetId)
)

    const addMessage = useMessageStore(state => state.addMessage)

    const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)
    const [remoteDatasets, setRemoteDatasets] = useState<Dataset[]>([])

    const handleOpenSelect = async (e: React.MouseEvent<HTMLElement>) => {
        setAnchorEl(e.currentTarget)

        const res = await fetchDatasets()
        if (res) {
            setRemoteDatasets(res)
        }
    }

    const handleCloseSelect = () => {
        setAnchorEl(null)
    }

    const applyDataset = (ds: Dataset) => {
        // 👉 复用你 store 逻辑
        useDatasetStore.setState(state => ({
            datasets: [ds, ...state.datasets.filter(d => d.id !== ds.id)],
            currentDatasetId: ds.id
        }))
        setOriginalDataset(ds)
        handleCloseSelect()
    }

    const handleSave = async () => {
        const result = validateDataset(currentDatasetId)

        if (!result.isValid()) {
            addMessage("error", result.message || "Validation current portfolio config failed")
            return
        }

        if (!dataset) {
            addMessage("error", "No dataset found to save/update")
            return
        }

        const res = await apiUpdateDataset(dataset)

        if (res) {
            setOriginalDataset(dataset)
            addMessage("success", `Dataset config "${datasetName}" saved`)
        } else {
            addMessage("error", `Save dataset config "${datasetName}" failed`)
        }
    }
    return (
        <MainCard
            title={
                <Typography sx={{ textAlign: "left", minWidth: 0, mb: 1 }}>
                    📊 Backtest Data
                </Typography>
            }

            secondary={
                <Stack
                    component="div"
                    direction="row"
                    spacing={1}
                    alignItems="center"   // ⭐ 必须有
                    alignContent="left"
                >
                    <Tooltip title="Edit dataset name">
                        <TextField
                            size="small"
                            value={datasetName}
                            sx={{ flex: 1, width: 400, maxWidth: 600, height: 40, paddingLeft: 2 }}
                            onChange={(e) => {
                                if (e.target.value && e.target.value.trim() == datasetName) {
                                    return
                                }

                                setDatesetName(currentDatasetId || "", e.target.value)
                            }
                            }
                        />
                    </Tooltip>
                    <Tooltip title="Save current dataset config">
                        <Button
                            disabled={!isDirty}
                            size="small"
                            onClick={handleSave}
                            sx={{
                                height: 40,          // ⭐ 和 Select 对齐
                                display: "flex",
                                alignItems: "center"
                            }}
                        >
                            Save
                        </Button>
                    </Tooltip>
                    <Tooltip title="Select dataset">
                        <IconButton
                            size="small"
                            color="primary"
                            onClick={handleOpenSelect}
                            sx={{
                                height: 40,
                                "&:hover": {
                                    backgroundColor: "action.hover"
                                }
                            }}
                        >
                            <StorageIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>
                    <Tooltip title="Edit dataset">
                        <Button
                            size="small"
                            onClick={() =>
                                openDialog("backtest_data", { dataset: dataset })
                            }
                        >
                            Edit
                        </Button>
                    </Tooltip>
                </Stack>

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

            <Popover
                open={Boolean(anchorEl)}
                anchorEl={anchorEl}
                onClose={handleCloseSelect}
                anchorOrigin={{
                    vertical: "bottom",
                    horizontal: "left"
                }}
            >
                <List sx={{ width: 320, maxHeight: 400, overflow: "auto" }}>
                    {remoteDatasets.map(ds => (
                        <ListItemButton
                            key={ds.id}
                            onClick={() => applyDataset(ds)}
                            sx={{ alignItems: "flex-start" }}
                        >
                            <Box sx={{ display: "flex", flexDirection: "column", width: "100%" }}>
                                <Typography sx={{ fontSize: 16, fontWeight: 500 }}>
                                    {ds.id === currentDatasetId ? (
                                        <Box>
                                            💙 {ds.name}
                                        </Box>
                                    ) : (
                                        ds.name
                                    )}
                                </Typography>

                                <Typography
                                    sx={{
                                        fontSize: 13,
                                        opacity: 0.6
                                    }}
                                >
                                    ID: {ds.id}
                                </Typography>
                            </Box>
                        </ListItemButton>
                    ))}
                </List>
            </Popover>
        </MainCard>

    )
}