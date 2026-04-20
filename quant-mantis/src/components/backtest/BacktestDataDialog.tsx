import { useState, useRef } from "react"
import {
    Dialog,
    DialogTitle,
    DialogContent,
    DialogActions,
    Button,
    Tabs,
    Tab,
    Box,
    TextField,
    Stack,
    MenuItem,
    Select,
    Typography
} from "@mui/material"
import { useEffect } from "react"
import UniDataGrid from "../table/UniDataGrid"
import { GridToolbar } from "@mui/x-data-grid"

import PlayArrowIcon from "@mui/icons-material/PlayArrow"

import { apiClient } from "../../api/Client"
import { type BacktestDataSource } from "../../store/dataset.store"

interface Props {
    open: boolean
    initialValue?: BacktestDataSource
    onClose: () => void
    onConfirm: (ds: any) => void
}

export default function BacktestDataDialog({
    open,
    initialValue,
    onClose,
    onConfirm
}: Props) {

    useEffect(() => {
    if (!initialValue) return

    if (initialValue.type === "sql") {
        setSql(initialValue.sql)
        setTab(1)
    } else {
        setMarkets(initialValue.markets || [])
        setSymbols(initialValue.symbols?.join(",") || "")
        setStart(initialValue.start)
        setEnd(initialValue.end)
        setTab(0)
    }

    // reset validate 状态
    setValid(false)
    setData({ rows: [], columns: [] })

    }, [initialValue, open])

    const isSql = initialValue?.type === "sql"

    const [tab, setTab] = useState(isSql ? 1 : 0)

    // =========================
    // Preset
    // =========================
    const [markets, setMarkets] = useState<string[]>([])
    const [symbols, setSymbols] = useState(
        initialValue?.type === "preset" && initialValue.symbols
            ? initialValue.symbols.join(",")
            : ""
    )
    const [start, setStart] = useState(
        initialValue?.type === "preset" ? initialValue.start : "2020-01-01"
    )
    const [end, setEnd] = useState(
        initialValue?.type === "preset" ? initialValue.end : "2024-01-01"
    )

    // =========================
    // SQL
    // =========================
    const [sql, setSql] = useState(
        initialValue?.type === "sql"
            ? initialValue.sql
            : "SELECT * FROM stock_daily LIMIT 100;"
    )

    const [data, setData] = useState<any>({ rows: [], columns: [] })
    const [valid, setValid] = useState(false)

    const inputRef = useRef<HTMLTextAreaElement | null>(null)

    // =========================
    // 执行 SQL（验证）
    // =========================
    const runSql = async () => {

        let finalSql = sql

        const el = inputRef.current
        if (el) {
            const { selectionStart, selectionEnd, value } = el
            if (selectionStart !== selectionEnd) {
                finalSql = value.slice(selectionStart, selectionEnd)
            }
        }

        if (!finalSql.trim()) return

        const res = await apiClient.post("/execute_sql", { sql: finalSql })

        if (res.data.status === "success") {

            const rows = res.data.data

            if (rows.length > 0) {
                const columns = Object.keys(rows[0]).map((key) => ({
                    field: key,
                    headerName: key.toUpperCase(),
                    width: 150
                }))

                setData({
                    columns,
                    rows
                })

                setValid(true)
            } else {
                setData({ rows: [], columns: [] })
                setValid(true)
            }
        } else {
            setValid(false)
        }
    }

    // =========================
    // Confirm
    // =========================
    const handleConfirm = () => {

        if (tab === 1) {
            if (!valid) return

            onConfirm({
                type: "sql",
                sql,
                schema: data.columns.map((c: any) => c.field)
            })

        } else {
            onConfirm({
                type: "preset",
                markets: markets.length ? markets : undefined,
                symbols: symbols
                    ? symbols.split(",").map(s => s.trim()).filter(Boolean)
                    : undefined,
                start,
                end
            })
        }
    }

    return (
        <Dialog open={open} onClose={onClose} fullWidth maxWidth="lg">

            <DialogTitle>Backtest Data</DialogTitle>

            <DialogContent>

                <Tabs value={tab} onChange={(_, v) => setTab(v)}>
                    <Tab label="Preset" />
                    <Tab label="SQL" />
                </Tabs>

                {/* ================= Preset ================= */}
                {tab === 0 && (
                    <Stack spacing={2} sx={{ mt: 2 }}>

                        <Select
                            multiple
                            value={markets || []}   // 🔥 兜底（关键）
                            onChange={(e) => setMarkets(e.target.value as ("cn" | "hk" | "us")[])}
                            renderValue={(selected) => (selected as string[]).join(", ")}
                            >
                            <MenuItem value="cn">CN</MenuItem>
                            <MenuItem value="hk">HK</MenuItem>
                            <MenuItem value="us">US</MenuItem>
                        </Select>

                        <TextField
                            label="Symbols (comma separated, e.g. AAPL.US, TSLA.US)"
                            value={symbols}
                            onChange={(e) => setSymbols(e.target.value)}
                        />
                        
                        {/* 👇 放这里 */}
                        <Typography variant="caption" sx={{ mt: 1, color: "#746d6d" }}>
                        note: Filters are combined with AND
                        </Typography>

                        <Stack direction="row" spacing={2}>
                            <TextField
                                type="date"
                                label="Start"
                                value={start}
                                onChange={(e) => setStart(e.target.value)}
                                InputLabelProps={{ shrink: true }}
                            />
                            <TextField
                                type="date"
                                label="End"
                                value={end}
                                onChange={(e) => setEnd(e.target.value)}
                                InputLabelProps={{ shrink: true }}
                            />
                        </Stack>

                    </Stack>
                )}

                {/* ================= SQL ================= */}
                {tab === 1 && (
                    <Box sx={{ mt: 2 }}>

                        <TextField
                            fullWidth
                            multiline
                            rows={5}
                            value={sql}
                            inputRef={inputRef}
                            onChange={(e) => setSql(e.target.value)}
                            sx={{
                                mb: 2,
                                '& .MuiInputBase-input': {
                                    fontFamily: 'Monaco, monospace'
                                }
                            }}
                        />

                        <Button
                            variant="contained"
                            startIcon={<PlayArrowIcon />}
                            onClick={runSql}
                            sx={{ mb: 2 }}
                        >
                            Validate
                        </Button>

                        {data.rows.length > 0 && (
                            <Box sx={{ height: 300 }}>
                                <UniDataGrid
                                    rows={data.rows}
                                    columns={data.columns}
                                    slots={{ toolbar: GridToolbar }}
                                    density="compact"
                                />
                            </Box>
                        )}

                    </Box>
                )}

            </DialogContent>

            <DialogActions>
                <Button onClick={onClose}>Cancel</Button>
                <Button
                    variant="contained"
                    onClick={handleConfirm}
                    disabled={tab === 1 && !valid}
                >
                    Confirm
                </Button>
            </DialogActions>

        </Dialog>
    )
}