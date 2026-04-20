import { useState, useRef, useEffect } from "react"
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

import UniDataGrid from "../table/UniDataGrid"
import { GridToolbar } from "@mui/x-data-grid"
import PlayArrowIcon from "@mui/icons-material/PlayArrow"

import { apiClient } from "../../api/Client"
import { type BacktestDataSource } from "../../store/dataset.store"
import { buildBacktestSQL } from "../../utils/buildBacktestSQL"

//
// ✅ FormRow（内联，避免多文件）
//
function FormRow({
    label,
    children
}: {
    label: string
    children: React.ReactNode
}) {
    return (
        <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Typography
                sx={{
                    width: 100,
                    fontSize: 16,
                    color: "text.secondary"
                }}
            >
                {label}
            </Typography>

            <Box sx={{ flex: 1 }}>{children}</Box>
        </Box>
    )
}

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

    const isSql = initialValue?.type === "sql"

    const [tab, setTab] = useState(isSql ? 1 : 0)

    // =========================
    // Preset
    // =========================
    const [markets, setMarkets] = useState<string[]>([])
    const [symbols, setSymbols] = useState("")
    const [sectors, setSectors] = useState<string[]>([])
    const [universe, setUniverse] = useState("")
    const [start, setStart] = useState("2020-01-01")
    const [end, setEnd] = useState("2024-01-01")

    // =========================
    // SQL
    // =========================
    const [sql, setSql] = useState("SELECT * FROM stock_daily LIMIT 100;")
    const [data, setData] = useState<any>({ rows: [], columns: [] })
    const [valid, setValid] = useState(false)

    const inputRef = useRef<HTMLTextAreaElement | null>(null)

    // =========================
    // 🔥 同步 initialValue（关键）
    // =========================
    useEffect(() => {
        if (!initialValue) return

        if (initialValue.type === "sql") {
            setSql(initialValue.sql)
            setTab(1)
        } else {
            setMarkets(initialValue.markets || [])
            setSymbols(initialValue.symbols?.join(",") || "")
            setSectors(initialValue.sectors || [])
            setUniverse(initialValue.universe || "")
            setStart(initialValue.start)
            setEnd(initialValue.end)
            setTab(0)
        }

        setValid(false)
        setData({ rows: [], columns: [] })

    }, [initialValue, open])

    // =========================
    // SQL 执行
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

                setData({ columns, rows })
            } else {
                setData({ rows: [], columns: [] })
            }

            setValid(true)
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
            const preset = {
                markets,
                symbols: symbols
                    ? symbols.split(",").map(s => s.trim()).filter(Boolean)
                    : undefined,
                sectors,
                universe,
                start,
                end
            }

            const sql = buildBacktestSQL(preset)

            onConfirm({
                type: "preset",
                markets: markets.length ? markets : undefined,
                symbols: symbols
                    ? symbols.split(",").map(s => s.trim()).filter(Boolean)
                    : undefined,
                sectors: sectors.length ? sectors : undefined,
                universe: universe || undefined,
                start,
                end,
                sql
            })
        }
    }

    return (
        <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">

            <DialogTitle>Backtest Data</DialogTitle>

            <DialogContent>

                <Tabs value={tab} onChange={(_, v) => setTab(v)}>
                    <Tab label="Preset" />
                    <Tab label="SQL" />
                </Tabs>

                {/* ================= Preset ================= */}
                {tab === 0 && (
                    <Stack spacing={2} sx={{ mt: 2 }}>

                        <FormRow label="Markets">
                            <Select
                                multiple
                                fullWidth
                                value={markets}
                                onChange={(e) => setMarkets(e.target.value as string[])}
                                renderValue={(v) => (v as string[]).join(", ")}
                                size="small"
                            >
                                <MenuItem value="cn">CN</MenuItem>
                                <MenuItem value="hk">HK</MenuItem>
                                <MenuItem value="us">US</MenuItem>
                            </Select>
                        </FormRow>

                        <FormRow label="Symbols">
                            <TextField
                                fullWidth
                                size="small"
                                value={symbols}
                                onChange={(e) => setSymbols(e.target.value)}
                                placeholder="AAPL.US, TSLA.US"
                            />
                        </FormRow>

                        <FormRow label="Sectors">
                            <Select
                                multiple
                                fullWidth
                                value={sectors}
                                onChange={(e) => setSectors(e.target.value as string[])}
                                renderValue={(v) => (v as string[]).join(", ")}
                                size="small"
                            >
                                <MenuItem value="SEC_TECH">Tech</MenuItem>
                                <MenuItem value="SEC_FINANCE">Finance</MenuItem>
                                <MenuItem value="SEC_ENERGY">Energy</MenuItem>
                            </Select>
                        </FormRow>

                        <FormRow label="Universe">
                            <Select
                                fullWidth
                                value={universe}
                                onChange={(e) => setUniverse(e.target.value)}
                                size="small"
                            >
                                <MenuItem value="">None</MenuItem>
                                <MenuItem value="SP500">SP500</MenuItem>
                                <MenuItem value="HS300">HS300</MenuItem>
                            </Select>
                        </FormRow>

                        <FormRow label="Date Range">
                            <Stack direction="row" spacing={2} sx={{ width: "100%" }}>
                                <TextField
                                    type="date"
                                    size="small"
                                    value={start}
                                    onChange={(e) => setStart(e.target.value)}
                                    fullWidth
                                />
                                <TextField
                                    type="date"
                                    size="small"
                                    value={end}
                                    onChange={(e) => setEnd(e.target.value)}
                                    fullWidth
                                />
                            </Stack>
                        </FormRow>

                        <Typography
                            variant="caption"
                            sx={{ color: "text.secondary", ml: "140px" }}
                        >
                            Filters are combined with AND
                        </Typography>

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