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
    Typography,
    Checkbox,
    ListItemText
} from "@mui/material"

import UniDataGrid from "../table/UniDataGrid"
import { GridToolbar } from "@mui/x-data-grid"
import PlayArrowIcon from "@mui/icons-material/PlayArrow"

import { apiClient } from "../../api/Client"
import { type BacktestDataSource } from "../../store/dataset.store"
import { DatePicker } from "@mui/x-date-pickers/DatePicker"
import dayjs from "dayjs"

//
// ✅ FormRow
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
            <Typography sx={{ width: 140, fontSize: 16, color: "text.secondary" }}>
                {label}
            </Typography>
            <Box sx={{ flex: 1 }}>{children}</Box>
        </Box>
    )
}

export default function BacktestDataDialog({
    open,
    initialValue,
    onClose,
    onConfirm
}: {
    open: boolean
    initialValue?: BacktestDataSource
    onClose: () => void
    onConfirm: (ds: any) => void
}) {

    const isSql = initialValue?.type === "sql"

    const [tab, setTab] = useState(isSql ? 1 : 0)

    // ===== preset state =====
    const [markets, setMarkets] = useState<string[]>([])
    const [symbols, setSymbols] = useState("")
    const [sectors, setSectors] = useState<string[]>([])
    const [universe, setUniverse] = useState("")
    const [start, setStart] = useState("2020-01-01")
    const [end, setEnd] = useState("2024-01-01")

    // ===== sql =====
    const [sql, setSql] = useState("SELECT * FROM stock_daily LIMIT 100;")
    const [data, setData] = useState<any>({ rows: [], columns: [] })
    const [valid, setValid] = useState(false)

    const inputRef = useRef<HTMLTextAreaElement | null>(null)

    // =========================
    // 同步 initialValue
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
    // SQL validate
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
                setData({ rows, columns })
            } else {
                setData({ rows: [], columns: [] })
            }

            setValid(true)
        } else {
            setValid(false)
        }
    }

    // =========================
    // confirm
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
                sectors: sectors.length ? sectors : undefined,
                universe: universe || undefined,
                start,
                end
            })
        }
    }

    return (
        <Dialog
            open={open}
            onClose={onClose}
            fullWidth
            maxWidth={tab === 0 ? "md" : "md"}
        >
            <DialogTitle>Backtest Data</DialogTitle>

            <DialogContent>

                <Tabs value={tab} onChange={(_, v) => setTab(v)}>
                    <Tab label="Preset" />
                    <Tab label="SQL" />
                </Tabs>

                {/* ================= Preset ================= */}
                {tab === 0 && (
                    <Box sx={{ maxWidth: 600, mx: "auto", mt: 2 }}>
                        <Stack spacing={2}>

                            {/* 🥇 Universe */}
                            <Box sx={{ border: "1px solid rgba(255,255,255,0.1)", p: 2 }}>
                                <Typography variant="caption" color="text.secondary">
                                    🌐 Base Universe
                                </Typography>

                                <FormRow label="Universe">
                                    <Select
                                        fullWidth
                                        size="small"
                                        value={universe}
                                        onChange={(e) => setUniverse(e.target.value)}
                                        sx={{
                                            mt: 1,
                                            bgcolor: universe ? "rgba(24,144,255,0.15)" : undefined
                                        }}
                                    >
                                        <MenuItem value="">None</MenuItem>
                                        <MenuItem value="SP500">SP500</MenuItem>
                                        <MenuItem value="HS300">HS300</MenuItem>
                                    </Select>
                                </FormRow>

                                {!universe && (
                                    <Typography variant="caption" color="warning.main">
                                        No base universe (full market scan)
                                    </Typography>
                                )}
                            </Box>

                            {/* 🥈 Filters */}
                            <Box sx={{ border: "1px solid rgba(255,255,255,0.1)", p: 2 }}>
                                <Typography variant="caption" color="text.secondary">
                                    ⚙ Filters (AND)
                                </Typography>

                                <FormRow label="Markets">
                                    <Select
                                        multiple
                                        fullWidth
                                        size="small"
                                        value={markets}
                                        onChange={(e) => setMarkets(e.target.value as string[])}
                                        renderValue={(v) => (v as string[]).join(", ")}
                                    >
                                        {["cn", "hk", "us"].map((m) => (
                                            <MenuItem key={m} value={m}>
                                                <Checkbox checked={markets.includes(m)} />
                                                <ListItemText primary={m.toUpperCase()} />
                                            </MenuItem>
                                        ))}
                                    </Select>
                                </FormRow>

                                <FormRow label="Sectors">
                                    <Select
                                        multiple
                                        fullWidth
                                        size="small"
                                        value={sectors}
                                        onChange={(e) => setSectors(e.target.value as string[])}
                                        renderValue={(v) => (v as string[]).join(", ")}
                                    >
                                        {["SEC_TECH", "SEC_FINANCE", "SEC_AUTO"].map((s) => (
                                            <MenuItem key={s} value={s}>
                                                <Checkbox checked={sectors.includes(s)} />
                                                <ListItemText primary={s.replace("SEC_", "")} />
                                            </MenuItem>
                                        ))}
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

                                {!markets.length && !sectors.length && !symbols && (
                                    <Typography variant="caption" color="text.secondary">
                                        No filters applied
                                    </Typography>
                                )}
                            </Box>

                            {/* 🥉 Time */}
                            <Box sx={{ border: "1px solid rgba(255,255,255,0.1)", p: 2 }}>
                                <Typography variant="caption" color="text.secondary">
                                    🕒 Time Range
                                </Typography>

                                <FormRow label="Date">
                                    <Stack direction="row" spacing={2} sx={{ width: "100%" }}>

                                        <DatePicker
                                            label="Start"
                                            value={dayjs(start)}
                                            onChange={(v) => v && setStart(v.format("YYYY-MM-DD"))}
                                            slotProps={{
                                                textField: { size: "medium", fullWidth: true }
                                            }}
                                        />

                                        <DatePicker
                                            label="End"
                                            value={dayjs(end)}
                                            onChange={(v) => v && setEnd(v.format("YYYY-MM-DD"))}
                                            slotProps={{
                                                textField: { size: "medium", fullWidth: true }
                                            }}
                                        />

                                    </Stack>
                                </FormRow>
                            </Box>

                        </Stack>
                    </Box>
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
                            sx={{ mb: 2 }}
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