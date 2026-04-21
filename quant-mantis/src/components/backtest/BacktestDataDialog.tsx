import { useState, useRef, useEffect, forwardRef, useImperativeHandle } from "react"
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
import { type BacktestDataSourceDef } from "../../store/dataset.store"
import { DatePicker } from "@mui/x-date-pickers/DatePicker"
import dayjs from "dayjs"

import { Light as SyntaxHighlighter } from "react-syntax-highlighter"
import sql from "react-syntax-highlighter/dist/esm/languages/hljs/sql"
import { atomOneDark } from "react-syntax-highlighter/dist/esm/styles/hljs"
import { format } from "sql-formatter"
SyntaxHighlighter.registerLanguage("sql", sql)
import { buildBacktestSQL_v2, buildCountSQL_v2, buildExplainSQL_v2 } from "../../datasource/sql/buildBacktestSQL"
import ExplainDetailDialog from "./ExplainDetailDialog"
import ExplainPanel from "../explain/ExplainPanel"

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
            {/* <Box sx={{ flex: 1 }}>{children}</Box> */}
            <Box sx={{ flex: 1, minWidth: 0 }}>{children}</Box>
        </Box>
    )
}

export type BacktestDataEditPanelRef = {
    getValue: () => BacktestDataSourceDef | null
    isValid: () => boolean
}

type Props = {
    initialValue: { datasetSourceDef: BacktestDataSourceDef }
}

export default function BacktestDataDialog(props: any) {
    const datasetSourceDef: BacktestDataSourceDef = props.initialValue?.datasetSourceDef
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
                    initialValue={datasetSourceDef}
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


export const BacktestDataEditPanel = forwardRef<BacktestDataEditPanelRef, Props>(
    ({ initialValue }, ref) => {


        const datasetSourceDef: BacktestDataSourceDef = initialValue?.datasetSourceDef
        const isSql = (datasetSourceDef?.type === "sql")

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

        const [previewSql, setPreviewSql] = useState("")
        const [rowCount, setRowCount] = useState<number | null>(null)
        const [explain, setExplain] = useState<string>("")

        const [explainDetailOpen, setExplainDetailOpen] = useState(false)

        // =========================
        // 同步 initialValue
        // =========================
        useEffect(() => {
            if (!datasetSourceDef) return

            if (datasetSourceDef.type === "sql") {
                setSql(datasetSourceDef.sql)
                setTab(1)
            } else {
                setMarkets(datasetSourceDef.markets || [])
                setSymbols(datasetSourceDef.symbols?.join(",") || "")
                setSectors(datasetSourceDef.sectors || [])
                setUniverse(datasetSourceDef.universe || "")
                setStart(datasetSourceDef.start)
                setEnd(datasetSourceDef.end)
                setTab(0)
            }

            setValid(false)
            setData({ rows: [], columns: [] })

        }, [initialValue, open])

        useEffect(() => {
            if (tab !== 0) return

            const ds = {
                markets,
                symbols: symbols.split(",").map(s => s.trim()).filter(Boolean),
                sectors,
                universe,
                start,
                end
            }

            const sql = buildBacktestSQL_v2(ds)
            setPreviewSql(sql)

        }, [markets, symbols, sectors, universe, start, end, tab])

        useImperativeHandle(ref, () => ({
            getValue: () => {
                if (tab === 1) {
                    if (!valid) return null

                    return {
                        type: "sql",
                        sql,
                        schema: data.columns.map((c: any) => c.field)
                    }
                }

                return {
                    type: "preset",
                    markets: markets.length ? markets : undefined,
                    symbols: symbols
                        ? symbols.split(",").map(s => s.trim()).filter(Boolean)
                        : undefined,
                    sectors: sectors.length ? sectors : undefined,
                    universe: universe || undefined,
                    start,
                    end
                }
            },

            isValid: () => {
                if (tab === 1) return valid
                return true
            }
        }))

        const runExplain = async () => {

            if (!previewSql.trim()) return

            // 1️⃣ explain
            const res1 = await apiClient.post("/execute_sql", {
                sql: buildExplainSQL_v2(previewSql)
            })

            if (res1.data.status === "success") {
                setExplain(JSON.stringify(res1.data.data, null, 2))
            }

            // 2️⃣ count
            const res2 = await apiClient.post("/execute_sql", {
                sql: buildCountSQL_v2(previewSql)
            })

            if (res2.data.status === "success") {
                setRowCount(res2.data.data[0]?.cnt ?? null)
            }
        }

        const runCount = async () => {

            if (!previewSql.trim()) return

            const res = await apiClient.post("/execute_sql", {
                sql: buildCountSQL_v2(previewSql)
            })

            if (res.data.status === "success") {
                setRowCount(res.data.data[0]?.cnt ?? null)
            }
        }

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

        // // =========================
        // // confirm
        // // =========================
        // const handleConfirm = () => {

        //     if (tab === 1) {
        //         if (!valid) return

        //         onConfirm({
        //             type: "sql",
        //             sql,
        //             schema: data.columns.map((c: any) => c.field)
        //         })

        //     } else {
        //         onConfirm({
        //             type: "preset",
        //             markets: markets.length ? markets : undefined,
        //             symbols: symbols
        //                 ? symbols.split(",").map(s => s.trim()).filter(Boolean)
        //                 : undefined,
        //             sectors: sectors.length ? sectors : undefined,
        //             universe: universe || undefined,
        //             start,
        //             end
        //         })
        //     }
        // }

        return (
            <Box sx={{ height: "100%", width: "100%", display: "flex", flexDirection: "column" }}>

                <Tabs value={tab} onChange={(_, v) => setTab(v)} >


                    <Tab label="Preset" />
                    <Tab label="SQL" />
                </Tabs>
                <Box sx={{ flex: 1, overflow: "auto", mt: 2 }}>
                    {/* ================= Preset ================= */}
                    {tab === 0 && (
                        <Box
                            sx={{
                                display: "grid",
                                gridTemplateColumns: "1fr 1fr",   // 🔥 左右等分
                                gap: 2,
                                mt: 2
                            }}
                        >

                            {/* ===== 左：配置 ===== */}
                            <Box sx={{ minWidth: 450 }}>
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
                                                {["CN", "HK", "US"].map((m) => (
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
                                            <Stack
                                                direction="row"
                                                spacing={2}
                                                sx={{
                                                    width: "100%",
                                                    '& > *': {
                                                        flex: 1,
                                                        minWidth: 0   // 🔥 必须
                                                    }
                                                }}
                                            >

                                                <DatePicker
                                                    label="Start"
                                                    value={dayjs(start)}
                                                    onChange={(v) => v && setStart(v.format("YYYY-MM-DD"))}
                                                    slotProps={{
                                                        textField: {
                                                            size: "small",
                                                            fullWidth: true,
                                                            sx: {
                                                                minWidth: 0   // 🔥 关键：允许 shrink
                                                            }
                                                        }
                                                    }}
                                                />

                                                <DatePicker
                                                    label="End"
                                                    value={dayjs(end)}
                                                    onChange={(v) => v && setEnd(v.format("YYYY-MM-DD"))}
                                                    slotProps={{
                                                        textField: {
                                                            size: "small",
                                                            fullWidth: true,
                                                            sx: {
                                                                minWidth: 0   // 🔥 关键：允许 shrink
                                                            }
                                                        }
                                                    }}
                                                />

                                            </Stack>
                                        </FormRow>
                                    </Box>

                                </Stack>
                            </Box>
                            {/* ===== 右：SQL Preview ===== */}
                            {/* <Box sx={{ flex: 1 }}> */}
                            <Box
                                sx={{
                                    flex: 1,
                                    minWidth: 0,

                                    display: "flex",              // 🔥 关键
                                    flexDirection: "column",
                                    alignItems: "stretch",        // 🔥 强制子元素贴满（不是居中）
                                }}
                            >
                                <Typography variant="caption" color="text.secondary">
                                    SQL Preview
                                </Typography>

                                <Box
                                    sx={{
                                        bgcolor: "#111",
                                        borderRadius: 1,
                                        p: 2,
                                        minHeight: 200,
                                        overflow: "auto"
                                    }}
                                >
                                    <SyntaxHighlighter
                                        language="sql"
                                        style={atomOneDark}
                                        wrapLongLines
                                        customStyle={{
                                            margin: 0,
                                            padding: 12,
                                            fontSize: 12
                                        }}
                                    >
                                        {
                                            format(previewSql, {
                                                language: "sql",
                                                indentStyle: "standard",
                                                tabWidth: 2
                                            })
                                        }
                                    </SyntaxHighlighter>
                                    <pre
                                    // style={{
                                    //   margin: 0,
                                    //   textAlign: "left",
                                    //   fontFamily: "Monaco, monospace",
                                    //   fontSize: 12,
                                    //   whiteSpace: "pre-wrap"
                                    // }}
                                    >
                                        {/* {previewSql} */}
                                    </pre>
                                </Box>

                                {/* ===== actions ===== */}
                                <Stack direction="row" spacing={1} sx={{ mt: 1 }}>

                                    <Button
                                        size="small"
                                        onClick={() => navigator.clipboard.writeText(previewSql)}
                                    >
                                        Copy
                                    </Button>

                                    <Button
                                        size="small"
                                        onClick={runExplain}
                                    >
                                        Explain
                                    </Button>

                                    <Button variant="outlined" onClick={runCount}>
                                        Estimate Rows
                                    </Button>

                                    <Button
                                        variant="outlined"
                                        disabled={!explain.length}
                                        onClick={() => setExplainDetailOpen(true)}
                                    >
                                        🔍 View Detail
                                    </Button>
                                </Stack>

                                {/* ===== explain ===== */}
                                {rowCount !== null && (
                                    <Typography variant="caption" sx={{ mt: 1, display: "block" }}>
                                        Rows: {rowCount}
                                    </Typography>
                                )}

                                {explain && (
                                    <Box
                                        sx={{
                                            mt: 1,
                                            bgcolor: "#0a0a0a",
                                            p: 1,
                                            borderRadius: 1,
                                            fontSize: 14,
                                            maxHeight: 200,
                                            overflow: "auto"
                                        }}
                                    >
                                        <ExplainPanel text={explain} />
                                    </Box>
                                )}

                            </Box>

                        </Box>
                    )}

                    {/* ================= SQL ================= */}
                    {tab === 1 && (
                        <Box sx={{
                            flex: 1,
                            minWidth: 0,
                            mt: 2,
                            display: "flex",
                            flexDirection: "column",
                            height: "100%", // 占满父级高度
                        }}>

                            <TextField
                                fullWidth
                                multiline
                                rows={5}
                                value={sql}
                                inputRef={inputRef}
                                onChange={(e) => setSql(e.target.value)}
                                sx={{
                                    flex: 1, // 🔥 核心：占剩余高度
                                    overflow: "auto", // 内容超了滚动
                                    "& .MuiInputBase-root": {
                                        height: "100%", // 强制内部高度100%
                                        display: "flex",
                                    },
                                    "& .MuiInputBase-input": {
                                        height: "100% !important", // 覆盖 autosize
                                        flex: 1,
                                        overflow: "auto", // 输入框内滚动
                                        whiteSpace: "pre-wrap", // 保留换行
                                    },
                                }}
                                InputProps={{
                                    disableUnderline: true,
                                    // 用原生 textarea 而非 autosize
                                    multiline: true,
                                    rows: undefined,
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

                            {/* {data.rows.length > 0 && ( */}
                                <Box sx={{ 
                                    height: 300, 
                                    mt: 2,
                                    border: "1px solid rgba(186, 181, 181, 0.92)",  // 边框
                                    borderRadius: "8px",                         // 圆角
                                    overflow: "hidden"                           // 让表格圆角生效
                                }}>
                                    <UniDataGrid
                                        rows={data.rows}
                                        columns={data.columns}
                                        slots={{ toolbar: GridToolbar }}
                                        density="compact"
                                        sx={{ border: "none", backgroundColor: "#5c5a5a" }}
                                    />
                                </Box>
                            {/* )} */}
                        </Box>
                    )}

                </Box>

                <ExplainDetailDialog
                    open={explainDetailOpen}
                    onClose={() => setExplainDetailOpen(false)}
                    data={[explain]}  // 简单处理成表格数据
                />
            </Box>
        )
    })

BacktestDataEditPanel.displayName = "BacktestDataEditPanel"