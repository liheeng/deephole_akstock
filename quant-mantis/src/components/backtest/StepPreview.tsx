import { useEffect, useState } from "react"
import { Box, Typography, Button, Stack } from "@mui/material"
import { apiClient } from "../../api/Client"
import { buildBacktestSQL_v2, buildExplainSQL_v2, buildCountSQL_v2 } from "../../datasource/sql/buildBacktestSQL"

import { Prism as SyntaxHighlighter } from "react-syntax-highlighter"
import { atomOneDark } from "react-syntax-highlighter/dist/esm/styles/hljs"
import { format } from "sql-formatter"
import ExplainViewer from "../explain/ExplainViewer"
import ExplainDetailDialog from "./ExplainDetailDialog"

export default function StepPreview({ ds, sx, ...props }: { ds: any, sx: any, props?: any }) {
    const [sql, setSql] = useState("")
    const [explain, setExplain] = useState("")
    const [count, setCount] = useState<number | null>(null)
    // const [loading, setLoading] = useState(false)
    const [explainDetailOpen, setExplainDetailOpen] = useState(false)

    useEffect(() => {
        if (!ds) return
        const s = buildBacktestSQL_v2(ds)
        if (!s) return
        setSql(s)
    }, [ds])


    const runExplain = async () => {
        // setLoading(true)
        const res = await apiClient.post("/execute_sql", {
            sql: buildExplainSQL_v2(ds)
        })
        if (res.data.status === "success") {
            setExplain(JSON.stringify(res.data.data, null, 2))
        }
        // setLoading(false)
    }

    const runCount = async () => {
        // setLoading(true)
        const res = await apiClient.post("/execute_sql", {
            sql: buildCountSQL_v2(ds)
        })
        if (res.data.status === "success") {
            setCount(res.data.data?.[0]?.cnt ?? null)
        }
        // setLoading(false)
    }

    return (
        <Stack
            spacing={2}
            {...props}
            sx={{
                ...sx,
                height: "100%",
                flex: 1,
                minHeight: 0,       // 🔥 必须
                flexDirection: "column",
                overflow: "hidden",
            }}
        >
            {/* SQL 预览区 */}
            <Box sx={{ flexShrink: 0 }}>
                <Typography variant="subtitle2">SQL Preview</Typography>
                <Box sx={{ border: "1px solid #333", borderRadius: 1 }}>
                    <SyntaxHighlighter
                        language="sql"
                        style={atomOneDark}
                        customStyle={{
                            margin: 0,
                            fontSize: 12,
                            padding: 12,
                            background: "#1e1e1e"
                        }}
                    >
                        {format(sql)}
                    </SyntaxHighlighter>
                </Box>
            </Box>

            {/* 按钮 */}
            <Stack direction="row" spacing={1} sx={{ flexShrink: 0 }}>
                <Button variant="outlined" onClick={runExplain}>Explain</Button>
                <Button variant="outlined" onClick={runCount}>Estimate Rows</Button>
                <Button
                    variant="outlined"
                    disabled={!explain.length}
                    onClick={() => setExplainDetailOpen(true)}
                >
                    🔍 View Detail
                </Button>
            </Stack>

            {/* 行数提示 */}
            {count !== null && (
                <Typography variant="body2" sx={{ fontSize: 12, flexShrink: 0 }}>
                    Estimated Rows: <b>{count?.toLocaleString()}</b>
                </Typography>
            )}

            {/* ====================== 核心：正常显示 + 占满高度 + 可滚动 ====================== */}
            <Box
                sx={{
                    flex: 1,
                    minHeight: 0,
                    overflow: "hidden",   // 🔥 关键：限制内部滚动区域
                    border: "1px solid #333",
                    borderRadius: "8px",
                    bgcolor: "#111",
                    p: 1.5,
                    display: "flex",
                    flexDirection: "column",
                }}
            >
                <Typography variant="caption" sx={{ mb: 1, display: "block" }}>
                    Execution Plan
                </Typography>

                <Box
                    sx={{
                        flex: 1,
                        overflow: "auto",
                        minHeight: 0,
                    }}
                >
                    {explain ? (
                        <ExplainViewer
                            text={explain}
                            fontSize={16}
                            nodeFontSize={15}
                            detailFontSize={14}
                        />
                    ) : (
                        <Typography variant="caption" color="text.secondary">
                            点击 Explain 查看执行计划
                        </Typography>
                    )}
                </Box>

            </Box>
        
            <ExplainDetailDialog
                open={explainDetailOpen}
                onClose={() => setExplainDetailOpen(false)}
                data={[explain]}  // 简单处理成表格数据
            />
        </Stack>
    )
}