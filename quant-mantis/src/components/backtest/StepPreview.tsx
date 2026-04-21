import { useEffect, useState } from "react"
import { Box, Typography, Button, Stack } from "@mui/material"
import { apiClient } from "../../api/Client"
import { buildBacktestSQL_v2, buildExplainSQL_v2, buildCountSQL_v2 } from "../../datasource/sql/buildBacktestSQL"

import { Prism as SyntaxHighlighter } from "react-syntax-highlighter"
import { atomOneDark } from "react-syntax-highlighter/dist/esm/styles/hljs"
import { format } from "sql-formatter"
import ExplainViewer from "../explain/ExplainViewer"

export default function StepPreview({ ds, ...props }: { ds: any }) {

    const [sql, setSql] = useState("")
    const [explain, setExplain] = useState("")
    const [count, setCount] = useState<number | null>(null)
    const [loading, setLoading] = useState(false)

    useEffect(() => {
        if (!ds) return

        const s = buildBacktestSQL_v2(ds)
        setSql(s)
    }, [ds])

    const runExplain = async () => {
        setLoading(true)

        const res = await apiClient.post("/execute_sql", {
            sql: buildExplainSQL_v2(ds)
        })

        if (res.data.status === "success") {
            setExplain(JSON.stringify(res.data.data, null, 2))
        }

        // setExplain(res.data.data || [])
        setLoading(false)
    }

    
    const runCount = async () => {
        setLoading(true)

        const res = await apiClient.post("/execute_sql", {
            sql: buildCountSQL_v2(ds)
        })
        if (res.data.status === "success") {
            setCount(res.data.data?.[0]?.cnt ?? null)
        }
        setLoading(false)
    }

    return (
        <Stack spacing={2} {...props}>

            {/* SQL */}
            <Box>
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

            {/* Actions */}
            <Stack direction="row" spacing={1}>
                <Button variant="outlined" onClick={runExplain}>
                    Explain
                </Button>

                <Button variant="outlined" onClick={runCount}>
                    Estimate Rows
                </Button>
            </Stack>
            
            {/* Count */}
            {count !== null && (
                <Typography variant="body2" sx={{ fontSize: 12 }}>
                    Estimated Rows: <b>{count?.toLocaleString()}</b>
                </Typography>
            )}

            {/* Explain */}
            {explain.length > 0 && (
  <Box sx={{ 
    maxHeight: 320,      // 限制高度
    overflow: "auto",    // 自动滚动
    border: "1px solid #333",
    borderRadius: "8px",
    bgcolor: "#111",
    p: 1.5,
    mt: 1,
    flex: 1,
    height: "100%"
  }}>
    <Typography variant="caption" sx={{ mb: 1, display: "block" }}>
      Execution Plan
    </Typography>

    <ExplainViewer
      text={explain}
      fontSize={16}
      nodeFontSize={15}
      detailFontSize={14}
    />
  </Box>
)}


        </Stack>
    )
}