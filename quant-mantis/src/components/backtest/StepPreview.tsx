import { useEffect, useState } from "react"
import { Box, Typography, Button, Stack } from "@mui/material"
import { apiClient } from "../../api/Client"
import { buildBacktestSQL_v2, buildExplainSQL_v2, buildCountSQL_v2 } from "../../datasource/sql/buildBacktestSQL"

import { Prism as SyntaxHighlighter } from "react-syntax-highlighter"
import { atomOneDark } from "react-syntax-highlighter/dist/esm/styles/hljs"
import { format } from "sql-formatter"

export default function StepPreview({ ds }: { ds: any }) {

    const [sql, setSql] = useState("")
    const [explain, setExplain] = useState<any[]>([])
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

        setExplain(res.data.data || [])
        setLoading(false)
    }

    const runCount = async () => {
        setLoading(true)

        const res = await apiClient.post("/execute_sql", {
            sql: buildCountSQL_v2(ds)
        })

        setCount(res.data.data?.[0]?.cnt ?? null)
        setLoading(false)
    }

    return (
        <Stack spacing={2}>

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

            {/* Explain */}
            {explain.length > 0 && (
                <Box>
                    <Typography variant="caption">Execution Plan</Typography>
                    <pre style={{ fontSize: 12 }}>
                        {JSON.stringify(explain, null, 2)}
                    </pre>
                </Box>
            )}

            {/* Count */}
            {count !== null && (
                <Typography variant="body2">
                    Estimated Rows: <b>{count.toLocaleString()}</b>
                </Typography>
            )}
        </Stack>
    )
}