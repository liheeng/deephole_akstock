import { useMemo, useState } from "react"
import {
    Box,
    TextField
} from "@mui/material"

import { parseDuckDBExplain } from "../../utils/parseDuckDBExplain"
import ExplainNode from "./ExplainNode"
import { analyzeExplain } from "../../utils/explainAnalyzer"
import ExplainHints from "./ExplainHints"

type Props = {
    text: string
    fontSize?: number
    nodeFontSize?: number
    detailFontSize?: number
}

export default function ExplainViewer({
    text,
    fontSize = 14,
    nodeFontSize = 13,
    detailFontSize = 12
}: Props) {

    const [keyword, setKeyword] = useState("")

    const tree = useMemo(() => parseDuckDBExplain(text), [text])
    const hints = analyzeExplain(tree)

    return (
        <Box sx={{ display: "flex", flexDirection: "column", height: "100%" }}>

            {/* 🔍 Search */}
            <Box sx={{ p: 1 }}>
                <TextField
                    size="small"
                    fullWidth
                    placeholder="Search (SCAN / FILTER / symbol / date...)"
                    value={keyword}
                    onChange={(e) => setKeyword(e.target.value)}
                />
            </Box>

            {/* ✅ 自动优化建议 */}
            <ExplainHints hints={hints} />

            {/* 🌳 Tree */}
            <Box
                sx={{
                    flex: 1,
                    overflow: "auto",
                    p: 2,
                    fontSize
                }}
            >
                {tree.map((n, i) => (
                    <ExplainNode
                        key={i}
                        node={n}
                        keyword={keyword}
                        nodeFontSize={nodeFontSize}
                        detailFontSize={detailFontSize}
                    />
                ))}
            </Box>
        </Box>
    )
}