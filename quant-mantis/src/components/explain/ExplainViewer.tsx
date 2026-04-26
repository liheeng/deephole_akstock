import { useMemo, useState } from "react"
import {
    Box,
    TextField
} from "@mui/material"

import { parseDuckDBExplain } from "../../utils/parseDuckDBExplain"
import ExplainNodeItem from "./ExplainNodeItem"
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
    const hints = analyzeExplain(tree as any)

    return (
        <Box sx={{ display: "flex", overflow: "auto", flex:1, maxHeight: "100%", flexDirection: "column", height: "100%" }}>

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
            <ExplainHints hints={hints} props={{}} />

            {/* 🌳 Tree */}
            <Box
                sx={{
                    flex: 1,
                    maxHeight: "100%",
                    overflow: "auto",
                    p: 2,
                    fontSize
                }}
            >
                {tree.map((n, i) => (
                    <ExplainNodeItem
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