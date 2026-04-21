import { useState, useMemo } from "react"
import { Box, Typography, Chip } from "@mui/material"
import ExpandMoreIcon from "@mui/icons-material/ExpandMore"
import ChevronRightIcon from "@mui/icons-material/ChevronRight"
import type { PlanNode } from "../../utils/parseDuckDBExplain"

function highlight(text: string, keyword: string) {
    if (!keyword) return text

    const regex = new RegExp(`(${keyword})`, "ig")
    const parts = text.split(regex)

    return parts.map((p, i) =>
        regex.test(p)
            ? <span key={i} style={{ background: "#ffc107", color: "#000" }}>{p}</span>
            : p
    )
}

function matchNode(node: PlanNode, keyword: string): boolean {
    if (!keyword) return true

    const k = keyword.toLowerCase()

    if (node.name.toLowerCase().includes(k)) return true
    if (node.rows?.includes(k)) return true
    if (node.extra.some(e => e.toLowerCase().includes(k))) return true

    return node.children.some(c => matchNode(c, keyword))
}

export default function ExplainNode({
    node,
    keyword,
    nodeFontSize,
    detailFontSize
}: {
    node: PlanNode
    keyword: string
    nodeFontSize: number
    detailFontSize: number
}) {

    const matched = useMemo(() => matchNode(node, keyword), [node, keyword])

    // 🔥 自动展开命中路径
    const [open, setOpen] = useState(keyword ? true : true)

    if (!matched) return null

    return (
        <Box sx={{ ml: 1.5, pl: 2, borderLeft: "1px dashed rgba(255,255,255,0.2)" }}>

            {/* Header */}
            <Box
                sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 1,
                    cursor: "pointer"
                }}
                onClick={() => setOpen(!open)}
            >
                {open ? <ExpandMoreIcon fontSize="small" /> : <ChevronRightIcon fontSize="small" />}

                <Chip
                    size="small"
                    label={node.name}
                    sx={{ fontSize: nodeFontSize, height: 24 }}
                    color={node.name.includes("SCAN") ? "error" : "default"}
                />

                {node.rows && (
                    <Typography sx={{ fontSize: nodeFontSize, opacity: 0.7 }}>
                        {highlight(`~${node.rows} rows`, keyword)}
                    </Typography>
                )}
            </Box>

            {/* Body */}
            {open && (
                <Box sx={{ mt: 0.5 }}>

                    {/* extra */}
                    {node.extra.map((e, i) => (
                        <Typography
                            key={i}
                            sx={{
                                fontFamily: "monospace",
                                fontSize: detailFontSize,
                                opacity: 0.7
                            }}
                        >
                            {highlight(e, keyword)}
                        </Typography>
                    ))}

                    {/* children */}
                    {node.children.map((c, i) => (
                        <ExplainNode
                            key={i}
                            node={c}
                            keyword={keyword}
                            nodeFontSize={nodeFontSize}
                            detailFontSize={detailFontSize}
                        />
                    ))}

                </Box>
            )}
        </Box>
    )
}