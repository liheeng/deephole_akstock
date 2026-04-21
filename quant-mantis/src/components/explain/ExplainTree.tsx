import { Box, Typography, Chip } from "@mui/material"
import type { PlanNode } from "../../utils/parseDuckDBExplain"

function getNodeStyle(type?: string, rows?: number) {

    const r = rows || 0
    
    if (type === "PROJECTION") return { bgcolor: "#041d6e", color: "#fff" }

    if (type === "SEQ_SCAN") {
        if (r > 5_000_000) return { bgcolor: "#d32f2f", color: "#fff" }
        if (r > 1_000_000) return { bgcolor: "#ed6c02", color: "#fff" }
        return {}
    }

    if (type?.includes("JOIN")) {
        return { bgcolor: "#f57c00", color: "#fff" }
    }

    if (type === "ORDER_BY" && r > 1_000_000) {
        return { bgcolor: "#ffa000", color: "#000" }
    }

    if (type === "FILTER") return { bgcolor: "#03981c", color: "#fff" }

    return {}
}

export default function ExplainTree({
    node,
    nodeFontSize = 13,
    detailFontSize = 12
}: {
    node: PlanNode
    nodeFontSize?: number
    detailFontSize?: number
}) {
    const style = getNodeStyle(node.name, node.rows)
    
    return (
        <Box sx={{ ml: 1.5, pl: 2, borderLeft: "1px dashed rgba(255,255,255,0.2)" }}>

            {/* ===== Node Header ===== */}
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 0.5 }}>

                <Chip
                    size="small"
                    label={node.name}
                    sx={{
                        fontSize: nodeFontSize,
                        height: 24,
                        ...style
                    }}
                />

                {node.rows && (
                    <Typography
                        sx={{
                            fontSize: nodeFontSize,
                            opacity: 0.8,
                            color:
                                parseInt(node.rows) > 5_000_000
                                    ? "error.main"
                                    : parseInt(node.rows) > 1_000_000
                                    ? "warning.main"
                                    : "text.secondary"
                        }}
                    >
                        ~{node.rows.toLocaleString()} rows
                    </Typography>
                )}

            </Box>

            {/* ===== Extra Info ===== */}
            {node.extra.length > 0 && (
                <Box sx={{ mb: 1 }}>
                    {node.extra.slice(0, 5).map((e, i) => (
                        <Typography
                            key={i}
                            sx={{
                                display: "block",
                                fontFamily: "monospace",
                                fontSize: detailFontSize,
                                opacity: 0.7,
                                lineHeight: 1.4
                            }}
                        >
                            {e}
                        </Typography>
                    ))}
                </Box>
            )}

            {/* ===== Children ===== */}
            {node.children.map((c, i) => (
                <ExplainTree
                    key={i}
                    node={c}
                    nodeFontSize={nodeFontSize}
                    detailFontSize={detailFontSize}
                />
            ))}

        </Box>
    )
}