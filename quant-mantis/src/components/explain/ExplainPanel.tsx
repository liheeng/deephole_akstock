import { Box, Typography } from "@mui/material"
import { parseDuckDBExplain } from "../../utils/parseDuckDBExplain"
import ExplainTree from "./ExplainTree"
import { analyzeExplain } from "../../utils/explainAnalyzer"
import ExplainHints from "./ExplainHints"

type Props = {
    text: string
    fontSize?: number
    nodeFontSize?: number
    detailFontSize?: number
}

export default function ExplainPanel({
    text,
    fontSize = 14,
    nodeFontSize = 13,
    detailFontSize = 12
}: Props) {

    if (!text) {
        return (
            <Typography variant="body2" color="text.secondary">
                No explain data
            </Typography>
        )
    }

    const tree = parseDuckDBExplain(text);
    const hints = analyzeExplain(tree);

    return (
        <Box
            sx={{
                flex: 1,
                overflow: "auto",
                p: 2,
                fontSize,
                display: "flex",
                flexDirection: "column",
                gap: 1
            }}
        >
            {/* ✅ 自动优化建议 */}
            <ExplainHints hints={hints} props={{}} />

            {/* explain tree */}
            <Box sx={{ flex: 1, fontSize, overflow: "auto" }}>
                {tree.map((n, i) => (
                    <ExplainTree
                        key={i}
                        node={n}
                        nodeFontSize={nodeFontSize}
                        detailFontSize={detailFontSize}
                    />
                ))}
            </Box>
        </Box>
    )
}