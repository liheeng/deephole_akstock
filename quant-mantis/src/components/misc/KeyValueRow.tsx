import { Box, Typography } from "@mui/material"

export default function KeyValueRow({
    label,
    value,
}: {
    label: string
    value: React.ReactNode
}) {
    return (
        <Box
            sx={{
                display: "grid",
                gridTemplateColumns: "90px 1fr",   // 👈 核心：固定 label 列
                alignItems: "center",
                columnGap: 1,
            }}
        >
            <Typography
                sx={{
                    color: "text.secondary",
                    fontSize: 16,
                    textAlign: "left",
                }}
            >
                {label}
            </Typography>

            <Typography
                sx={{
                    fontSize: 16,
                    fontFamily: "Monaco, monospace",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                }}
            >
                {value}
            </Typography>
        </Box>
    )
}