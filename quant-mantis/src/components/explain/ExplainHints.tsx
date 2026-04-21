import { Box, Typography, Chip, Stack } from "@mui/material"

type Hint = {
    level: "info" | "warning" | "error"
    message: string
}

const colorMap = {
    info: "default",
    warning: "warning",
    error: "error"
} as const

export default function ExplainHints({ hints }: { hints: Hint[] }) {

    return (
        <Box
            sx={{
                p: 2,
                border: "1px solid rgba(255,255,255,0.1)",
                borderRadius: 1,
                mb: 2
            }}
        >
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
                🧠 Optimization Hints
            </Typography>

            <Stack spacing={1}>
                {hints.map((h, i) => (
                    <Box key={i} sx={{ display: "flex", gap: 1 }}>
                        <Chip
                            size="small"
                            label={h.level.toUpperCase()}
                            color={colorMap[h.level] as any}
                        />
                        <Typography fontSize={14}>
                            {h.message}
                        </Typography>
                    </Box>
                ))}
            </Stack>
        </Box>
    )
}