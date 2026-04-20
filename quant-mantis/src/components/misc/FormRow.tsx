// components/misc/FormRow.tsx

import { Box, Typography } from "@mui/material"

export default function FormRow({
    label,
    children
}: {
    label: string
    children: React.ReactNode
}) {
    return (
        <Box
            sx={{
                display: "flex",
                alignItems: "center",
                gap: 2
            }}
        >
            <Typography
                sx={{
                    width: 140,
                    fontSize: 13,
                    color: "text.secondary"
                }}
            >
                {label}
            </Typography>

            <Box sx={{ flex: 1 }}>
                {children}
            </Box>
        </Box>
    )
}