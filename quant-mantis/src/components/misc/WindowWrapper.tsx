import { Box, IconButton } from "@mui/material"
import { useState } from "react"
import AspectRatioIcon from "@mui/icons-material/AspectRatio"
import FullscreenExitIcon from "@mui/icons-material/FullscreenExit"

export default function WindowWrapper({
    title,
    children,
    defaultHeight,
}: {
    title: string
    children: React.ReactNode
    defaultHeight?: string | number
}) {
    const [maximized, setMaximized] = useState(false)

    return (
        <Box
            sx={{
                position: maximized ? "fixed" : "relative",
                top: maximized ? 0 : undefined,
                left: maximized ? 0 : undefined,
                width: maximized ? "100vw" : "100%",
                height: maximized ? "100vh" : defaultHeight ?? "100%",
                zIndex: maximized ? 1300 : "auto",
                bgcolor: "#0f0f0f",
                border: "1px solid #333",
                borderRadius: 1,
                display: "flex",
                flexDirection: "column",
                overflow: "hidden",
            }}
        >
            {/* ===== Window Bar ===== */}
            <Box
                sx={{
                    height: 32,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    px: 1,
                    borderBottom: "1px solid #333",
                    bgcolor: "#1a1a1a",
                    flexShrink: 0
                }}
            >
                <Box sx={{ fontSize: 12 }}>{title}</Box>

                <IconButton
                    size="small"
                    onClick={() => setMaximized(v => !v)}
                    sx={{
                        color: "#888",
                        "& svg": {
                            fontSize: 16   // 🔥 关键：图标变小
                        },
                        "&:hover": {
                            color: "#fff",
                            bgcolor: "rgba(255,255,255,0.08)"
                        }
                    }}
                >
                    {maximized ? (
                        <FullscreenExitIcon fontSize="small" />
                    ) : (
                        <AspectRatioIcon fontSize="small" />
                    )}
                </IconButton>
            </Box>

            {/* ===== Content ===== */}
            <Box sx={{
                flex: 1,
                minHeight: 0,
                height: 0,        // 🔥 关键！！！
                display: "flex",  // 🔥 关键！！
                flexDirection: "column"
            }}>
                {children}
            </Box>
        </Box>
    )
}