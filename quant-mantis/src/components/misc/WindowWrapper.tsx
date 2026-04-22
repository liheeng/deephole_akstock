import { Box, IconButton } from "@mui/material";
import MinimizeIcon from "@mui/icons-material/Remove";
import FullscreenIcon from "@mui/icons-material/Fullscreen";
import FullscreenExitIcon from "@mui/icons-material/FullscreenExit";
import CropSquareIcon from "@mui/icons-material/CropSquare";
import { useState } from "react";
import { createPortal } from "react-dom";

type Props = {
    title: string;
    children: React.ReactNode;
    defaultMode?: "normal" | "min" | "max";
};

export default function WindowWrapper({
    title,
    children,
    defaultMode = "normal",
}: Props) {
    const [mode, setMode] = useState(defaultMode);

    const toggleMin = () => {
        setMode(m => (m === "min" ? "normal" : "min"));
    };

    const toggleMax = () => {
        setMode(m => (m === "max" ? "normal" : "max"));
    };

    const content = (
        <Box
            sx={{
                width: "100%",
                height: "100%",
                display: "flex",
                flexDirection: "column",
                bgcolor: "background.paper",
                overflow: "hidden",
            }}
        >
            {/* HEADER */}
            <Box
                sx={{
                    height: 36,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    px: 1,
                    borderBottom: "1px solid",
                    borderColor: "divider",
                    flexShrink: 0,
                }}
            >
                <Box>{title}</Box>

                <Box>
                    {/* minimize */}
                    <IconButton
                        size="small"
                        onClick={toggleMin}
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
                        <MinimizeIcon fontSize="small" />
                    </IconButton>

                    {/* max */}
                    <IconButton
                        size="small"
                        onClick={toggleMax}
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
                        {mode === "max" ? (
                            <FullscreenExitIcon fontSize="small" />
                        ) : (
                            <FullscreenIcon fontSize="small" />
                        )}
                    </IconButton>
                </Box>
            </Box>

            {/* BODY */}
            {mode !== "min" && (
                <Box sx={{ flex: 1, minHeight: 0 }}>
                    {children}
                </Box>
            )}
        </Box>
    );

    // ===== MAX MODE =====
    if (mode === "max") {
        return createPortal(
            <Box
                sx={{
                    position: "fixed",
                    inset: 0,
                    zIndex: 9999,
                    bgcolor: "background.paper",
                }}
            >
                {content}
            </Box>,
            document.body
        );
    }

    // ===== MIN MODE =====
    if (mode === "min") {
        return (
            <Box
                sx={{
                    height: 36,
                    display: "flex",
                    alignItems: "center",
                    px: 1,
                    border: "1px solid",
                    borderColor: "divider",
                    bgcolor: "background.paper",
                }}
            >
                <Box sx={{ flex: 1 }}>{title}</Box>

                <IconButton size="small" onClick={() => setMode("normal")}>
                    <CropSquareIcon fontSize="small" />
                </IconButton>
            </Box>
        );
    }

    return content;
}