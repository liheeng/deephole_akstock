import { Box, IconButton } from "@mui/material"
import { Fullscreen, FullscreenExit } from "@mui/icons-material"
import { useEffect } from "react"

// interface FullscreenBoxProps {
//     children: React.ReactNode
//     isFull: boolean
//     onToggle?: () => void
//     sx?: any
// }

export const FullScreenBox = ({ children, isFull=false, onToggle, sx }: any) => {
    
    useEffect(() => {
        const handleEsc = (e: KeyboardEvent) => {
            // 只有当当前处于全屏状态且按下的是 Escape 键时，才触发关闭
            if (e.key === 'Escape' && isFull) {
                onToggle();
            }
        }

        // 全局监听键盘事件
        window.addEventListener('keydown', handleEsc);
        
        // 清理函数：组件卸载时移除监听
        return () => window.removeEventListener('keydown', handleEsc);
    }, [isFull, onToggle]); // 依赖项包含 isFull 和 onToggle

    return (
        <Box
            sx={{
                position: isFull ? "fixed" : "relative",
                top: isFull ? 0 : "unset",
                left: isFull ? 0 : "unset",
                width: isFull ? "100vw" : "100%",
                height: isFull ? "100vh" : "100%",
                zIndex: isFull ? 9999 : 1, 
                bgcolor: "background.paper",
                display: "flex",
                flexDirection: "column",
                boxSizing: "border-box",
                transition: "width 0.1s, height 0.1s",
                ...sx
            }}
        >
            <IconButton
                onClick={(e) => {
                    e.stopPropagation();
                    onToggle();
                }}
                sx={{ 
                    position: "absolute", 
                    top: 0, 
                    right: 0, 
                    zIndex: 10000, 
                    bgcolor: "rgba(95, 93, 93, 0.4)",
                    color: "#7d7c7cd8",
                    // "&:hover": { bgcolor: "rgba(0,0,0,0.6)" },
                    "&:hover": {
                                color: "#4003f5",
                                bgcolor: "rgba(249, 247, 247, 0.92)"
                            },
                    "& svg": {
                                fontSize: 18   // 🔥 关键：图标变小
                            },
                }}
                size="small"
            >
                {isFull ? <FullscreenExit /> : <Fullscreen />}
            </IconButton>
            
            <Box sx={{ flex: 1, minHeight: 0, width: "100%" }}>
                {children}
            </Box>
        </Box>
    )
}