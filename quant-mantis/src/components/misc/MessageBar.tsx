import {
    Box,
    Button,
    Typography,
    Dialog,
    DialogTitle,
    DialogContent,
    List,
    ListItem,
    ListItemText,
} from "@mui/material";
import { useMessageStore } from "../../store/message.store";
import { useState } from "react";

const typeColor = {
    info: "#2196f3",
    success: "#4caf50",
    warning: "#ff9800",
    error: "#f44336",
};

// ✅ 消息类型对应表情
const typeIcon = {
    info: "ℹ️",
    success: "✅",
    warning: "⚠️",
    error: "❌",
};

export default function MessageBar() {
    const { current, history } = useMessageStore();
    const [open, setOpen] = useState(false);

    if (!current && history.length === 0) return null;

    return (
        <Box
            sx={{
                display: "flex",
                alignItems: "center",
                gap: 1.5,
                flexShrink: 0,
                // 边框样式
                border: "1px solid #ddd",
                borderRadius: "4px",
                px: 1.5,
                py: 0.5,
                backgroundColor: "#020202",
                flex: 1,
                minWidth: 0
            }}
        >
            {/* 查看历史按钮 */}
            <Button
                size="small"
                variant="outlined"
                onClick={() => setOpen(true)}
                sx={{ height: 28, minWidth: "auto", fontSize: 12 }}
            >
                View
            </Button>

            {/* 当前消息 + 表情 */}
            {current && (
                <Typography
                    sx={{
                        whiteSpace: "nowrap",
                        // maxWidth: 00,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        color: current.isGray ? "#999" : typeColor[current.type],
                        fontWeight: 500,
                        display: "flex",
                        alignItems: "center",
                        gap: 0.5,
                    }}
                >
                    {typeIcon[current.type]} {current.content}
                </Typography>
            )}

            {/* 历史弹窗 */}
            <Dialog open={open} onClose={() => setOpen(false)} maxWidth="sm" fullWidth>
                <DialogTitle>Message History</DialogTitle>
                <DialogContent>
                    <List>
                        {history.length === 0 ? (
                            <Typography>No messages</Typography>
                        ) : (
                            history.map((msg) => (
                                <ListItem key={msg.id} divider>
                                    <ListItemText
                                        primary={
                                            <Typography
                                                sx={{
                                                    color: msg.isGray ? "#999" : typeColor[msg.type],
                                                    display: "flex",
                                                    alignItems: "center",
                                                    gap: 0.5,
                                                }}
                                            >
                                                {typeIcon[msg.type]} {msg.content}
                                            </Typography>
                                        }
                                        secondary={new Date(msg.timestamp).toLocaleString()}
                                    />
                                </ListItem>
                            ))
                        )}
                    </List>
                </DialogContent>
            </Dialog>
        </Box>
    );
}