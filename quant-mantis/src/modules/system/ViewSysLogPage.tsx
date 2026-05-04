import { useEffect, useRef, useState } from "react";
import { Box, Paper, Typography, TextField, Select, MenuItem, Stack, Button } from "@mui/material";
import { fetchDefaultLogs, createSystemLogWebsockChannel } from '../../api/Client';

interface LogLine {
    timestamp: string;
    level: string;
    message: string;
}

export default function ViewSysLogPage() {
    const [logs, setLogs] = useState<LogLine[]>([]);
    const [filterKeyword, setFilterKeyword] = useState("");
    const [filterLevel, setFilterLevel] = useState("");
    const [running, setRunning] = useState(false);

    const wsRef = useRef<WebSocket | null>(null);
    const containerRef = useRef<HTMLDivElement>(null);

    // 初次加载历史日志
    useEffect(() => {
        const loadLogs = async () => {
            const res = await fetchDefaultLogs(200);
            setLogs(res);
        };
        loadLogs();
    }, []);

    // WebSocket 实时日志
    useEffect(() => {
        if (!running) return;

        createSystemLogWebsockChannel().then(ws => {
            wsRef.current = ws;
            if (!ws) return;

            ws.onopen = () => console.log("日志 WebSocket 已连接");
            ws.onmessage = (event) => {
                const data: LogLine = JSON.parse(event.data);
                setLogs(prev => [...prev, data]);
            };
            ws.onclose = () => console.log("日志 WebSocket 关闭");

            return () => {
                ws.close();
                wsRef.current = null;
            };
        });
    }, [running]);

    // 自动滚动
    useEffect(() => {
        if (containerRef.current) {
            containerRef.current.scrollTop = containerRef.current.scrollHeight;
        }
    }, [logs]);

    // 过滤
    const filteredLogs = logs.filter(
        (l) =>
            (!filterLevel || l.level === filterLevel) &&
            (!filterKeyword || l.message.toLowerCase().includes(filterKeyword.toLowerCase()))
    );

    // ====================== 日志级别颜色 ======================
    const getLogColor = (level: string) => {
        const lv = level.toUpperCase();
        if (lv.includes("ERROR")) return "#f44336"; // 红
        if (lv.includes("WARN")) return "#ffeb3b";  // 黄
        if (lv.includes("INFO")) return "#ffffff";  // 白
        if (lv.includes("DEBUG")) return "#9e9e9e"; // 灰
        return "#ffffff";
    };

    return (
        <Box sx={{ display: "flex", flexDirection: "column", height: "100%" }}>
            <Stack direction="row" spacing={1} sx={{ mb: 1 }}>
                <TextField
                    size="small"
                    label="关键字"
                    value={filterKeyword}
                    onChange={(e) => setFilterKeyword(e.target.value)}
                />
                <Select
                    size="small"
                    value={filterLevel}
                    onChange={(e) => setFilterLevel(e.target.value)}
                    displayEmpty
                >
                    <MenuItem value="">全部</MenuItem>
                    <MenuItem value="INFO">INFO</MenuItem>
                    <MenuItem value="ERROR">ERROR</MenuItem>
                    <MenuItem value="DEBUG">DEBUG</MenuItem>
                    <MenuItem value="WARN">WARN</MenuItem>
                </Select>
                <Button
                    variant="contained"
                    color={running ? "error" : "primary"}
                    onClick={() => setRunning(r => !r)}
                >
                    {running ? "停止实时日志" : "开始实时日志"}
                </Button>
            </Stack>

            <Paper
                ref={containerRef}
                sx={{
                    flex: 1,
                    p: 2,
                    overflowY: "auto",
                    backgroundColor: "#1e1e1e",
                    fontFamily: "monospace",
                    whiteSpace: "pre-wrap",
                    textAlign: 'left'
                }}
            >
                {filteredLogs.length === 0 ? (
                    <Typography sx={{ color: "gray", fontStyle: "italic" }}>暂无日志</Typography>
                ) : (
                    filteredLogs.map((line, idx) => (
                        <Typography
                            key={idx}
                            sx={{
                                color: getLogColor(line.level),
                                lineHeight: 1.5
                            }}
                        >
                            [{line.timestamp}] [{line.level}] {line.message}
                        </Typography>
                    ))
                )}
            </Paper>
        </Box>
    );
}