import { useEffect, useRef } from "react";
import { Box } from "@mui/material";
import { Terminal } from "xterm";
import { FitAddon } from "xterm-addon-fit";
import "xterm/css/xterm.css";

export default function WebTerminal({ target }: { target: string }) {
    const ref = useRef<HTMLDivElement>(null);
    const socketRef = useRef<WebSocket | null>(null);
    const termRef = useRef<Terminal | null>(null);
    const sendQueue = useRef<string[]>([]);

    useEffect(() => {
        if (!ref.current) return;
        if (termRef.current) return;

        const term = new Terminal({
            cursorBlink: true,
            fontSize: 14,
        });

        const fitAddon = new FitAddon();
        term.loadAddon(fitAddon); // 🔥 必须

        term.open(ref.current);
        fitAddon.fit();

        termRef.current = term;

        const ws = new WebSocket(
            `ws://localhost:8000/ws/terminal?target=${target}`
        );

        socketRef.current = ws;

        const sendResize = () => {
            const payload = JSON.stringify({
                type: "resize",
                cols: term.cols,
                rows: term.rows
            });

            if (ws.readyState === WebSocket.OPEN) {
                ws.send(payload);
            } else {
                sendQueue.current.push(payload);
            }
        };

        const handleResize = () => {
            fitAddon.fit();
            sendResize();
        };

        window.addEventListener("resize", handleResize);

        ws.onopen = () => {
            term.write("\r\n[Connected]\r\n");

            // 🔥 正确时机
            sendResize();

            while (sendQueue.current.length > 0) {
                ws.send(sendQueue.current.shift()!);
            }
        };

        ws.onmessage = (e) => {
            term.write(e.data);
        };

        ws.onclose = () => {
            term.write("\r\n[Disconnected]\r\n");
        };

        term.onData((data) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(data);
            } else {
                sendQueue.current.push(data);
            }
        });

        term.onResize(({ cols, rows }) => {
            const payload = JSON.stringify({ type: "resize", cols, rows });

            if (ws.readyState === WebSocket.OPEN) {
                ws.send(payload);
            }
        });

        return () => {
            window.removeEventListener("resize", handleResize);
            ws.close();
            term.dispose();
            socketRef.current = null;
            termRef.current = null;
        };
    }, [target]);

    return (
        <Box
            sx={{
                width: "100%",
                height: 600,
                display: "flex",
                justifyContent: "flex-start", // 🔥 左对齐
                alignItems: "flex-start",
            }}
        > <div ref={ref}
            style={{
                width: "100%",
                height: "100%",
            }} />
        </Box>
    )

}