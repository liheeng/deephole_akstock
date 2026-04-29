import { useEffect, useRef, useState } from "react";
import { Box } from "@mui/material";
import { Terminal } from "xterm";
import { FitAddon } from "xterm-addon-fit";
import "xterm/css/xterm.css";
import { fetchAPIServiceIp } from "../../api/Client"

export default function WebTerminal({ target }: { target: any[] }) {
    const containerRef = useRef<HTMLDivElement>(null);
    const termRef = useRef<Terminal | null>(null);
    const wsRef = useRef<WebSocket | null>(null);
    const sendQueue = useRef<string[]>([]);
    const mountedRef = useRef(false); // 🔥 关键
    const [apiServiceIp, setApiServiceIp] = useState<any>(null);

    console.log("api_service: ", apiServiceIp)
    
    useEffect(() => {
            fetchAPIServiceIp().then(res => {
                setApiServiceIp(res.server_ip);
            });
        }, []);

    useEffect(() => {
        if (!containerRef.current) return;

        // 🚫 防止 StrictMode 重复执行
        if (mountedRef.current) return;
        mountedRef.current = true;

        const container = containerRef.current;

        const term = new Terminal({
            fontFamily: `"JetBrains Mono", Consolas, monospace`,
            fontSize: 13,
            lineHeight: 1.2,

            cursorBlink: true,
            cursorStyle: "block",

            theme: {
                background: "#1e1e1e",
                foreground: "#d4d4d4",
                cursor: "#ffffff",
                selectionBackground: "#264f78",
            },

            scrollback: 1000,
        });

        const fitAddon = new FitAddon();
        term.loadAddon(fitAddon);

        term.open(container);

        // 👉 延迟 fit（避免 dimensions undefined）
        setTimeout(() => {
            fitAddon.fit();
        }, 0);

        termRef.current = term;

        const ws = new WebSocket(
            `ws://${apiServiceIp}:8000/api/ws/terminal`
        );

        wsRef.current = ws;

        const sendResize = () => {
            if (!termRef.current) return;

            const payload = JSON.stringify({
                type: "resize",
                cols: term.cols,
                rows: term.rows,
            });

            if (ws.readyState === WebSocket.OPEN) {
                ws.send(payload);
            }
        };

        ws.onopen = () => {
            term.write("\r\n[Connected]\r\n");

            // 🔥 关键：发送 target
            ws.send(JSON.stringify({
                type: "init",
                target
            }));

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

        const resizeObserver = new ResizeObserver(() => {
    // Only fit if the element is actually visible
    if (container.offsetWidth > 0 && container.offsetHeight > 0) {
        requestAnimationFrame(() => {
            if (!termRef.current) return;
            try {
                fitAddon.fit();
                sendResize();
            } catch (e) {}
        });
    }
});

        resizeObserver.observe(container);

        return () => {
            mountedRef.current = false;

            resizeObserver.disconnect();

            if (wsRef.current) {
                wsRef.current.close();
                wsRef.current = null;
            }

            if (termRef.current) {
                termRef.current.dispose();
                termRef.current = null;
            }
        };
    }, [target, apiServiceIp]);

    return (
        <Box
            sx={{
                width: "100%",
                height: 600,
                background: "#1e1e1e",
                display: "flex",
                alignItems: "stretch",
                justifyContent: "stretch",
            }}
        >
            <div
                ref={containerRef}
                style={{
                    width: "100%",
                    height: "100%",
                }}
            />
        </Box>
    );
}