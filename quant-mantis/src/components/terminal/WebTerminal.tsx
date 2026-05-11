import { useEffect, useRef, useState } from "react";
import { Box } from "@mui/material";
import { Terminal } from "xterm";
import { FitAddon } from "xterm-addon-fit";
import "xterm/css/xterm.css";
import { fetchAPIServiceIp } from "../../api/Client"
import { WS_API_URL_BASE } from '../../configs/apiConfig'
interface Props {
    target: any;
    fontSize: number;
    theme: any;
}

export default function WebTerminal({ target, fontSize, theme }: Props) {
    const containerRef = useRef<HTMLDivElement>(null);
    const termRef = useRef<Terminal | null>(null);
    const fitAddonRef = useRef<FitAddon | null>(null);
    const wsRef = useRef<WebSocket | null>(null);
    const sendQueue = useRef<string[]>([]);
    const mountedRef = useRef(false);
    const [apiServiceIp, setApiServiceIp] = useState<any>(null);

    useEffect(() => {
        fetchAPIServiceIp().then(res => {
            setApiServiceIp(res.server_ip);
        });
    }, []);

    // 监听字号/主题变化，实时生效
    useEffect(() => {
        if (!termRef.current || !fitAddonRef.current) return;
        const term = termRef.current;
        term.options.fontSize = fontSize;
        term.options.theme = theme;

        setTimeout(() => {
            fitAddonRef.current?.fit();
        }, 30);
    }, [fontSize, theme]);

    useEffect(() => {
        if (!containerRef.current || !apiServiceIp) return;
        if (mountedRef.current) return;
        mountedRef.current = true;

        const container = containerRef.current;

        const term = new Terminal({
            fontFamily: `"JetBrains Mono", Consolas, monospace`,
            fontSize: fontSize,
            lineHeight: 1.2,
            cursorBlink: true,
            cursorStyle: "block",
            theme: theme,
            scrollback: 1000,
        });

        const fitAddon = new FitAddon();
        fitAddonRef.current = fitAddon;
        term.loadAddon(fitAddon);
        term.open(container);

        setTimeout(() => {
            fitAddon.fit();
        }, 0);

        termRef.current = term;

        const ws = new WebSocket(
            `${WS_API_URL_BASE}/api/ws/terminal`
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
            if (container.offsetWidth > 0 && container.offsetHeight > 0) {
                requestAnimationFrame(() => {
                    if (!termRef.current) return;
                    try {
                        fitAddon.fit();
                        sendResize();
                    } catch (e) { }
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
                height: "100%",
                background: "#1e1e1e",
                display: "flex",
                alignItems: "stretch",
                justifyContent: "stretch"
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