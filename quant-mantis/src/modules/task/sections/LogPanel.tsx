// components/LogPanel.tsx
import React, { useEffect, useRef } from 'react';
import { Paper, Typography } from '@mui/material';
import { useLogStream, type LogLine } from '../../../hooks/useLogStream';

interface LogPanelProps {
    jobId: string | null;
}

export const LogPanel: React.FC<LogPanelProps> = ({ jobId }) => {
    const logs = useLogStream(jobId);
    const containerRef = useRef<HTMLDivElement>(null);

    // 自动滚动到底部
    useEffect(() => {
        if (containerRef.current) {
            containerRef.current.scrollTop = containerRef.current.scrollHeight;
        }
    }, [logs]);

    const renderLine = (line: LogLine, idx: number) => {
        let color = 'inherit';
        if (line.level === 'ERROR') color = 'red';
        else if (line.level === 'DEBUG') color = 'gray';

        return (
            <Typography
                key={idx}
                component="div"
                sx={{ fontFamily: 'monospace', color, whiteSpace: 'pre-wrap' }}
            >
                [{line.timestamp}] [{line.level}] {line.message}
            </Typography>
        );
    };

    return (
        <Paper
            ref={containerRef}
            sx={{
                mt: 2,
                p: 2,
                height: 300,
                overflowY: 'auto',
                backgroundColor: '#1e1e1e',
                textAlign: "left"
            }}
        >
            {logs.length === 0 ? (
                <Typography sx={{ color: 'gray', fontStyle: 'italic' }}>暂无日志</Typography>
            ) : (
                logs.map(renderLine)
            )}
        </Paper>
    );
};