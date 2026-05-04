// hooks/useLogStream.ts
import { useEffect, useState } from 'react';
import { logStream } from '../api/Client';

export interface LogLine {
    timestamp: string;
    level: 'INFO' | 'ERROR' | 'DEBUG';
    message: string;
}

export function useLogStream(jobId: string | null) {
    const [logs, setLogs] = useState<LogLine[]>([]);

    useEffect(() => {
        if (!jobId) {
            setLogs([]);
            return;
        }

        logStream(jobId).then(es => {
            if (es) {
                es.onmessage = (event) => {
                    try {
                        const log: LogLine = JSON.parse(event.data);
                        setLogs(prev => [...prev, log]);
                    } catch (err) {
                        console.error('解析日志失败', err);
                    }
                };

                es.onerror = (err) => {
                    console.error('日志 SSE 错误', err);
                    es.close();
                };

                return () => {
                    es.close();
                    setLogs([]);
                };
            }
        });


    }, [jobId]);

    return logs;
}