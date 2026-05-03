// pages/stock/ScriptExecutorPage.tsx
import { useState, useEffect } from 'react';
import { Box, Grid, Paper } from '@mui/material';
import ScriptEditor from './sections/ScriptEditor';
import { LogPanel } from './sections/LogPanel';
import RunControls from './sections/RunControls';
import { ExecutionHistory } from './sections/ExecutionHistory';
import { useLogStream, type LogLine } from '../../hooks/useLogStream';
import { fetchScriptExecutorJobs } from '../../api/Client';

export default function ScriptExecutorPage() {
    const [script, setScript] = useState<string>('');
    const [jobId, setJobId] = useState<string | null>(null);
    const [status, setStatus] = useState<'IDLE' | 'RUNNING' | 'SUCCESS' | 'FAILED'>('IDLE');
    const [logs, setLogs] = useState<LogLine[]>([]);
    const [jobs, setJobs] = useState<any[]>([]); // ExecutionHistory 数据

    // 订阅日志
    const streamedLogs = useLogStream(jobId || '');

    useEffect(() => {
        setLogs(streamedLogs);
    }, [streamedLogs]);

    // 加载历史 Job
    useEffect(() => {
        const loadJobs = async () => {
            const allJobs = await fetchScriptExecutorJobs()
            setJobs(allJobs);
        };

        loadJobs();
    }, [jobId]); // 新 Job 完成后刷新历史

    return (
        <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 1 }}>
            {/* Run Controls */}
            <RunControls
                status={status}
                script={script}
                onRun={(newJobId: string) => {
                    setJobId(newJobId);
                    setStatus('RUNNING');
                    setLogs([]);
                }}
                onStop={() => setStatus('IDLE')}
            />

            {/* 主体区域 - 上下布局 */}
            <Grid container spacing={1} sx={{ flexGrow: 1, flexDirection: 'column', height: '100%' }}>
                <Grid item xs={12} sx={{ flex: 1 }}>
                    <Paper sx={{ height: '100%', p: 1 }}>
                        <ScriptEditor value={script} onChange={setScript} />
                    </Paper>
                </Grid>

                <Grid item xs={12} sx={{ flex: 1, mt: 1 }}>
                    <Paper sx={{ height: '100%', p: 1 }}>
                        <LogPanel jobId={jobId} />
                    </Paper>
                </Grid>
            </Grid>

            {/* 历史执行记录 */}
            <Box sx={{ mt: 1, flexShrink: 0 }}>
                <ExecutionHistory
                    jobs={jobs}             // 从上层 state 传入
                    onSelect={(id) => setJobId(id)}  // 选中 Job 后更新 jobId，LogPanel 自动订阅
                    selectedJobId={jobId}   // 高亮当前选中 Job
                />
            </Box>
        </Box>
    );
}