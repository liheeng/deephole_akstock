// pages/stock/ScriptExecutorPage.tsx
import { useState, useEffect } from 'react';
import { Box, Paper } from '@mui/material';
import ScriptEditor from './sections/ScriptEditor';
import { LogPanel } from './sections/LogPanel';
import { ExecutionHistory } from './sections/ExecutionHistory';
import { useLogStream, type LogLine } from '../../hooks/useLogStream';
import { fetchScriptExecutorJobs, executeScriptJob, cancelScriptJob } from '../../api/Client';

export default function ScriptExecutorPage() {
    const [script, setScript] = useState<string>('');
    const [jobId, setJobId] = useState<string | null>(null);
    const [jobType, setJobType] = useState<string | null>(null);
    const [status, setStatus] = useState<'IDLE' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'STOP' | 'CANCEL'>('IDLE');
    const [logs, setLogs] = useState<LogLine[]>([]);
    const [jobs, setJobs] = useState<any[]>([]);

    // 订阅日志
    const streamedLogs = useLogStream(jobId || '');

    useEffect(() => {
        setLogs(streamedLogs);
    }, [streamedLogs]);

    // 加载历史 Job
    useEffect(() => {
        const loadJobs = async () => {
            const allJobs = await fetchScriptExecutorJobs();
            setJobs(allJobs || []);
        };

        loadJobs();
    }, [jobId]);

    return (
        <Box
            sx={{
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
                gap: 1,
                overflow: 'hidden', // 防止整体撑开
            }}
        >
            {/* ✅ 编辑器（唯一自适应区域） */}
            <Box sx={{ flex: 1, minHeight: 0 }}>
                <Paper sx={{ height: '100%', p: 1 }}>
                    <ScriptEditor
                        value={script}
                        onChange={setScript}
                        onRun={() => {
                            executeScriptJob(script).then(res => {
                                if (res) {
                                    setJobId(res.jobId);
                                    setJobType(res.jobType);
                                    setStatus('RUNNING');
                                    setLogs([]);
                                }
                            });
                        }}
                        onStop={() => {
                            if (jobId && jobType) {
                                cancelScriptJob(jobId, jobType).then(res => {
                                    if (res) {
                                        setStatus('STOP');
                                    }
                                });
                            }
                        }}
                    />
                </Paper>
            </Box>

            {/* ✅ 日志区域（固定高度 + 滚动） */}
            <Box sx={{ height: 250, flexShrink: 0 }}>
                <Paper
                    sx={{
                        height: '100%',
                        p: 1,
                        overflow: 'auto',
                        display: 'flex',
                        flexDirection: 'column',
                    }}
                >
                    <LogPanel jobId={jobId} logs={logs} />
                </Paper>
            </Box>

            {/* ✅ 历史执行记录（固定高度 + 滚动） */}
            <Box sx={{ height: 180, flexShrink: 0 }}>
                <Paper
                    sx={{
                        height: '100%',
                        p: 1,
                        overflow: 'auto',
                    }}
                >
                    <ExecutionHistory
                        jobs={jobs}
                        onSelect={(id) => setJobId(id)}
                        selectedJobId={jobId}
                    />
                </Paper>
            </Box>
        </Box>
    );
}