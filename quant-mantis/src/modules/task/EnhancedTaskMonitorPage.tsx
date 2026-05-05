import { useState, useEffect } from 'react';
import {
  Box,
} from '@mui/material';
import { fetchTasks, fetchJobs } from "../../api/Client";
import { Task } from './types/task'
import { Job } from './types/job'
import { LogPanel } from './sections/LogPanel';
import RunControls from './sections/RunControls';
import TaskTable from './sections/TaskTable';
import JobTable from './sections/JobTable';

export default function EnhancedTaskMonitorPage() {
    const [tasks, setTasks] = useState<Task[]>([]);
    const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
    const [jobs, setJobs] = useState<Job[]>([]);
    const [selectedJobId, setSelectedJobId] = useState<string | null>(null);

    useEffect(() => {
        const loadTasks = async () => {
            const data = await fetchTasks();
            setTasks(data || []);
        };
        loadTasks();
    }, []);

    useEffect(() => {
        if (!selectedTaskId) return;
        const loadJobs = async () => {
            const data = await fetchJobs(selectedTaskId);
            setJobs(data || []);
        };
        loadJobs();
    }, [selectedTaskId]);

    return (
        <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
            <RunControls taskId={selectedTaskId} onRefresh={() => {/* reload tasks/jobs */ }} />
            <Box sx={{ display: 'flex', flex: 1, gap: 1 }}>
                <Box sx={{ flex: 1 }}>
                    <TaskTable
                        tasks={tasks}
                        selectedTaskId={selectedTaskId || undefined}
                        onSelectTask={setSelectedTaskId}
                    />
                </Box>
                <Box sx={{ flex: 1 }}>
                    <JobTable
                        jobs={jobs}
                        selectedJobId={selectedJobId}
                        onSelectJob={setSelectedJobId}
                    />
                </Box>
            </Box>
            <LogPanel jobId={selectedJobId} />
        </Box>
    );
}