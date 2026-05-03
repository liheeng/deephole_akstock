import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, Typography } from '@mui/material';
import { Job, JobStatus } from '../types/job';

interface JobTableProps {
    jobs: Job[];
    selectedJobId: string | null;
    onSelectJob: (jobId: string) => void;
}

export default function JobTable({ jobs, selectedJobId, onSelectJob }: JobTableProps) {
    return (
        <TableContainer>
            <Typography variant="h6" gutterBottom>任务 Job 列表</Typography>
            <Table size="small" stickyHeader>
                <TableHead>
                    <TableRow>
                        <TableCell>ID</TableCell>
                        <TableCell>类型</TableCell>
                        <TableCell>状态</TableCell>
                        <TableCell>开始</TableCell>
                        <TableCell>结束</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {jobs.map(job => (
                        <TableRow
                            key={job.id}
                            hover
                            selected={selectedJobId === job.id}
                            sx={{ cursor: 'pointer' }}
                            onClick={() => onSelectJob(job.id)}
                        >
                            <TableCell>{job.id}</TableCell>
                            <TableCell>{job.type}</TableCell>
                            <TableCell>
                                <Chip
                                    label={job.status}
                                    color={
                                        job.status === JobStatus.SUCCESS ? 'success' :
                                            job.status === JobStatus.FAILED ? 'error' :
                                                job.status === JobStatus.RUNNING ? 'warning' : 'default'
                                    }
                                    size="small"
                                />
                            </TableCell>
                            <TableCell>{job.execute_time || '-'}</TableCell>
                            <TableCell>{job.stop_time || '-'}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </TableContainer>
    );
}