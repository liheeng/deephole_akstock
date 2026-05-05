// ExecutionHistory.tsx
import React, { useState } from 'react';
import {
    Box, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
    Paper, Typography, TablePagination, Chip
} from '@mui/material';

interface ExecutionHistoryProps {
    jobs: any[];
    onSelect: (jobId: string) => void;
    selectedJobId: string | null;
}

export const ExecutionHistory: React.FC<ExecutionHistoryProps> = ({ jobs, onSelect, selectedJobId }) => {
    const [page, setPage] = useState(0);
    const [rowsPerPage, setRowsPerPage] = useState(5);

    return (
        <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
                Script Executor 历史记录
            </Typography>
            <TableContainer component={Paper} sx={{ maxHeight: 240 }}>
                <Table stickyHeader size="small">
                    <TableHead>
                        <TableRow>
                            <TableCell>Job ID</TableCell>
                            <TableCell>Status</TableCell>
                            <TableCell>开始时间</TableCell>
                            <TableCell>结束时间</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {jobs
                            .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                            .map((job) => (
                                <TableRow
                                    key={job.id}
                                    hover
                                    selected={job.id === selectedJobId} // 🔥 高亮选中
                                    sx={{ cursor: 'pointer' }}
                                    onClick={() => onSelect(job.id)}
                                >
                                    <TableCell>{job.id}</TableCell>
                                    <TableCell>
                                        <Chip
                                            label={job.status}
                                            color={
                                                job.status === 'SUCCESS'
                                                    ? 'success'
                                                    : job.status === 'FAILED'
                                                        ? 'error'
                                                        : job.status === 'RUNNING'
                                                            ? 'warning'
                                                            : 'default'
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
            <TablePagination
                component="div"
                count={jobs.length}
                page={page}
                onPageChange={(e, newPage) => {
                    e;
                    setPage(newPage)}
                }
                rowsPerPage={rowsPerPage}
                onRowsPerPageChange={(e) => {
                    setRowsPerPage(parseInt(e.target.value, 10));
                    setPage(0);
                }}
                rowsPerPageOptions={[5, 10, 25]}
            />
        </Box>
    );
};