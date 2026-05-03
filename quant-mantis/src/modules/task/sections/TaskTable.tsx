// TaskTable.tsx
import { Table, TableHead, TableBody, TableRow, TableCell, Chip } from '@mui/material'
import { TaskStatus } from '../types/task'

interface TaskTableProps {
    tasks: any[]
    selectedTaskId?: string
    onSelectTask: (taskId: string) => void
    onRun?: (taskId: string) => void
}

export default function TaskTable({ tasks, selectedTaskId, onSelectTask, onRun }: TaskTableProps) {
    return (
        <Table size="small">
            <TableHead>
                <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Description</TableCell>
                    <TableCell>Status</TableCell>
                    {onRun && <TableCell>Action</TableCell>}
                </TableRow>
            </TableHead>
            <TableBody>
                {tasks.map(task => (
                    <TableRow
                        key={task.id}
                        hover
                        selected={selectedTaskId === task.id}
                        sx={{ cursor: 'pointer' }}
                        onClick={() => onSelectTask(task.id)}
                    >
                        <TableCell>{task.id}</TableCell>
                        <TableCell>{task.description}</TableCell>
                        <TableCell>
                            <Chip
                                label={task.status}
                                color={
                                    task.status === TaskStatus.SUCCESS ? 'success' :
                                        task.status === TaskStatus.FAILED ? 'error' :
                                            task.status === TaskStatus.RUNNING ? 'warning' : 'default'
                                }
                                size="small"
                            />
                        </TableCell>
                        {onRun && (
                            <TableCell>
                                <button onClick={e => { e.stopPropagation(); onRun(task.id) }}>Run</button>
                            </TableCell>
                        )}
                    </TableRow>
                ))}
            </TableBody>
        </Table>
    )
}