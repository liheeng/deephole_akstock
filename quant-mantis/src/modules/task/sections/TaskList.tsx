import { useEffect, useState } from 'react'
import { Stack, Typography } from '@mui/material'
import MainCard from '../../../components/visual/MainCard'
import TaskTable from './TaskTable'
import { Task } from '../types/task'
import { getTasks, runTask } from '../../../api/task'

interface TaskListProps {
    onSelectTask: (task: Task) => void
}

export default function TaskList({onSelectTask}: TaskListProps) {
    const [tasks, setTasks] = useState<any[]>([])

    const load = async () => {
        const res = await getTasks()
        setTasks(res)
    }

    useEffect(() => {
        load()
    }, [])

    return (
        <MainCard>
            <Stack spacing={2}>
                <Typography variant="h4">Task List</Typography>

                <TaskTable
                    tasks={tasks}
                    onRun={async (id: string) => {
                        await runTask(id)
                        load()
                    }}
                    onSelectTask={(taskId: string) => {
                            const task = tasks.filter((task: Task) => task.id === taskId);
                            onSelectTask(task[0]);
                        }
                    }
                />
            </Stack>
        </MainCard>
    )
}