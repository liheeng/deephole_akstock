import { useParams } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { Grid, Typography } from '@mui/material'
import MainCard from '../../../components/visual/MainCard'
import { LogPanel } from './LogPanel'
import RunControls from './RunControls'
import { getTaskDetail } from '../../../api/task'

export default function TaskDetail() {
    const { id } = useParams()
    const [task, setTask] = useState<any>(null)

    const load = async () => {
        const res = await getTaskDetail(id!)
        setTask(res)
    }

    useEffect(() => {
        load()
    }, [id])

    if (!task) return null

    return (
        <MainCard>
            <Typography variant="h4">Task: {task.id}</Typography>

            <RunControls taskId={task.id} onRefresh={load} />

            <Grid container spacing={2}>
                <Grid item sx={{xs:12}}>
                    <LogPanel jobId={task.id} />
                </Grid>
            </Grid>
        </MainCard>
    )
}