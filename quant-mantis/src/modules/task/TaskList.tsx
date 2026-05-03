import { useEffect, useState } from 'react'
import { Button, Stack, Typography } from '@mui/material'
import MainCard from 'components/MainCard'
import TaskTable from 'sections/task/TaskTable'
import { getTasks, runTask } from 'api/task'

export default function TaskList() {
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
        />
      </Stack>
    </MainCard>
  )
}