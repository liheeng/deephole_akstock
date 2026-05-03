import { Button, Stack } from '@mui/material'
import { runTask, cancelTask } from '../../../api/task'

export default function RunControls({ taskId, onRefresh }: any) {
    return (
        <Stack direction="row" spacing={2} sx={{ mb: 2 }}>
            <Button
                variant="contained"
                onClick={async () => {
                    await runTask(taskId)
                    onRefresh()
                }}
            >
                Run
            </Button>

            <Button
                color="error"
                onClick={async () => {
                    await cancelTask(taskId)
                }}
            >
                Cancel
            </Button>
        </Stack>
    )
}