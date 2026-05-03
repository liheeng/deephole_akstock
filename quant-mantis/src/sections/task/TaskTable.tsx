import {
  Table, TableHead, TableRow, TableCell,
  TableBody, Button, Chip, Stack
} from '@mui/material'
import { useNavigate } from 'react-router-dom'

const statusColor: any = {
  SUCCESS: 'success',
  FAILED: 'error',
  RUNNING: 'warning',
  CREATED: 'default'
}

export default function TaskTable({ tasks, onRun }: any) {
  const nav = useNavigate()

  return (
    <Table>
      <TableHead>
        <TableRow>
          <TableCell>ID</TableCell>
          <TableCell>Status</TableCell>
          <TableCell>Action</TableCell>
        </TableRow>
      </TableHead>

      <TableBody>
        {tasks.map((t: any) => (
          <TableRow key={t.id}>
            <TableCell>{t.id}</TableCell>

            <TableCell>
              <Chip label={t.status} color={statusColor[t.status]} />
            </TableCell>

            <TableCell>
              <Stack direction="row" spacing={1}>
                <Button onClick={() => nav(`/task/${t.id}`)}>View</Button>
                <Button onClick={() => onRun(t.id)}>Run</Button>
              </Stack>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}