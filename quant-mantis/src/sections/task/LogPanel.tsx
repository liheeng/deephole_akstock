import { useEffect, useState, useRef } from 'react'
import { Box } from '@mui/material'
import useLogStream from 'hooks/useLogStream'

export default function LogPanel({ jobId }: { jobId: string }) {
  const logs = useLogStream(jobId)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    ref.current?.scrollTo(0, ref.current.scrollHeight)
  }, [logs])

  return (
    <Box
      ref={ref}
      sx={{
        height: 400,
        overflow: 'auto',
        bgcolor: '#000',
        color: '#0f0',
        p: 1,
        fontFamily: 'monospace'
      }}
    >
      {logs.map((l: any, i: number) => (
        <div key={i}>
          [{l.level}] {l.message}
        </div>
      ))}
    </Box>
  )
}