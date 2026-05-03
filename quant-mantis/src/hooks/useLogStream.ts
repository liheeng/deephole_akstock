import { useEffect, useState } from 'react'

export default function useLogStream(jobId: string) {
  const [logs, setLogs] = useState<any[]>([])

  useEffect(() => {
    const ws = new WebSocket(`ws://localhost:8000/ws/logs?job_id=${jobId}`)

    ws.onmessage = (e) => {
      const log = JSON.parse(e.data)
      setLogs((prev) => [...prev, log])
    }

    return () => ws.close()
  }, [jobId])

  return logs
}