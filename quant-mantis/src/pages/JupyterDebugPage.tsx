import { useEffect, useRef, useState } from 'react'
import { Box, CircularProgress, Typography } from '@mui/material'
import { startJupyterLab } from '../api/Client'
import { useJupyterLabStore } from '../store/jupyterlab.store'
import { FullScreenBox } from "../components/misc//FullScreenBox"

// const JUPYTER_API = 'http://localhost:9000'

export default function JupyterDebugPage() {
    // const [jupyterUrl, setJupyterUrl] = useState<string>('')
    const jupyterUrl = useJupyterLabStore(s => s.url)
    const jupyterStatus = useJupyterLabStore(s => s.status)
    const setJupyter = useJupyterLabStore(s => s.setJupyter)
    // const updateJupyterStatus = useJupyterStore(s => s.updateStatus)
    const [loading, setLoading] = useState(true)
    const isMounted = useRef(true)
    const [fullSection, setFullSection] = useState<string | null>(null);

    // 启动 Jupyter
    const startJupyter = async () => {
        try {
            const data = await startJupyterLab()
            if (data) {
                setJupyter(data.processId, Date.now().toString(), data.url, data.status)
                setLoading(false)
                console.log(`Jupyter started, process id: ${data.process_id}, url: ${data.url}`)
            }
        } catch (err) {
            console.error('启动失败', err)
        }
    }

    // 页面进入：启动
    useEffect(() => {
        isMounted.current = true
        if (!jupyterUrl
            ||
            (jupyterStatus !== "started" && jupyterStatus !== "running")) {
            startJupyter()
        } else {
            setLoading(false)
        }

        // 页面离开/卸载：自动停止 ✅
        return () => {
            isMounted.current = false
            // stopJupyter()
        }
    }, [jupyterUrl])

    return (
        <FullScreenBox
            isFull={fullSection === 'jupyterlab'}
            onToggle={() => setFullSection(fullSection === 'jupyterlab' ? null : 'jupyterlab')}
            sx={{ height: "100%", flex: 1, minHeight: 0, minWidth: 0 }}
        >
            <Box sx={{
                flex: 1, // 填充剩余宽度
                height: "100%",
                display: "flex",
                flexDirection: "column",
                minWidth: 0,
                minHeight: 0
            }}>
                {/* <Box sx={{ p: 3, height: 'calc(100vh - 100px)' }}> */}
                <Typography sx={{ ariant: "h6", mb: 2 }}>
                    Python 在线调试器（JupyterLab）
                </Typography>

                {loading && (
                    <Box style={{ justifyContent: 'center' }} sx={{ display: "flex", mt: 10 }}>
                        <CircularProgress />
                    </Box>
                )}

                {jupyterUrl && !loading && (
                    <Box sx={{ width: '100%', height: '100%', border: '1px solid #ddd' }}>
                        <iframe
                            src={jupyterUrl}
                            width="100%"
                            height="100%"
                            frameBorder={0}
                            allowFullScreen
                        />
                    </Box>
                )}
            </Box>
        </FullScreenBox>
    )
}