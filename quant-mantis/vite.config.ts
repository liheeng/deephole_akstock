import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    base: "/",
    plugins: [
        react(),
    ],
    server: {
        allowedHosts: [
            'sowow.online', // 只放行这个域名，最安全
            'localhost',
            '192.168.50.12'
        ],
        proxy: {
            // --------------------------
            // 👇 WebSocket 专属配置（唯一必加的核心）
            // --------------------------
            '/api/ws': {
                // target: 'ws://192.168.50.12:8000', // 例：ws://192.168.1.100:8080
                target: 'ws://localhost:8000',
                ws: true, // ✅ 开启WebSocket代理（关键！不加就无法穿透）
                changeOrigin: true,
            },
            // 所有 /api 开头的请求，自动转发到内网API
            '/api': {
                // target: 'http://192.168.50.12:8000', // 比如 http://192.168.1.100:8080
                target: 'http://localhost:8000',
                changeOrigin: true,
                // 如果你不需要重写路径就删掉 rewrite
                // rewrite: (path) => path.replace(/^\/api/, '')
            }
        }
    }
})