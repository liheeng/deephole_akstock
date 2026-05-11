import { getCurrentDomain } from '../utils/browserUtil';
export const API_URL_BASE = `${getCurrentDomain()}` || 'http://localhost:8000';
// 获取当前域名并将协议替换为 ws/wss
export const WS_API_URL_BASE = getCurrentDomain()?.replace(/^http/, 'ws') || 'ws://localhost:8000';
// export const API_URL_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';
export const CONTENT_TYPE = 'application/json'
export const CORS_CONFIG = {
    allow_credentials: true,
    allow_origins: ["*"],
    allow_methods: ["*"],
    allow_headers: ["*"]
}