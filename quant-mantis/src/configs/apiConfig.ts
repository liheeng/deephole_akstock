export const API_URL_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';
export const CONTENT_TYPE = 'application/json'
export const CORS_CONFIG = {
    allow_credentials: true,
    allow_origins: ["*"],
    allow_methods: ["*"],
    allow_headers: ["*"]
}