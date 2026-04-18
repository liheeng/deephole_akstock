// src/api/client.ts
import axios from 'axios';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const apiClient = axios.create({
  baseURL: API_BASE,
});

// 2. 跨域关键：允许携带 cookie / token 凭证
apiClient.defaults.withCredentials = true;

// 3. 通用请求头（可选，但建议加）
apiClient.defaults.headers.common['Content-Type'] = 'application/json';

export interface Task {
  id: string;
  status: 'CREATED' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'PARTIAL_SUCCESS';
  description: string;
  start_time: string;
  stop_time: string;
  jobs: Job[];
}

export interface Job {
  id: string;
  name: string;
  status: string;
  job_type: string;
}