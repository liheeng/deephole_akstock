// src/api/Client.ts
import axios from 'axios';
// Init nodes
import { NodeRegistry } from "../model/dsl_node/node_registry";
import { useNodes } from "../hooks/useNodes"

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

export async function initRegisteredNodes() {
  try {
    const response = await apiClient.get('/nodes', { withCredentials: true });
    if (response.status !== 200) return [];
    const nodes = response.data;
    NodeRegistry.fromDict(nodes);
  } catch (err: any) {
    NodeRegistry.fromDict(useNodes());
  }
}

export async function callBacktest(payload: any) {
  try {
    // 1. 确保是 JSON 对象
    const jsonPayload = typeof payload === 'string' ? JSON.parse(payload) : payload;

    // 2. 等待请求完成
    const res = await apiClient.post("/backtest", jsonPayload, {
      withCredentials: true,
      headers: {
        "Content-Type": "application/json",
      },
    });

    // 3. 成功 → 返回数据
    return res.data;

  } catch (err) {
    // 4. 失败 → 返回 null
    console.error("回测失败", err);
    return null;
  }
}
