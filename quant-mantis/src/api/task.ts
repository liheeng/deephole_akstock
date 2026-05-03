import { apiClient } from './Client'

export const getTasks = () => apiClient.get('/tasks', { withCredentials: true }).then(res => res.data)

export const getTaskDetail = (id: string) =>
    apiClient.get(`/tasks/${id}`, { withCredentials: true }).then(res => res.data)

export const runTask = (id: string) =>
    apiClient.post(`/tasks/${id}/run`, { withCredentials: true })

export const cancelTask = (id: string) =>
    apiClient.post(`/tasks/${id}/cancel`, { withCredentials: true })