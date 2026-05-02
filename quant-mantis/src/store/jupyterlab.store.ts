import { create } from "zustand";

// 1. 修复拼写：JupyterStore
interface JupyterLabStore {
    processId: string | number | undefined;
    startTime: string | undefined;
    status: string;
    url: string | undefined;

    // 2. 修复拼写：setJupyter
    setJupyter: (processId: string | number, startTime: string, url: string, status: string) => void;
    updateStatus: (processId: string | number, newStatus: string) => void;
    clearJupyter: () => void;
}

export const useJupyterLabStore = create<JupyterLabStore>((set) => ({
    processId: undefined,
    startTime: undefined,
    status: 'not_running',
    url: undefined,

    // 设置完整信息
    setJupyter: (processId, startTime, url, status) => {
        set({
            processId,
            startTime,
            status,
            url,
        });
    },

    // 3. 修复逻辑：pid 匹配才更新状态！！！
    updateStatus: (pid, newStatus) => {
        set((state) => ({
            status: state.processId === pid ? newStatus : state.status,
        }));
    },

    // 额外加一个清空方法（停止时用）
    clearJupyter: () => set({
        processId: undefined,
        startTime: undefined,
        status: 'not_running',
        url: undefined
    })
}));