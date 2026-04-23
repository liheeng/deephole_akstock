import { create } from "zustand";

export type MessageType = "info" | "success" | "warning" | "error";

export interface MessageItem {
    id: string;
    type: MessageType;
    content: string;
    timestamp: number;
    isGray: boolean; // 标记是否变灰
}

interface MessageStore {
    current: MessageItem | null;
    history: MessageItem[];
    addMessage: (type: MessageType, content: string) => void;
    clearCurrent: () => void;
    clearHistory: () => void;
    markCurrentAsGray: () => void; // 把当前消息变灰
}

export const useMessageStore = create<MessageStore>((set, get) => ({
    current: null,
    history: [],

    addMessage: (type, content) => {
        const newMsg: MessageItem = {
            id: Date.now().toString(),
            type,
            content,
            timestamp: Date.now(),
            isGray: false
        };

        set({
            current: newMsg
        });
        set({
            history: [get().current, ...get().history].filter(Boolean) as MessageItem[]
        });
        // 这里原来的自动消失代码已经全部删掉
        // 5 秒后把当前这条消息变灰
        setTimeout(() => {
            if (get().current?.id === newMsg.id) {
                get().markCurrentAsGray();
            }
        }, 5000);
    },

    markCurrentAsGray: () =>
        set((state) => ({
            current: state.current ? { ...state.current, isGray: true } : null,
        })),

    clearCurrent: () => set({ current: null }),
    clearHistory: () => set({ history: [] }),
}));