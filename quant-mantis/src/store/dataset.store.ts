import { create } from "zustand"
import { nanoid } from "nanoid"
import { SimpleCheckResult, type CheckResult } from "../common/Types"

// ==============================
// 工具函数
// ==============================
export function getDefaultPresetDateRange(): { start: string; end: string } {
    const today = new Date()
    const end = today.toISOString().split("T")[0]!
    const start = `${today.getFullYear() - 3}-01-01`
    return { start, end }
}

// ==============================
// 类型定义
// ==============================
export type Filter =
    | { field: "market"; op: "in"; value: string[] }
    | { field: "symbol"; op: "in"; value: string[] }
    | { field: "sector"; op: "in"; value: string[] }
    | { field: "universe"; op: "in"; value: string }
    | { field: "date"; op: "between"; value: [string, string] }

export type Preset = {
    type: "preset" | "sql"
    markets?: string[]
    symbols?: string[]
    sectors?: string[]
    universe?: string
    start: string
    end: string
    sql?: string
}

export type BacktestDataSourceDef =
    | {
        name: string
        type: "sql"
        sql: string
        schema?: string[]
    }
    | Preset
    | {
        type: "filters"
        filters: Filter[]
    }

export interface Dataset {
    id: string
    name: string
    createdAt: string
    sourceDef: BacktestDataSourceDef
    schema?: string[]
    rowCount?: number
    cache?: {
        status: 'ready' | 'running' | 'error'
        tableName?: string
    }
}

interface DatasetState {
    datasets: Dataset[]
    originalDatasets: Record<string, Dataset>
    currentDatasetId?: string

    createDataset: (ds: BacktestDataSourceDef, schema?: string[]) => Dataset
    getDatasetName: (id?: string) => string
    setDatasetName: (id: string | undefined, name: string) => void
    getDataset: (id?: string) => Dataset | undefined
    updateDataset: (dataset: Dataset) => void
    updateSourceDef: (id: string | undefined, sourceDef: BacktestDataSourceDef) => Dataset | undefined
    setCurrentDataset: (id: string) => void

    // ⭐ 核心
    setOriginalDataset: (ds: Dataset) => void
    isDatasetDirty: (id?: string) => boolean

    validateDataset: (id?: string) => CheckResult
    buildCurrentDatasetPayload: () => Dataset | null
}

// ==============================
// Store
// ==============================
export const useDatasetStore = create<DatasetState>((set, get) => ({

    datasets: [],
    originalDatasets: {},
    currentDatasetId: undefined,

    // =========================
    // 创建
    // =========================
    createDataset: (ds, schema) => {
        const id = "ds_" + nanoid()

        let finalSourceDef = ds
        if (ds.type === "preset") {
            const { start, end } = getDefaultPresetDateRange()
            finalSourceDef = {
                ...ds,
                start: ds.start || start,
                end: ds.end || end,
            }
        }

        const dataset: Dataset = {
            id,
            name: id,
            createdAt: new Date().toISOString(),
            sourceDef: finalSourceDef,
            schema
        }

        set(state => ({
            datasets: [dataset, ...state.datasets],
            currentDatasetId: id,
            originalDatasets: {
                ...state.originalDatasets,
                [id]: structuredClone(dataset)
            }
        }))

        return dataset
    },

    // =========================
    // 获取
    // =========================
    getDatasetName: (id) => {
        const _id = id || get().currentDatasetId
        return get().datasets.find(d => d.id === _id)?.name || ""
    },

    getDataset: (id) => {
        const _id = id || get().currentDatasetId
        return get().datasets.find(d => d.id === _id)
    },

    // =========================
    // 更新
    // =========================
    setDatasetName: (id, name) => {
        let _id = id || get().currentDatasetId

        if (!_id) {
            const ds = get().createDataset({ type: "preset" } as BacktestDataSourceDef)
            _id = ds.id
        }

        set(state => ({
            datasets: state.datasets.map(d =>
                d.id === _id ? { ...d, name } : d
            )
        }))
    },

    updateDataset: (dataset) => {
        set(state => ({
            datasets: state.datasets.map(d =>
                d.id === dataset.id ? { ...dataset } : d
            )
        }))
    },

    updateSourceDef: (id, sourceDef) => {
        const _id = id || get().currentDatasetId
        if (!_id) return undefined

        let updated: Dataset | undefined

        set(state => ({
            datasets: state.datasets.map(d => {
                if (d.id !== _id) return d

                updated = {
                    ...d,
                    sourceDef
                }
                return updated
            })
        }))

        return updated
    },

    setCurrentDataset: (id) => set({ currentDatasetId: id }),

    // =========================
    // ⭐ Snapshot（核心）
    // =========================
    setOriginalDataset: (ds) => {
        set(state => ({
            originalDatasets: {
                ...state.originalDatasets,
                [ds.id]: structuredClone(ds)
            }
        }))
    },

    // =========================
    // ⭐ Dirty Check（核心）
    // =========================
    isDatasetDirty: (id) => {
        const _id = id || get().currentDatasetId
        if (!_id) return false

        const current = get().datasets.find(d => d.id === _id)
        const original = get().originalDatasets[_id]

        if (!current || !original) return false

        return JSON.stringify(current) !== JSON.stringify(original)
    },

    // =========================
    // 校验
    // =========================
    validateDataset: (id) => {
        const errors: string[] = []

        const _id = id || get().currentDatasetId
        if (!_id) {
            errors.push("未选择任何数据集")
            return new SimpleCheckResult(...errors)
        }

        const dataset = get().datasets.find(d => d.id === _id)
        if (!dataset) {
            errors.push("数据集不存在")
            return new SimpleCheckResult(...errors)
        }

        const def = dataset.sourceDef

        if (def.type === "sql") {
            if (!def.sql?.trim()) {
                errors.push("SQL 不能为空")
            }
        } else if (def.type === "preset") {
            if (!def.start) errors.push("开始时间不能为空")
            if (!def.end) errors.push("结束时间不能为空")
        } else if (def.type === "filters") {
            if (!def.filters?.length) {
                errors.push("至少需要一个过滤条件")
            }
        }

        return new SimpleCheckResult(...errors)
    },

    // =========================
    // Payload
    // =========================
    buildCurrentDatasetPayload: () => {
        const id = get().currentDatasetId
        if (!id) return null
        return get().datasets.find(d => d.id === id) || null
    }

}))