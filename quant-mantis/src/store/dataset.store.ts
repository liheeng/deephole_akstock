import { create } from "zustand"
import { nanoid } from "nanoid"

export type Filter =
    | { field: "market"; op: "in"; value: string[] }
    | { field: "symbol"; op: "in"; value: string[] }
    | { field: "sector"; op: "in"; value: string[] }     // 👈 新增
    | { field: "universe"; op: "in"; value: string }     // 👈 新增
    | { field: "date"; op: "between"; value: [string, string] }

export type BacktestDataSource =
    | {
        type: "sql"
        sql: string
        schema?: string[]
    }
    | {
        type: "preset"
        markets?: string[]
        symbols?: string[]

        sectors?: string[]      // 👈 新增
        universe?: string       // 👈 新增

        start: string
        end: string
        sql: string
    }
    | {
        type: "filters"
        filters: Filter[]
    }

export function presetToFilters(ds: BacktestDataSource): Filter[] {
    if (ds.type !== "preset") return []

    const filters: Filter[] = []

    if (ds.markets?.length) {
        filters.push({
            field: "market",
            op: "in",
            value: ds.markets
        })
    }

    if (ds.symbols?.length) {
        filters.push({
            field: "symbol",
            op: "in",
            value: ds.symbols
        })
    }

    if (ds.sectors?.length) {
        filters.push({
            field: "sector",
            op: "in",
            value: ds.sectors
        })
    }

    if (ds.universe) {
        filters.push({
            field: "universe",
            op: "in",
            value: ds.universe
        })
    }

    if (ds.start && ds.end) {
        filters.push({
            field: "date",
            op: "between",
            value: [ds.start, ds.end]
        })
    }

    return filters
}

export interface Dataset {
    id: string
    name: string
    createdAt: string
    source: BacktestDataSource

    schema?: string[]
    rowCount?: number

    cache?: {
        status: 'ready' | 'running' | 'error'
        tableName?: string
    }
}

interface DatasetState {
    datasets: Dataset[]
    currentDatasetId?: string

    createDataset: (ds: BacktestDataSource, schema?: string[]) => string
    setCurrentDataset: (id: string) => void
    buildCurrentDatasourcePayload: () => any
}

export const useDatasetStore = create<DatasetState>((set, get) => ({

    datasets: [],

    createDataset: (source, schema) => {

        const id = "ds_" + nanoid(6)

        const dataset: Dataset = {
            id,
            name: id,
            createdAt: new Date().toISOString(),
            source,
            schema
        }

        set(state => ({
            datasets: [dataset, ...state.datasets],
            currentDatasetId: id
        }))

        return id
    },

    setCurrentDataset: (id) => set({ currentDatasetId: id }),

    // =========================
    // Payload
    // =========================
    buildCurrentDatasourcePayload: () => {

        const s = get()
        if (!s.currentDatasetId) return null

        const dataset = s.datasets.find(d => d.id === s.currentDatasetId)
        if (!dataset) return null

        return {
            id: dataset.id,
            name: dataset.name,
            source: dataset.source,
            schema: dataset.schema
        }
    },

    buildDatasourcePayload: (id: string) => {
        const datasets = get().datasets;

        // 👇 id 为空 → 只返回所有 source 组成的数组
        if (!id || id.trim() === "") {
            return datasets.map(item => item.source);
        }

        // 有 id → 返回单条数据
        const dataset = datasets.find(d => d.id === id);
        if (!dataset) return null;

        return {
            id: dataset.id,
            name: dataset.name,
            source: dataset.source,
            schema: dataset.schema
        };
    }
}))