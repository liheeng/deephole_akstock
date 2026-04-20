import { create } from "zustand"
import { nanoid } from "nanoid"

export type BacktestDataSource =
    | {
        type: "sql"
        sql: string
        schema?: string[]
    }
    | {
        type: "preset"
        markets?: ("cn" | "hk" | "us")[]
        symbols?: string[]
        start: string
        end: string
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
}

export const useDatasetStore = create<DatasetState>((set) => ({

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

    setCurrentDataset: (id) => set({ currentDatasetId: id })

}))