import { create } from "zustand"
import { nanoid } from "nanoid"
import { SimpleCheckResult, type CheckResult } from "../common/Types"

// ==============================
// ✅ 工具函数：获取默认日期范围
// ==============================
export function getDefaultPresetDateRange(): { start: string; end: string } {
    const today = new Date();

    // 结束日期 = 今天
    const end = today.toISOString().split("T")[0]!;

    // 开始日期 = 3年前的 1月1日
    const threeYearsAgo = today.getFullYear() - 3;
    const start = `${threeYearsAgo}-01-01`;

    return { start, end };
}

export type Filter =
    | { field: "market"; op: "in"; value: string[] }
    | { field: "symbol"; op: "in"; value: string[] }
    | { field: "sector"; op: "in"; value: string[] }     // 👈 新增
    | { field: "universe"; op: "in"; value: string }     // 👈 新增
    | { field: "date"; op: "between"; value: [string, string] }

export type BacktestDataSourceDef =
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
        sql?: string
    }
    | {
        type: "filters"
        filters: Filter[]
    }

export function presetToFilters(ds: BacktestDataSourceDef): Filter[] {
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
    currentDatasetId?: string

    createDataset: (ds: BacktestDataSourceDef, schema?: string[]) => string
    setCurrentDataset: (id: string) => void
    validateCurrentDataset: () => CheckResult
    buildCurrentDatasetPayload: () => any
}

export const useDatasetStore = create<DatasetState>((set, get) => ({

    datasets: [],

    createDataset: (sourceDef, schema) => {
        const id = "ds_" + nanoid(6)

        // ✅ 如果是 preset 类型，自动填充默认日期
        let finalSourceDef = sourceDef;
        if (sourceDef.type === "preset") {
            const { start, end } = getDefaultPresetDateRange();
            finalSourceDef = {
                ...sourceDef,
                start: sourceDef.start || start,
                end: sourceDef.end || end,
            };
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
            currentDatasetId: id
        }))

        return id
    },

    setCurrentDataset: (id) => set({ currentDatasetId: id }),

    // ==============================================
    // ✅ 完美实现：严格按类型 & 可选字段允许为空
    // ==============================================
    validateCurrentDataset: (): CheckResult => {
        const s = get()
        const errors: string[] = []

        // 1. 必须选择数据集
        if (!s.currentDatasetId) {
            errors.push("未选择任何数据集")
            return new SimpleCheckResult(...errors)
        }

        const dataset = s.datasets.find(d => d.id === s.currentDatasetId)
        if (!dataset) {
            errors.push("数据集不存在或已被删除")
            return new SimpleCheckResult(...errors)
        }

        const def = dataset.sourceDef

        // --------------------------
        // 类型 1：sql
        // --------------------------
        if (def.type === "sql") {
            if (!def.sql || !def.sql.trim()) {
                errors.push("SQL 模式：SQL 语句不能为空")
            }
            return new SimpleCheckResult(...errors)
        }

        // --------------------------
        // 类型 2：preset
        // --------------------------
        else if (def.type === "preset") {
            // 必填：start
            if (!def.start || !def.start.trim()) {
                errors.push("Preset 模式：开始日期不能为空")
            }
            // 必填：end
            if (!def.end || !def.end.trim()) {
                errors.push("Preset 模式：结束日期不能为空")
            }

            // 👇 markets / symbols / sectors / universe 都是可选，不校验
            return new SimpleCheckResult(...errors)
        }

        // --------------------------
        // 类型 3：filters
        // --------------------------
        else if (def.type === "filters") {
            if (!def.filters || def.filters.length === 0) {
                errors.push("Filters 模式：至少需要添加一个过滤条件")
            }
            else {
                def.filters.forEach((f, idx) => {
                    if (!f.field) {
                        errors.push(`过滤条件 ${idx + 1}：field 不能为空`)
                    }
                    if (!f.op) {
                        errors.push(`过滤条件 ${idx + 1}：op 不能为空`)
                    }
                    if (f.value === undefined || f.value === null) {
                        errors.push(`过滤条件 ${idx + 1}：value 不能为空`)
                    }
                })
            }
            return new SimpleCheckResult(...errors)
        }

        // --------------------------
        // 未知类型
        // --------------------------
        else {
            errors.push(`不支持的数据源类型：${(def as any).type}`)
        }

        return new SimpleCheckResult(...errors)
    },
    // =========================
    // Payload
    // =========================
    buildCurrentDatasetPayload: () => {

        const s = get()
        if (!s.currentDatasetId) return null

        const dataset = s.datasets.find(d => d.id === s.currentDatasetId)
        if (!dataset) return null

        return dataset
    },

    buildDatasetPayload: (id: string) => {
        const datasets = get().datasets;

        // 👇 id 为空 → 只返回所有 source 组成的数组
        if (!id || id.trim() === "") {
            return datasets.map(item => item.sourceDef);
        }

        // 有 id → 返回单条数据
        const dataset = datasets.find(d => d.id === id);
        if (!dataset) return null;

        return dataset
    }
}))