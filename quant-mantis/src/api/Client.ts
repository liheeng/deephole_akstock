// src/api/Client.ts
import axios from 'axios';
// Init nodes
import { NodeRegistry } from "../model/dsl_node/node_registry";
import { useNodes } from "../hooks/useNodes"
import { API_URL_BASE, CONTENT_TYPE, CORS_CONFIG } from "../configs/apiConfig"
import { type Dataset } from "../store/dataset.store";
import { type BacktestState } from "../store/backtest/backtest.store";
import { type BacktestResultState } from "../store/backtest/backtestresult.store";
import { useStrategyStore, type Strategy } from '../store/backtest/strategy.store';
import { useFactorStore, type Factor } from '../store/backtest/factor.store';
import { useSignalStore, type Signal } from '../store/backtest/signal.store';

export const apiClient = axios.create({
    baseURL: API_URL_BASE + "/api",
});
// 2. 跨域关键：允许携带 cookie / token 凭证
apiClient.defaults.withCredentials = CORS_CONFIG.allow_credentials || false;
// 3. 通用请求头（可选，但建议加）
apiClient.defaults.headers.common['Content-Type'] = CONTENT_TYPE;


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

export async function callBacktest(backtest_config: any) {
    try {
        // 1. 确保是 JSON 对象
        const jsonPayload = typeof backtest_config === 'string'
            ? JSON.parse(backtest_config)
            : backtest_config;

        // 2. 关键：把所有 undefined 转成 null，避免 422
        const fixedPayload = JSON.parse(JSON.stringify(jsonPayload, (_, v) =>
            v === undefined ? null : v
        ));

        // 3. 等待请求完成
        const res = await apiClient.post("/backtest", fixedPayload, {
            withCredentials: true,
            headers: {
                "Content-Type": "application/json",
            },
        });

        // 4. 成功 → 返回数据
        return res.data;

    } catch (err) {
        // 5. 失败 → 返回 null
        console.error("回测失败", err);
        return null;
    }
}


// ======================================================
// 1. 获取所有数据集
// ======================================================
export async function fetchDatasets(): Promise<Dataset[] | null> {
    try {
        const res = await apiClient.get<Dataset[]>("/backtest/datasets", { withCredentials: true });
        return res.data;
    } catch (err) {
        console.error("获取数据集失败", err);
        return null;
    }
}

// ======================================================
// 2. 创建 / 更新数据集（UPSERT）
// ======================================================
export async function updateDataset(dataset: Dataset) {
    try {
        // 后端需要：sourceDef / schema / cache → JSON
        const payload = {
            withCredentials: true,
            id: dataset.id,
            name: dataset.name,
            sourceDef: dataset.sourceDef,
            schema: dataset.schema,
            rowCount: dataset.rowCount,
            cache: dataset.cache,
        };

        const res = await apiClient.post("/backtest/dataset", payload);
        return res.data;
    } catch (err) {
        console.error("保存数据集失败", err);
        return null;
    }
}

// 回测配置 TS 接口 (与后端 Python BacktestConfig 完全对齐)
export interface BacktestConfig {
    id: string;
    name: string;
    portfolio_mode: string;
    params: Record<string, any>;
    schedule_signal: Record<string, any>;
    strategy_op: Record<string, any>;
    vote_weights: Record<string, any>;
    strategy_weights: Record<string, any>;
    // 策略数组
    strategies: Record<string, Strategy>;
    factors: Record<string, Factor>;
    signals: Record<string, Signal>;
    created_at: string | null;
    updated_at: string | null;
}

// 默认值（对应 Python = [] / None）
export const defaultBacktestConfig: BacktestConfig = {
    id: '',
    name: '',
    portfolio_mode: '',
    params: {},
    schedule_signal: {},
    strategy_op: {},
    vote_weights: {},
    strategy_weights: {},
    strategies: {},
    factors: {},
    signals: {},
    created_at: null,
    updated_at: null
};

// ======================================================
// 3. 获取所有回测配置
// ======================================================
export async function fetchBacktestConfigs(): Promise<BacktestConfig[] | null> {
    try {
        const res = await apiClient.get<BacktestConfig[]>("/backtest/configs", { withCredentials: true });
        return res.data;
    } catch (err) {
        console.error("获取回测配置失败", err);
        return null;
    }
}

// 别名（兼容你现有代码）
export const getBacktestConfigs = fetchBacktestConfigs;

// ======================================================
// 4. 创建 / 更新 回测配置（UPSERT）
// 自动把 BacktestState → 后端需要的结构
// ======================================================
export async function updateBacktestConfig(config: BacktestState) {
    try {
        // ==============================================
        // 🔥 关键：从 strategy / factor / signal store 取数据
        // ==============================================
        const { strategies } = useStrategyStore.getState();
        const { factors } = useFactorStore.getState();
        const { signals } = useSignalStore.getState();

        // ==============================================
        // 🔥 构建完整 payload（包括 strategies！）
        // ==============================================
        const payload = {
            withCredentials: true,
            id: config.id,
            name: config.name,
            portfolio_mode: config.portfolio_mode,
            params: config.params,
            schedule_signal: config.schedule_signal,
            strategy_op: config.strategy_op,
            vote_weights: config.vote_weights,
            strategy_weights: config.strategy_weights,
            strategies: strategies,
            factors: factors,
            signals: signals
        };

        const res = await apiClient.post("/backtest/config", payload);
        return res.data;
    } catch (err) {
        console.error("保存回测配置失败", err);
        return null;
    }
}

/**
 * 获取历史回测结果
 * 可按 dataset_config_id / portfolio_name 筛选
 */
export async function fetchBacktestResults(
    dataset_config_id?: string,
    portfolio_name?: string
): Promise<BacktestResultState[] | null> {
    try {
        const res = await apiClient.get<BacktestResultState[]>("/backtest/results", {
            params: {
                withCredentials: true,
                dataset_config_id,
                portfolio_name,
            },
        });

        return res.data;
    } catch (err) {
        console.error("获取回测结果失败", err);
        return null;
    }
}

export async function fetchAPIServiceIp(): Promise<any | null> {
    try {
        const res = await apiClient.get("/api_service/ip", { withCredentials: true });
        return res.data
    } catch (err) {
        console.error("获取api_service/ip失败", err);
        return null;
    }
}


export async function fetchTerminalTargets(): Promise<any | null> {
    try {
        const res = await apiClient.get("/terminal/targets", { withCredentials: true });
        return res.data
    } catch (err) {
        console.error("获取terminal targets失败", err);
        return null;
    }
}


// 建议放在 api 文件夹或 store 中
export const fetchStockDaily = async (symbol: string, startDate: string, endDate: string) => {
    // 构造你提到的 SQL 语句
    const sql = `
        SELECT date, open, close, high, low, volume 
        FROM stock_daily 
        WHERE symbol = '${symbol}' 
          AND date >= '${startDate}' 
          AND date <= '${endDate}'
        ORDER BY date ASC
    `;

    const res = await apiClient.post("/execute_sql", { sql }, {
        withCredentials: true,
    });

    if (res.data.status === 'success') {
        // ECharts Candlestick 需要的格式通常是: [date, open, close, low, high]
        return res.data.data.map((item: any) => ({
            date: item.date,
            values: [item.open, item.close, item.low, item.high],
            volume: item.volume
        }));
    }
    return [];
};

export async function startJupyterLab(): Promise<any | null> {
    try {
        const res = await apiClient.get("/jupyter/start-jupyter", { withCredentials: true });
        return res?.data
    } catch (err) {
        console.error("Jupyter启动失败!...", err);
        return null;
    }
}

export async function stopJupyterLab(): Promise<any | null> {
    try {
        const res = await apiClient.get("/jupyter/stop-jupyter", { withCredentials: true });
        return res?.data
    } catch (err) {
        console.error("Jupyter停止失败...", err);
        return null;
    }
}

