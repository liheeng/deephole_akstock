import duckdb
import os
from db.db_common import DB
from utils.common import is_running_in_docker

DB_PATH = DB


def update_db():

    data_dir = "/data" if is_running_in_docker() else "./data"
    os.makedirs(data_dir, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    print("🚀 Update database...")

    # =========================
    # 1. -- 回测系统-数据集元数据表
    # =========================
    con.execute("""
        -- =============================================
        -- 数据集元数据表
        -- 对应：interface Dataset 类型
        -- 存储：回测数据集配置、数据源定义、缓存状态
        -- =============================================
        CREATE TABLE IF NOT EXISTS datasets (
            id VARCHAR PRIMARY KEY,                     -- 数据集唯一ID（如 ds_xxxxxx）
            name VARCHAR NOT NULL,                      -- 数据集名称
            createdAt TIMESTAMP NOT NULL,               -- 创建时间
            updatedAt TIMESTAMP NOT NULL,               -- 更新时间
            sourceDef JSON NOT NULL,                    -- 数据源定义（preset/sql/filters）
            schema JSON,                                -- 字段结构（string[] 数组）
            rowCount BIGINT,                            -- 数据总行数
            cache JSON                                  -- 缓存信息 {status, tableName}
        );

        -- 索引（DuckDB 必须单独创建）
        CREATE INDEX IF NOT EXISTS idx_datasets_id ON datasets (id);
        CREATE INDEX IF NOT EXISTS idx_datasets_name ON datasets (name);
    """)

    # =========================
    # 2. -- 回测系统 - 回测配置主表
    # =========================
    con.execute("""
        -- =============================================
        -- 回测配置主表
        -- 存储：回测名称、运行模式、参数、组合配置等
        -- =============================================
        CREATE TABLE IF NOT EXISTS backtest_config (
            id VARCHAR PRIMARY KEY,                     -- 回测配置唯一ID
            name VARCHAR NOT NULL,                      -- 回测名称/组合名称
            portfolio_mode VARCHAR NOT NULL,            -- 组合模式：signal_strategy / weight_strategy
            params JSON NOT NULL,                       -- 回测参数：{freq, init_cash}
            schedule_signal JSON NOT NULL,              -- 调度信号：{enabled, signalId}
            strategy_op JSON NOT NULL,                  -- 策略组合运算符：{enabled, value}
            vote_weights JSON NOT NULL,                 -- 投票权重：{enabled, value[]}
            strategy_weights JSON NOT NULL,             -- 策略权重：{enabled, value[]}
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 创建时间
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- 更新时间
        );

        -- 索引：按名称查询
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_backtest_config_name ON backtest_config(name);


        -- =============================================
        -- 回测策略表
        -- =============================================
        CREATE TABLE IF NOT EXISTS backtest_strategy (
            id VARCHAR PRIMARY KEY,                     -- 策略唯一ID
            backtest_id VARCHAR NOT NULL,               -- 关联回测ID
            name VARCHAR NOT NULL,                      -- 策略名称
            factor_ids JSON NOT NULL,                   -- 关联因子ID数组
            signal_id VARCHAR,                          -- 关联信号ID
            config JSON NOT NULL,                       -- 策略配置：{mode, threshold, top_n}
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 创建时间
        );

        -- 索引：按回测ID批量查询策略
        CREATE INDEX IF NOT EXISTS idx_backtest_strategy_bid ON backtest_strategy(backtest_id);


        -- =============================================
        -- 回测因子表
        -- =============================================
        CREATE TABLE IF NOT EXISTS backtest_factor (
            id VARCHAR PRIMARY KEY,                     -- 因子唯一ID
            backtest_id VARCHAR NOT NULL,               -- 关联回测ID
            name VARCHAR NOT NULL,                      -- 因子名称
            expr VARCHAR NOT NULL,                      -- 因子表达式
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 创建时间
        );

        -- 索引：按回测ID批量查询因子
        CREATE INDEX IF NOT EXISTS idx_backtest_factor_bid ON backtest_factor(backtest_id);


        -- =============================================
        -- 回测信号表
        -- =============================================
        CREATE TABLE IF NOT EXISTS backtest_signal (
            id VARCHAR PRIMARY KEY,                     -- 信号唯一ID
            backtest_id VARCHAR NOT NULL,               -- 关联回测ID
            name VARCHAR NOT NULL,                      -- 信号名称
            expr VARCHAR NOT NULL,                      -- 信号表达式
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 创建时间
        );

        -- 索引：按回测ID批量查询信号
        CREATE INDEX IF NOT EXISTS idx_backtest_signal_bid ON backtest_signal(backtest_id);
    """)


    # =========================
    # 3. -- 回测系统 - 回测结果表
    # =========================
    con.execute("""
        -- =============================================
        -- 回测结果表（每次运行回测保存一条）
        -- 联合 KEY：dataset_config_id + portfolio_name
        -- =============================================
        CREATE TABLE IF NOT EXISTS backtest_portfolio_results (
            id VARCHAR PRIMARY KEY,
            dataset_config_id VARCHAR NOT NULL,
            portfolio_name VARCHAR NOT NULL,
            stats JSON NOT NULL,
            equity JSON NOT NULL,
            trades JSON NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 索引必须单独创建！
        CREATE INDEX IF NOT EXISTS idx_backtest_res_ds_id ON backtest_portfolio_results (dataset_config_id);
        CREATE INDEX IF NOT EXISTS idx_backtest_res_portfolio ON backtest_portfolio_results (portfolio_name);
        CREATE INDEX IF NOT EXISTS idx_backtest_res_ds_portfolio ON backtest_portfolio_results (dataset_config_id, portfolio_name);
    """)

    con.close()

    print("✅ Schema + Views ready")


if __name__ == "__main__":
    update_db()
