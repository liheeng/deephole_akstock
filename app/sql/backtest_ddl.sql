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
CREATE INDEX IF NOT EXISTS idx_backtest_config_name ON backtest_config(name);


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