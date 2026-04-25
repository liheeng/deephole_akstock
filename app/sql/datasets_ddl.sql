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