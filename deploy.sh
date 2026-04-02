#!/bin/bash

# 0. The IFIND_REFRESH_TOKEN environment variable must be set before running this script
if [[ -z "${IFIND_REFRESH_TOKEN}" ]]; then
    echo "❌ 错误：系统环境变量 $IFIND_REFRESH_TOKEN 未设置，请先设置"
    exit 1
fi

# 1. 读取环境文件（支持传参，默认 .env.dev）
ENV_FILE=${1:-.env.dev}
set -o allexport
source "$ENV_FILE"
set +o allexport

# 2. 文件定义
COMPOSE_TPL="docker-compose.yaml"
COMPOSE_FINAL="docker-compose-final.yaml"

# 3. 干净复制
rm -f "$COMPOSE_FINAL"
cp "$COMPOSE_TPL" "$COMPOSE_FINAL"

# 4. 安全替换（mac + Linux 通用，绝对不炸！）
sed -i.bak "s|_TEMP_IMAGE_NAME_|$_TEMP_IMAGE_NAME_|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_IMAGE_VERSION|$_TEMP_IMAGE_VERSION|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_DB_INIT_SERVICE|$_TEMP_DEEPHOLE_DB_INIT_SERVICE|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_API_SERVICE|$_TEMP_DEEPHOLE_API_SERVICE|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_FETCHER_SERVICE|$_TEMP_DEEPHOLE_FETCHER_SERVICE|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_DASHBOARD_SERVICE|$_TEMP_DEEPHOLE_DASHBOARD_SERVICE|g" "$COMPOSE_FINAL"

# 5. 删除备份文件
rm -f "${COMPOSE_FINAL}.bak"

# 6. 部署
docker build -t "${_TEMP_IMAGE_NAME_}:${_TEMP_IMAGE_VERSION}" .
docker compose -f "$COMPOSE_FINAL" --env-file "$ENV_FILE" down
docker compose -f "$COMPOSE_FINAL" --env-file "$ENV_FILE" up -d