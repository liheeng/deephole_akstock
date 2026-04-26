#!/bin/bash

# 1. 进入项目根目录（关键）
cd "$(dirname "$0")/.." || exit

# 2. 读取环境变量
ENV_FILE=${1:-.env.dev}
set -o allexport
source "docker/$ENV_FILE"
set +o allexport

# 3. 文件路径
COMPOSE_TPL="docker/docker-compose.yaml"
COMPOSE_FINAL="docker/docker-compose-final.yaml"

# 4. 复制
rm -f "$COMPOSE_FINAL"
cp "$COMPOSE_TPL" "$COMPOSE_FINAL"

# 5. 替换变量
sed -i.bak "s|_TEMP_IMAGE_NAME_|$_TEMP_IMAGE_NAME_|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_IMAGE_VERSION|$_TEMP_IMAGE_VERSION|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_DB_INIT_SERVICE|$_TEMP_DEEPHOLE_DB_INIT_SERVICE|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_API_SERVICE|$_TEMP_DEEPHOLE_API_SERVICE|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_FETCHER_SERVICE|$_TEMP_DEEPHOLE_FETCHER_SERVICE|g" "$COMPOSE_FINAL"
sed -i.bak "s|_TEMP_DEEPHOLE_DASHBOARD_SERVICE|$_TEMP_DEEPHOLE_DASHBOARD_SERVICE|g" "$COMPOSE_FINAL"

rm -f "${COMPOSE_FINAL}.bak"

# 6. build（关键修复：context=project root）
docker build -t "${_TEMP_IMAGE_NAME_}:${_TEMP_IMAGE_VERSION}" -f docker/Dockerfile .

# 7. 启动
docker compose -f "$COMPOSE_FINAL" --env-file "docker/$ENV_FILE" down
docker compose -f "$COMPOSE_FINAL" --env-file "docker/$ENV_FILE" up -d