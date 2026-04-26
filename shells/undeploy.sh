#!/bin/bash

# 1. 切到项目根目录（关键）
cd "$(dirname "$0")/.." || exit

# 2. 读取 env（在 docker/ 目录下）
ENV_FILE=${1:-.env.dev}
set -o allexport
source "docker/$ENV_FILE"
set +o allexport

# 3. 定义 compose 文件
COMPOSE_FILE="docker/docker-compose-final.yaml"

# 4. 停掉服务
docker compose -f "$COMPOSE_FILE" --env-file "docker/$ENV_FILE" down

# 5. 删除镜像（避免误删，加判断）
if [[ -n "${_TEMP_IMAGE_NAME_}" && -n "${_TEMP_IMAGE_VERSION}" ]]; then
    docker rmi -f "${_TEMP_IMAGE_NAME_}:${_TEMP_IMAGE_VERSION}" || true
fi