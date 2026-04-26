#!/bin/bash

# 1. 切到项目根目录（关键）
cd "$(dirname "$0")/.." || exit

# 环境变量文件：优先取命令行参数，默认 .env.dev
ENV_FILE=${1:-.env.dev}

# 加载环境变量
set -o allexport
source "docker/$ENV_FILE"
set +o allexport

# 启动容器（全部从env读取，不写死）
docker start ${_TEMP_DEEPHOLE_API_SERVICE}
docker start ${_TEMP_DEEPHOLE_FETCHER_SERVICE}
docker start ${_TEMP_DEEPHOLE_DASHBOARD_SERVICE}