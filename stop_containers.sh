#!/bin/bash

# 环境变量文件：优先取命令行参数，默认 .env.dev
ENV_FILE=${1:-.env.dev}

# 加载环境变量
set -o allexport
source "$ENV_FILE"
set +o allexport

# 停止容器（全部从env读取，不写死）
docker stop ${_TEMP_DEEPHOLE_FETCHER_SERVICE}
docker stop ${_TEMP_DEEPHOLE_DASHBOARD_SERVICE}
docker stop ${_TEMP_DEEPHOLE_API_SERVICE}