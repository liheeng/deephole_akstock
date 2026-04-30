#!/bin/bash

# 1. 切到项目根目录（关键）
cd "$(dirname "$0")/../quant-mantis" || exit

# 2. 定义你的进程名字
NAME="deephole_dashboard_dev"

# 检查是否存在
if pm2 id "$NAME" > /dev/null 2>&1; then
    echo "进程 $NAME 已存在，直接启动"
    pm2 start "$NAME"
else
    echo "进程 $NAME 不存在，新建并启动"
    pm2 start "npm run dev" --name "$NAME"
fi