#!/bin/sh
set -e

# 二次强制校验
if [ -z "$IFIND_REFRESH_TOKEN" ]; then
  echo "WARN: 没有设置 IFIND_REFRESH_TOKEN 环境变量"
fi

# ======================================
# 只有设置了 ENABLE_CRON=true 才启动 cron
# ======================================
if [ "$ENABLE_CRON" != "true" ]; then
  echo "=== 不启动 cron，执行用户命令: $*"
  exec "$@"
  exit 0
fi

# 下面只有 ENABLE_CRON=true 的容器才会运行
CRON_SCHEDULE=${CRON_SCHEDULE:-"0 18 * * *"}
mkdir -p /logs

echo "$CRON_SCHEDULE cd /app && python /app/cron_runner.py >> /logs/cron_\$(date +\%Y\%m\%d).log 2>&1" > /app/crontab
echo "=============================="
echo "CRON SCHEDULE: $CRON_SCHEDULE"
cat /app/crontab
echo "=============================="

exec /usr/local/bin/supercronic /app/crontab