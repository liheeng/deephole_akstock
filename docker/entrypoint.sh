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
# 每个市场独立的 cron 调度规则（默认值可根据需求调整）
CRON_SCHEDULE_CN=${CRON_SCHEDULE_CN:-"0 18 * * *"}
CRON_SCHEDULE_HK=${CRON_SCHEDULE_HK:-"0 21 * * *"}
CRON_SCHEDULE_US=${CRON_SCHEDULE_US:-"0 8 * * *"} 
CRON_SCHEDULE_BAOSTOCK_HIS=${CRON_SCHEDULE_BAOSTOCK_HIS:-"0 20 * * *"} 

mkdir -p /logs

# 清空 crontab 文件（避免重复）
> /app/crontab

# 写入 CN 市场 cron 任务
if [ -n "$CRON_SCHEDULE_CN" ]; then
  echo "$CRON_SCHEDULE_CN export PYTHONPATH=/app && cd /app && python /app/cron/cron_runner_cn.py >> /logs/cron_cn_\$(date +\%Y\%m\%d).log 2>&1" >> /app/crontab
fi

# 写入 HK 市场 cron 任务
if [ -n "$CRON_SCHEDULE_HK" ]; then
  echo "$CRON_SCHEDULE_HK export PYTHONPATH=/app && cd /app && python /app/cron/cron_runner_hk.py >> /logs/cron_hk_\$(date +\%Y\%m\%d).log 2>&1" >> /app/crontab
fi

# 写入 US 市场 cron 任务
if [ -n "$CRON_SCHEDULE_US" ]; then
  echo "$CRON_SCHEDULE_US export PYTHONPATH=/app && cd /app && python /app/cron/cron_runner_us.py >> /logs/cron_us_\$(date +\%Y\%m\%d).log 2>&1" >> /app/crontab
fi

# 写入 US 市场 cron 任务
if [ -n "$CRON_SCHEDULE_BAOSTOCK_HIS" ]; then
  echo "$CRON_SCHEDULE_BAOSTOCK_HIS export PYTHONPATH=/app && cd /app && python /app/tools/baostock/append_download.py >> /logs/cron_baostock_\$(date +\%Y\%m\%d).log 2>&1" >> /app/crontab
fi

# 打印 cron 配置（便于调试）
echo "=============================="
echo "CRON SCHEDULE CN: $CRON_SCHEDULE_CN"
echo "CRON SCHEDULE HK: $CRON_SCHEDULE_HK"
echo "CRON SCHEDULE US: $CRON_SCHEDULE_US"
echo "CRON SCHEDULE Baostock History: $CRON_SCHEDULE_BAOSTOCK_HIS"
echo "------------------------------"
cat /app/crontab
echo "=============================="

# 启动 cron
exec /usr/local/bin/supercronic /app/crontab

export CPU_COUNT=$(nproc)
export NUMBA_NUM_THREADS=$CPU_COUNT
export OMP_NUM_THREADS=$CPU_COUNT
export MKL_NUM_THREADS=$CPU_COUNT