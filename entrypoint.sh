#!/bin/sh

set -e

# 默认 cron（可被 docker env 覆盖）
CRON_SCHEDULE=${CRON_SCHEDULE:-"0 18 * * *"}

mkdir -p /logs

echo "$CRON_SCHEDULE python /app/cron_runner.py >> /logs/cron.log 2>&1" > /app/crontab

echo "=============================="
echo "CRON SCHEDULE: $CRON_SCHEDULE"
echo "CRONTAB CONTENT:"
cat /app/crontab
echo "=============================="

exec /usr/local/bin/supercronic /app/crontab