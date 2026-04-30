# 1. 切到项目根目录（关键）
cd "$(dirname "$0")/.." || exit

# kill existing services
ps aux | grep "uvicorn" | grep -v grep | awk '{print $2}' | xargs kill -9

mkdir -p logs

# start api service
export PYTHONPATH=./app
nohup uvicorn api:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
echo "api service started..."

# start dashboard
cd quant-mantis || exit
# 定义你的进程名字
NAME="deephole_dashboard_dev"

# 检查是否存在
# 正确判断 pm2 进程是否存在（在线/停止都算存在）
if pm2 list | grep -q "$NAME"; then
    echo "✅ 进程 $NAME 已存在，启动/重启它"
    pm2 start "$NAME"
else
    echo "🆕 进程 $NAME 不存在，全新启动"
    pm2 start "npm run dev" --name "$NAME"
fi
echo "dashboard started.."
