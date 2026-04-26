# 1. 切到项目根目录（关键）
cd "$(dirname "$0")/.." || exit

# kill existing services
ps aux | grep "vite" | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | grep "uvicorn" | grep -v grep | awk '{print $2}' | xargs kill -9

export PYTHONPATH=./app

mkdir -p logs

nohup uvicorn api:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
cd quant-mantis || exit
nohup npm run dev > logs/streamlit.log 2>&1 &
cd -