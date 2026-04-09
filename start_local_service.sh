#!/bin/bash
workpath=$1

if [ -z "$workpath" ]; then
    workpath=$(pwd)
fi

cd "${workpath}" || exit

# kill existing services
ps aux | grep "streamlit" | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | grep "uvicorn" | grep -v grep | awk '{print $2}' | xargs kill -9

export PYTHONPATH=./app

mkdir -p logs

nohup uvicorn api:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
nohup streamlit run app/dashboard.py --server.port 8051 --server.address 0.0.0.0 > logs/streamlit.log 2>&1 &
