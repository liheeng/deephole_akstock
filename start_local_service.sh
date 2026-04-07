#!/bin/bash

cd /home/pi/git/stock/deephole_akstock || exit

export PYTHONPATH=./app

mkdir -p logs

nohup uvicorn api:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
nohup streamlit run app/dashboard.py --server.port 8051 --server.address 0.0.0.0 > logs/streamlit.log 2>&1 &
