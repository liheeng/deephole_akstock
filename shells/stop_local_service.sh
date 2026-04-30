#!/bin/bash

pm2 stop deephole_dashboard_dev
ps aux | grep "uvicorn" | grep -v grep | awk '{print $2}' | xargs kill -9
