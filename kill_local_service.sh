#!/bin/bash

ps aux | grep "streamlit" | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | grep "uvicorn" | grep -v grep | awk '{print $2}' | xargs kill -9