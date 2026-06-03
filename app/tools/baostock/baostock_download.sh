#!/bin/bash

cd /home/pi/git/stock/deephole_akstock
export PYTHONPATH=./app
/usr/local/bin/python app/tools/baostock/baostock_download.py >> ./logs/cron_baostock_$(date +\%Y\%m\%d).log 2>&1
/usr/local/bin/python app/tools/baostock/update_baostock_indicators.py >> ./logs/cron_baostock_$(date +\%Y\%m\%d).log 2>&1
/usr/local/bin/python app/tools/baostock/update_baostock_signals.py >> ./logs/cron_baostock_$(date +\%Y\%m\%d).log 2>&1
/usr/local/bin/python app/tools/baostock/update_baostock_factor_scores.py >> ./logs/cron_baostock_$(date +\%Y\%m\%d).log 2>&1
