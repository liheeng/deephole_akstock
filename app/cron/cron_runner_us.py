# app/cron_runner_us.py
import os

import requests
from core.job import JobType
from loguru import logger
from datetime import datetime, timedelta
from utils.common import is_running_in_docker
from sources.data_source import DataSourceApiName
from utils.log_manager import init_logger
from utils.trading_uitl import is_trading_day, get_target_sync_date

init_logger()

API_SERVICE_NAME = os.getenv("API_SERVICE_NAME", "akstock_api_service")
API_PORT = os.getenv("API_PORT", "8000")
API = "http://" + API_SERVICE_NAME + ":" + API_PORT if is_running_in_docker() else "http://localhost:" + API_PORT
sync_us_daily_url = f"{API}/api/sync_daily/" + JobType.US_DAILY_SYNC.value

logger.info(f"cron task --- start US cron task at {datetime.now()}")
yesterday = datetime.now().date() - timedelta(days=1)
if not is_trading_day("US", get_target_sync_date()):
    logger.info(f"cron task --- today ({datetime.now()}) is not trading day, no need to sync data of US market.")
else:
    response = requests.get(
        url=sync_us_daily_url,
        params={"data_source": DataSourceApiName.AKSHARE_SINA_API.value}
    )

    # 日志/打印结果
    logger.info(f"cron task --- {response.status_code}")
    logger.info(f"cron task --- {response.text}")
    logger.info(f"cron task --- {response.json()}")
    logger.info(f"cron task --- US cron task result: {response.status_code} - {response.text} - {response.json()}")
