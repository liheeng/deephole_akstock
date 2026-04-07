# app/cron_runner_cn.py
import os

import requests
from core.job import JobType
from utils.log_manager import get_logger
from datetime import datetime
from utils.common import is_running_in_docker
from sources.data_source import DataSourceApiName

logger = get_logger(__name__)

API_SERVICE_NAME = os.getenv("API_SERVICE_NAME", "akstock_api_service")
API_PORT = os.getenv("API_PORT", "8000")
API = "http://" + API_SERVICE_NAME + ":" + API_PORT if is_running_in_docker() else "http://localhost:" + API_PORT
sync_cn_daily_url = f"{API}/sync_daily/" + JobType.CN_DAILY_SYNC.value

logger.info(f"start CN cron task at {datetime.now()}")

response = requests.get(
    url=sync_cn_daily_url,
    params={"data_source": DataSourceApiName.AKSHARE_SINA_API.value}
)

# 日志/打印结果
print(response.status_code)
print(response.text)
print(response.json())
logger.info(f"CN cron task result: {response.status_code} - {response.text} - {response.json()}")
