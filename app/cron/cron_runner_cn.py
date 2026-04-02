# app/cron_runner_cn.py
import requests
from core.job import JobType
from utils.log_manager import get_logger
from datetime import datetime
from utils.common import is_running_in_docker
from sources.data_source import DataSourceApiName

logger = get_logger(__name__)

API = "http://akstock_api_service:8000" if is_running_in_docker() else "http://localhost:8000"
sync_cn_daily_url = f"{API}/sync_daily/" + JobType.CN_DAILY_SYNC.value

logger.info(f"start CN cron task at {datetime.now()}")

response = requests.get(
    url=sync_cn_daily_url,
    params={"data_source_api": DataSourceApiName.IFIND_API.value}
)

# 日志/打印结果
print(response.status_code)
print(response.text)
print(response.json())
logger.info(f"CN cron task result: {response.status_code} - {response.text} - {response.json()}")