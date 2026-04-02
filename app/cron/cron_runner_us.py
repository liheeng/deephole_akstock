# app/cron_runner_us.py
import requests
from core.job import JobType
from utils.log_manager import get_logger
from datetime import datetime
from utils.common import is_running_in_docker
from sources.data_source import DataSourceApiName

logger = get_logger(__name__)

API = "http://akstock_api_service:8000" if is_running_in_docker() else "http://localhost:8000"
sync_us_daily_url = f"{API}/sync_daily/" + JobType.US_DAILY_SYNC.value

logger.info(f"start US cron task at {datetime.now()}")

response = requests.get(
    url=sync_us_daily_url,
    params={"data_source_api": DataSourceApiName.IFIND_API.value}
)

# 日志/打印结果
print(response.status_code)
print(response.text)
print(response.json())
logger.info(f"US cron task result: {response.status_code} - {response.text} - {response.json()}")