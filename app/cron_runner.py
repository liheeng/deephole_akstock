# app/cron_runner.py

# from task_runner import run_fetch

# run_fetch(source="cron")
# curl -X POST "http://api_service:8000/fetch?market=CN"
# curl -X POST "http://api_service:8000/fetch?market=HK"
# curl -X POST "http://api_service:8000/fetch?market=US"

import requests
from core.job import JobType
from utils.log_manager import get_logger
from datetime import datetime
from utils.common import is_running_in_docker
from sources.data_source import DataSourceApiName

logger = get_logger(__name__)

API = "http://akstock_api_service:8000" if is_running_in_docker() else "http://localhost:8000"

sync_cn_daily_url = f"{API}/sync_daily/" + JobType.CN_DAILY_SYNC.value
sync_hk_daily_url = f"{API}/sync_daily/" + JobType.HK_DAILY_SYNC.value
sync_us_daily_url = f"{API}/sync_daily/" + JobType.US_DAILY_SYNC.value

# logger.warning(f"cros task is disabled in version 1.1, will be enabled in version 1.2!")
logger.info(f"start cron tasks at {datetime.now()}")

response = requests.get(
    url=sync_cn_daily_url,
    params={"data_source_api": DataSourceApiName.IFIND_API.value}
)

# 查看结果
print(response.status_code)  # 200 = 成功
print(response.text)         # 返回文本
print(response.json())       # 如果是接口 JSON，直接转字典
logger.info(f"cron task sync_cn_daily_url result: {response.status_code} - {response.text} - {response.json()}")

response = requests.get(
    url=sync_hk_daily_url,
    params={"data_source_api": DataSourceApiName.IFIND_API.value}
)

# 查看结果
print(response.status_code)  # 200 = 成功
print(response.text)         # 返回文本
print(response.json())       # 如果是接口 JSON，直接转字典
logger.info(f"cron task sync_hk_daily_url result: {response.status_code} - {response.text} - {response.json()}")

response = requests.get(
    url=sync_us_daily_url,
    params={"data_source_api": DataSourceApiName.IFIND_API.value}

)
# 查看结果
print(response.status_code)  # 200 = 成功
print(response.text)         # 返回文本
print(response.json())       # 如果是接口 JSON，直接转字典
logger.info(f"cron task sync_us_daily_url result: {response.status_code} - {response.text} - {response.json()}")