# app/executors/hk_daily_sync_executor.py

from jobs.job import JobType
from markets.hk_stock import HongKongStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater

@register_executor(JobType.HK_DAILY_SYNC.value)
class HKDailySyncExecutor:

    def execute(self, job):
        patch_requests()
        
        Updater().run(HongKongStockMarket())
        
        # market = job.params["market"]
        # symbols = job.params.get("symbols", [])

        # data = fetch_data(market, symbols)

        # return data
        return "HongKong daily sync completed"