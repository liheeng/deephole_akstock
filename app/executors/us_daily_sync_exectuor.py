# app/executors/us_daily_sync_executor.py

from jobs.job import JobType
from markets.us_stock import USStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater

@register_executor(JobType.US_DAILY_SYNC.value)
class USDailySyncExecutor:

    def execute(self, job):
        patch_requests()
        
        Updater().run(USStockMarket())
        
        # market = job.params["market"]
        # symbols = job.params.get("symbols", [])

        # data = fetch_data(market, symbols)

        # return data
        return "US daily sync completed"