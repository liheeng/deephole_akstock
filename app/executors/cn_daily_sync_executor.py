# app/executors/cn_daily_sync_executor.py

from jobs.job import JobType
from markets.cna_stock import CNAStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater

@register_executor(JobType.CN_DAILY_SYNC.value)
class CNDailySyncExecutor:

    def execute(self, job):
        patch_requests()
        
        Updater().run(CNAStockMarket())
        
        # market = job.params["market"]
        # symbols = job.params.get("symbols", [])

        # data = fetch_data(market, symbols)

        # return data
        return "CN daily sync completed"