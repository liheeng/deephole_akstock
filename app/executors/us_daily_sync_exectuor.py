# app/executors/us_daily_sync_executor.py

from core.job import JobType, Job
from markets.us_stock import USStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater


@register_executor(JobType.US_DAILY_SYNC.value)
class USDailySyncExecutor:

    def execute(self, job: Job):
        patch_requests()
        
        Updater().run(USStockMarket(), job)
        
        # return data
        return f"US daily sync completed, job= {job.id} - {job.type.value}"