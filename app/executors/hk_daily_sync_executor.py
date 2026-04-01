# app/executors/hk_daily_sync_executor.py

from core.job import JobType, Job
from markets.hk_stock import HongKongStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater

@register_executor(JobType.HK_DAILY_SYNC.value)
class HKDailySyncExecutor:

    def execute(self, job: Job):
        patch_requests()
        
        Updater().run(HongKongStockMarket(), job)
        
        # return data
        return f"HongKong daily sync completed, job= {job.id} - {job.type.value}"