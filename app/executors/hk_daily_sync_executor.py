# app/executors/hk_daily_sync_executor.py

from core.job import JobType, Job
from markets.hk_stock import HongKongStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater
from .base import ExecutorBase

@register_executor(JobType.HK_DAILY_SYNC.value)
class HKDailySyncExecutor(ExecutorBase):

    def execute_job(self, job: Job):
        patch_requests()
        
        Updater().run(HongKongStockMarket(), job)
        
        # return data
        return f"HongKong daily sync completed, job= {job.id} - {job.type.value}"
    
    def cancel_job(self, job_id: str) -> bool:
        return super().cancel_job(job_id)