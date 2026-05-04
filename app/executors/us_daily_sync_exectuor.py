# app/executors/us_daily_sync_executor.py

from core.job import JobType, Job
from markets.us_stock import USStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater
from .base import ExecutorBase

@register_executor(JobType.US_DAILY_SYNC.value)
class USDailySyncExecutor(ExecutorBase):

    def execute_job(self, job: Job):
        patch_requests()
        
        Updater().run(USStockMarket(), job)
        
        # return data
        return f"US daily sync completed, job= {job.id} - {job.type.value}"
    
    def cancel_job(self, job_id: str) -> bool:
        return super().cancel_job(job_id)