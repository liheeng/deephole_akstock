# app/executors/cn_daily_sync_executor.py

from core.job import JobType, Job
from markets.cna_stock import CNAStockMarket
from executors.base import register_executor
from utils.http import patch_requests
from core.updater import Updater
from .base import ExecutorBase


@register_executor(JobType.CN_DAILY_SYNC.value)
class CNDailySyncExecutor(ExecutorBase):

    def execute_job(self, job: Job):
        patch_requests()
        
        Updater().run(CNAStockMarket(), job)
        
        return f"CN daily sync completed, job= {job.id} - {job.type.value}"
    
    def cancel_job(self, job_id: str) -> bool:
        return super().cancel_job(job_id)