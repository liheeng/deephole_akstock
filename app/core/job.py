from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
import enum
# from core.task import Task

class JobType(enum.Enum):
    CN_DAILY_SYNC = "cn_daily_sync" # 同步中国股票的日线数据
    HK_DAILY_SYNC = "hk_daily_sync" # 同步香港股票的日线数据
    US_DAILY_SYNC = "us_daily_sync" # 同步美国股票的日线数据

class JobConcurrencyKey(enum.Enum):
    CN_DAILY_SYNC = "cn_daily_sync"
    HK_DAILY_SYNC = "hk_daily_sync"
    US_DAILY_SYNC = "us_daily_sync"

@dataclass
class JobDefinition:
    type: JobType
    concurrency_key: str
    max_concurrency: int
    singleton: bool = False # whether only one instance of this job type can run at the same time across the whole system

JOB_DEFINITIONS = {
    JobType.US_DAILY_SYNC: JobDefinition(
        type=JobType.US_DAILY_SYNC,
        concurrency_key=JobConcurrencyKey.US_DAILY_SYNC.value,
        max_concurrency=1,
        singleton=True
    ),
    JobType.HK_DAILY_SYNC: JobDefinition(
        type=JobType.HK_DAILY_SYNC,
        concurrency_key=JobConcurrencyKey.HK_DAILY_SYNC.value,
        max_concurrency=1,
        singleton=True
    ),
    JobType.CN_DAILY_SYNC: JobDefinition(
        type=JobType.CN_DAILY_SYNC,
        concurrency_key=JobConcurrencyKey.CN_DAILY_SYNC.value,
        max_concurrency=1,
        singleton=True
    ),
}
class JobStatus(enum.Enum):
    CREATED = "CREATED" # indicates that the job is waiting to be executed
    QUEUED = "QUEUED" # indicates the job is added into quequ and be waiting for execution
    RUNNING = "RUNNING" # indicates that the job is currently being executed
    SUCCESS = "SUCCESS" # indicates that the job has completed successfully
    FAILED = "FAILED" # indicates that the job has completed with a failure

@dataclass
class Job:
    id: str
    type: JobType
    task_id: str | None = None  # task object reference
    task: Task | None = None  # task object reference
    status: JobStatus = JobStatus.CREATED
    params: Dict = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    
    retry_count: int = 0
    retries: int = 3

    execute_time: str = ""
    stop_time: str = ""
    message: str = ""
    error: str = ""

    def get_id(self):
        return self.id

    def update_status(self, new_status: JobStatus) -> bool:
        self.status = new_status
        # Here you can also add code to log the status change or perform other actions as needed
        if self.task:
            return self.task.update_status_based_on_jobs() 
        else:
            return False