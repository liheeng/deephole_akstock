from core.task import Task
from core.job import JobType, Job
from utils.task_db import generate_job_id, generate_task_id
from typing import Dict, Any
from dataclasses import dataclass
from sources.data_source import DataSourceApiName


@dataclass
class TaskResult(Dict[str, Any], Any):
    status: bool = False
    data: Any | None = None
    message: str = ""
    error: str = ""
    time: float = 0.0


def create_sync_cn_daily_task(data_source: DataSourceApiName) -> Task:
    # create sync CN daily task
    task = Task(
        id=generate_task_id(), jobs=[], description="Sync daily of Chinese A market")
    job = Job(id=generate_job_id(), type=JobType.CN_DAILY_SYNC, task_id=task.id, task=task, data_source=data_source.value)
    task.jobs.append(job)
    return task


def create_sync_hk_daily_task(data_source: DataSourceApiName) -> Task:
    # create sync HK daily task
    task = Task(
        id=generate_task_id(), jobs=[], description="Sync daily of HK market")
    job = Job(id=generate_job_id(), type=JobType.HK_DAILY_SYNC, task_id=task.id, task=task, data_source=data_source.value)
    task.jobs.append(job)
    return task


def create_sync_us_daily_task(data_source: DataSourceApiName) -> Task:
    # create sync US daily task
    task = Task(
        id=generate_task_id(), jobs=[], description="Sync daily of US market")
    job = Job(id=generate_job_id(), type=JobType.US_DAILY_SYNC, task_id=task.id, task=task, data_source=data_source.value)
    task.jobs.append(job)
    return task


def create_sync_daily_task(sync_type: str, data_source: DataSourceApiName) -> Task | None:
    job_type = JobType(sync_type)
    if job_type == JobType.CN_DAILY_SYNC:
        return create_sync_cn_daily_task(data_source)
    elif job_type == JobType.HK_DAILY_SYNC:
        return create_sync_hk_daily_task(data_source)
    elif job_type == JobType.US_DAILY_SYNC:
        return create_sync_us_daily_task(data_source)
    else:
        return None