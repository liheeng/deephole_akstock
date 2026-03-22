# app/core/task.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
import enum
from core.job import JobStatus
from datetime import datetime
class TaskStatus(enum.Enum):
    CREATED = "CREATED" # indicates that the task has been created but not yet started
    SUBMITTED = "SUBMITTED" # indicates that the task has been submitted for execution, usually is in queue waiting for resources or dependencies to be resolved
    RUNNING = "RUNNING" # indicates that the task is currently being executed
    SUSPENDED = "SUSPENDED" # indicates that the task execution has been temporarily suspended, it means the task ever ran but is currently paused, it can be resumed later
    SUCCESS = "SUCCESS" # indicates that the task has completed successfully
    FAILED = "FAILED" # indicates that the task has completed with a failure
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS" # indicates that some jobs in the task succeeded while others failed

class TaskMode(enum.Enum):
    SEQUENTIAL = "sequential" # indicates that the jobs in the task should be executed one after another in a specific order
    PARALLEL = "parallel" # indicates that the jobs in the task can be executed concurrently without any specific order
    DAG = "dag" # indicates that the jobs in the task have dependencies and should be executed according to a directed acyclic graph (DAG) structure, where some jobs may depend on the completion of others before they can start  

@dataclass
class Task:
    id: str
    description: str = ""
    status: TaskStatus = TaskStatus.CREATED
    jobs: List[Job] = field(default_factory=list)
    mode: TaskMode = TaskMode.DAG   # sequential / parallel / dag
    create_time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time: str = ""
    execute_time: str = ""
    stop_time: str = ""
    message: str = ""

    def update_status_based_on_jobs(self) -> bool:
        status = self.status
        if all(job.status == JobStatus.SUCCESS for job in self.jobs):
            self.status = TaskStatus.SUCCESS
        elif any(job.status == JobStatus.FAILED for job in self.jobs):
            self.status = TaskStatus.PARTIAL_SUCCESS if any(job.status == JobStatus.SUCCESS for job in self.jobs) else TaskStatus.FAILED
        elif any(job.status == JobStatus.RUNNING for job in self.jobs):
            self.status = TaskStatus.RUNNING
        else:
            pass

        return status != self.status

