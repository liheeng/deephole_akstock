# app/core/task.py
from __future__ import annotations
from dataclasses import dataclass, field
import enum
from typing import Dict, List

class JobStatus(enum.Enum):
    PENDING = "pending" # indicates that the job is waiting to be executed
    RUNNING = "running" # indicates that the job is currently being executed
    COMPLETED = "completed" # indicates that the job has completed execution, regardless of success or failure
    # SUCCESS = "success" # indicates that the job has completed successfully
    FAILED = "failed" # indicates that the job has completed with a failure

@dataclass
class JobMetadata:
    id: str
    type: str
    retries: int = 3
    depends_on: List[str] = field(default_factory=list)

@dataclass
class Job:
    metadata: JobMetadata
    status: JobStatus = JobStatus.PENDING # pending / running / success / failed
    params: Dict = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    belongs_to_task: Task | None = None  # task object reference
    retry_count: int = 0
    execute_time: str = ""
    stop_time: str = ""
    message: str = ""
    error: str = ""

    def get_id(self):
        return self.metadata.id
    
    def update_status(self, new_status: JobStatus):
        self.status = new_status
        # Here you can also add code to log the status change or perform other actions as needed
        self.belongs_to_task.update_status_based_on_jobs() if self.belongs_to_task else None
    
class TaskStatus(enum.Enum):
    CREATED = "created" # indicates that the task has been created but not yet started
    SUBMITTED = "submitted" # indicates that the task has been submitted for execution, usually is in queue waiting for resources or dependencies to be resolved
    # PENDING = "pending" # indicates that the task is pending execution, usually is in queue waiting for resources or dependencies to be resolved
    RUNNING = "running" # indicates that the task is currently being executed
    SUSPENDED = "suspended" # indicates that the task execution has been temporarily suspended, it means the task ever ran but is currently paused, it can be resumed later
    SUCCESS = "success" # indicates that the task has completed successfully
    FAILED = "failed" # indicates that the task has completed with a failure
    PARTIAL_SUCCESS = "partial_success" # indicates that some jobs in the task succeeded while others failed

class TaskMode(enum.Enum):
    SEQUENTIAL = "sequential" # indicates that the jobs in the task should be executed one after another in a specific order
    PARALLEL = "parallel" # indicates that the jobs in the task can be executed concurrently without any specific order
    DAG = "dag" # indicates that the jobs in the task have dependencies and should be executed according to a directed acyclic graph (DAG) structure, where some jobs may depend on the completion of others before they can start  

@dataclass
class Task:
    id: str
    desc: str = ""
    status: TaskStatus = TaskStatus.CREATED
    jobs: List[Job] = field(default_factory=list)
    mode: TaskMode = TaskMode.DAG   # sequential / parallel / dag
    create_time: str = ""
    start_time: str = ""
    stop_time: str = ""
    message: str = ""

    def update_status_based_on_jobs(self):
        if all(job.status == JobStatus.COMPLETED for job in self.jobs):
            self.status = TaskStatus.SUCCESS
        elif any(job.status == JobStatus.FAILED for job in self.jobs):
            self.status = TaskStatus.PARTIAL_SUCCESS if any(job.status == JobStatus.COMPLETED for job in self.jobs) else TaskStatus.FAILED
        elif any(job.status == JobStatus.RUNNING for job in self.jobs):
            self.status = TaskStatus.RUNNING
        else:
            pass

