# app/executors/base.py
from abc import ABC, abstractmethod
from core.job import JobType, Job

EXECUTOR_REGISTRY = {}


def register_executor(name: str):
    def wrapper(cls):
        EXECUTOR_REGISTRY[name] = cls()
        return cls
    return wrapper


def get_executor(job_type: JobType):
    return EXECUTOR_REGISTRY[job_type.value]


class ExecutorBase(ABC):
    @abstractmethod
    def execute_job(self, job: Job):
        pass

    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        pass
