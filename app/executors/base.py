# app/executors/base.py
from core.job import JobType

EXECUTOR_REGISTRY = {}

def register_executor(name: str):
    def wrapper(cls):
        EXECUTOR_REGISTRY[name] = cls()
        return cls
    return wrapper


def get_executor(job_type: JobType):
    return EXECUTOR_REGISTRY[job_type.value]