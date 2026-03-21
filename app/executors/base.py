# app/executors/base.py

EXECUTOR_REGISTRY = {}

def register_executor(name):
    def wrapper(cls):
        EXECUTOR_REGISTRY[name] = cls()
        return cls
    return wrapper


def get_executor(name):
    return EXECUTOR_REGISTRY[name]