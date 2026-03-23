import logging
import os
from utils.common import is_running_in_docker

logs_volume = "/logs" if is_running_in_docker() else "./logs"
LOG_DIR = logs_volume

def get_default_logger():
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("api")
    logger.setLevel(logging.INFO)

    # ❗避免重复 handler（非常重要）
    if logger.handlers:
        return logger

    log_file = os.path.join(LOG_DIR, "default.log")

    handler = logging.FileHandler(log_file, encoding="utf-8")

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

# def get_task_logger(task_id: str = None):
#     _task_id = task_id if task_id is not None else task_manager.task_id

#     os.makedirs(LOG_DIR, exist_ok=True)

#     logger = logging.getLogger(_task_id)
#     logger.setLevel(logging.INFO)

#     # ❗避免重复 handler（非常重要）
#     if logger.handlers:
#         return logger

#     log_file = os.path.join(LOG_DIR, f"{_task_id}.log")

#     handler = logging.FileHandler(log_file, encoding="utf-8")

#     formatter = logging.Formatter(
#         "%(asctime)s | %(levelname)s | %(message)s"
#     )

#     handler.setFormatter(formatter)
#     logger.addHandler(handler)

#     return logger