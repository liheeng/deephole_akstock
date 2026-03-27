import logging
import os
from utils.common import is_running_in_docker

logs_volume = "/logs" if is_running_in_docker() else "./logs"
LOG_DIR = logs_volume


def get_default_logger(name="default"):
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # ❗避免重复 handler（非常重要）
    if logger.handlers:
        return logger

    log_file = os.path.join(LOG_DIR, "default.log")

    handler = logging.FileHandler(log_file, encoding="utf-8")

    # ✅【关键修复】加上 %(name)s 才能打印 logger 名字
    formatter = logging.Formatter(
        "%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # ✅ 可选：同时输出到控制台
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def get_logger(name):
    return get_default_logger(name)

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