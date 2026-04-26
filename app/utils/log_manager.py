# import logging
import os
from utils.common import is_running_in_docker
from loguru import logger


def init_logger():
    logs_volume = "/logs" if is_running_in_docker() else "./logs"
    LOG_DIR = logs_volume
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "default.log")
    logger.add(log_file, rotation="100 MB", encoding="utf-8", enqueue=True)
    logger.info("Logger is initialized")