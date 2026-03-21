# app/task_runner.py

from utils.log_manager import get_default_logger
from utils.task_manager import task_manager
from utils.task_db import generate_task_id, insert_task, finish_task


def run_fetch(source="unknown"):
    task_id = generate_task_id()
    logger = get_task_logger(task_id)

    logger.info(f"Task started | source={source}")

    if not task_manager.start(source, task_id):
        print(f"[{source}] already running by {task_manager.source}")
        return False

    id = insert_task(task_id, "stock_fetch", source)

    try:
        print(f"[{source}] fetch started")

        from main import main
        main()

        task_manager.success()
        finish_task(id, "SUCCESS")

        print(f"[{source}] fetch success")

    except Exception as e:
        task_manager.fail(str(e))
        finish_task(id, "FAILED", str(e))

        print(f"[{source}] fetch failed: {e}")

    return True