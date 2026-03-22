from utils.task_db import generate_job_id, generate_task_id, insert_task, finish_task
from utils.http import patch_requests
from utils.task_manager_old import task_manager
from utils.log_manager import get_default_logger   
from core.updater import Updater
from core.registry import MARKETS
from core.job import JobType

from db.db_common import DB
import api
from core.task import Task, TaskStatus, Job, JobMetadata, JobStatus
import time
import signal

# 用来安全退出
running = True

def stop(signum, frame):
    global running
    running = False

# 注册 Ctrl+C 信号
signal.signal(signal.SIGINT, stop)

def main():
    # # 👉 启用 UA 注入
    # patch_requests()

    # updater = Updater(DB)

    # for market in MARKETS:
    #     updater.run(market)
    api.init()

    # create task
    task = Task(
        id=generate_task_id(), jobs=[], desc="Sync daily of Chinese A market")
    sync_job = JobMetadata(id=generate_job_id(), type=JobType.CN_DAILY_SYNC.value)
    job = Job(metadata=sync_job, belongs_to_task=task)
    task.jobs.append(job)

    api.run_task(task)

    # ======================
    # 主线程 HOLD 住
    # ======================
    print("主线程已启动，按 Ctrl+C 退出")
    while running:
        time.sleep(1)

if __name__ == "__main__":
    main()
    # source = "standalone"

    # # init task
    # task_id = generate_task_id()
    # logger = get_task_logger(task_id)

    # logger.info(f"Task started | source={source}")

    # if not task_manager.start(source, task_id):
    #     print(f"[{source}] already running by {task_manager.source}")
    #     logger.warning(f"[{source}] already running by {task_manager.source}")
    #     exit(0)

    # id = insert_task(task_id, "stock_fetch", source)
    # try:
    #     main()
        
    #     task_manager.success()
    #     finish_task(id, "SUCCESS")

    #     print(f"[{source}] fetch success")
    #     logger.info(f"[{source}] fetch success")

    # except Exception as e:
    #     task_manager.fail(str(e))
    #     finish_task(id, "FAILED", str(e))

    #     print(f"[{source}] fetch failed: {e}")
    #     logger.error(f"[{source}] fetch failed: {e}")
