# app/core/worker.py

import threading
from time import time
from core.job import Job, JobStatus, JOB_DEFINITIONS
from core.task_manager import job_can_run, task_manager
from core.queue import job_queue
from executors.base import get_executor
from core.result_store import result_store
from utils.log_manager import get_default_logger

def worker_loop():
    while True:
        # 1. 清理超时 running 任务
        task_manager.clean_stale_running_jobs()

        job: Job = job_queue.get()
        
        if not job_can_run(job):
            job_queue.put(job)   # 放回队列
            time.sleep(1)        # 避免空转
            continue

        executor = get_executor(job.type)

        defn = JOB_DEFINITIONS[job.type.value]
        # Step 1: 抢占并发槽位（非常关键）
        ok = task_manager.try_acquire_concurrency_slot(job, defn)
        if not ok:
            job_queue.put(job)   # 放回队列
            time.sleep(1)        # 避免空转
            continue
        task_manager.update_job_status(job, JobStatus.RUNNING)

        try:
            # Step 2: execute  job
            result = executor.execute(job)
            task_manager.update_job_status(job, JobStatus.SUCCESS)
            result_store.set(job.id, result)
        except Exception as e:
            task_manager.update_job_status(job, JobStatus.FAILED)
            job.error = str(e)
            get_default_logger().error(f"Job failed: {job.id}, error={e}")
            print(f"Job failed: {job.id}, {e}")
        finally:
            task_manager.release_concurrency_slot(job)

        job_queue.task_done()


def start_workers(n=4):
    for i in range(n):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.name = f"api-service-worker-{i}"
        t.start()
        get_default_logger().info(f"Worker thread started: {t.name}")
        print(f"Worker thread started: {t.name}")