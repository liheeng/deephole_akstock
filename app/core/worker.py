# app/core/worker.py

import threading
from time import time
from core.task import Job, JobStatus
from core.queue import job_queue
from executors.base import get_executor
from core.result_store import result_store
from utils.log_manager import get_default_logger

def worker_loop():
    while True:
        job: Job = job_queue.get()

        executor = get_executor(job.metadata.type)

        try:
            job.update_status(JobStatus.RUNNING)
            job.execute_time = time.time()
            result = executor.execute(job)
            job.update_status(JobStatus.COMPLETED)
            result_store.set(job.metadata.id, result)
        except Exception as e:
            job.update_status(JobStatus.FAILED)
            job.error = str(e)
            get_default_logger().error(f"Job failed: {job.metadata.id}, error={e}")
            print(f"Job failed: {job.metadata.id}, {e}")
        finally:
            job.stop_time = time.time()

        job_queue.task_done()


def start_workers(n=4):
    for i in range(n):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.name = f"api-service-worker-{i}"
        t.start()
        get_default_logger().info(f"Worker thread started: {t.name}")
        print(f"Worker thread started: {t.name}")