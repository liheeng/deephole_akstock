import asyncio
from collections import defaultdict
from datetime import datetime

# 每个 job 一个队列
log_queues = defaultdict(asyncio.Queue)


def publish_log(job_id: str, message: str, level="INFO"):
    queue = log_queues[job_id]

    log = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "job_id": job_id,
        "level": level,
        "message": message,
    }

    try:
        queue.put_nowait(log)
    except Exception:
        pass


async def subscribe_logs(job_id: str):
    queue = log_queues[job_id]
    while True:
        log = await queue.get()
        yield log