# app/core/scheduler.py

from collections import defaultdict
from core.queue import job_queue
from core.task import Task, Job

def run_task(task: Task):
    completed = set() # The completed set contains those jobs which is scheduled and be added into queue, and "ready to run".
    job_map = {job.metadata.id: job for job in task.jobs}

    while len(completed) < len(task.jobs):
        for job in task.jobs:
            if job.metadata.id in completed:
                continue
            
            if not job.metadata.depends_on:  # No dependencies, can be scheduled directly
                job_queue.put(job)
                completed.add(job.metadata.id)
            elif all(dep in completed for dep in job.metadata.depends_on):
                job_queue.put(job)
                completed.add(job.metadata.id)