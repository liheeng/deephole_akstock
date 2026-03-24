# app/core/scheduler.py

from collections import defaultdict
from core.queue import job_queue
from core.task import Task, TaskStatus
from core.job import JobStatus
from core.task_manager import task_is_allowed, task_can_run, task_manager
from utils.log_manager import get_default_logger
from core.error import TaskError, ERROR_CODE

def run_task(task: Task) -> bool:
    if not task_is_allowed(task):
        emsg = f"some jobs in task({task.id}-{task.description})) is  not allowed, it is singleton."
        err = TaskError(ERROR_CODE.ERROR_TASK_NOT_ALLOWED, emsg)
        get_default_logger().error(err)
        raise err
        
    _task = task_manager.load_save_task(task)
    if _task != task:
        get_default_logger.warning(f"load same task {task.id} from DB!")

    if not task_can_run(_task):
        wmsg = f"task {_task.id} - ({_task.description}) cannot be run!"
        get_default_logger().warning(wmsg)
        task_manager.update_task_status(task, TaskStatus.SUSPENDED)
        emsg = f"some jobs in task({task.id}-{task.description})) cannot be run, it does not meet job dag condition or concurrency limit or exceed retry limit, please check job policy!"
        err = TaskError(ERROR_CODE.ERROR_TASK_CAN_NOT_RUN, emsg)
        get_default_logger().error(err)
        raise err
    
    completed = set() # The completed set contains those jobs which is scheduled and be added into queue, and "ready to run".
    job_map = {job.id: job for job in _task.jobs}

    while len(completed) < len(_task.jobs):
        for job in _task.jobs:
            if job.id in completed:
                continue
            
            if not job.depends_on:  # No dependencies, can be scheduled directly
                job_queue.put(job)
                task_manager.update_job_status(job, JobStatus.QUEUED)
                completed.add(job.id)
            elif all(dep in completed for dep in job.depends_on):
                job_queue.put(job)
                task_manager.update_job_status(job, JobStatus.QUEUED)
                completed.add(job.id)

    _task = task_manager.update_task_status(_task, TaskStatus.SUBMITTED)
    return True