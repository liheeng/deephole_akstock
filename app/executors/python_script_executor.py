import subprocess
import threading
from core.job import Job
from executors.base import register_executor
from core.log_stream import publish_log
from core.job import JobType

running_processes = {}  # 用于 cancel


@register_executor(JobType.PYTHON_SCRIPT.value)
class PythonScriptExecutor:

    def execute(self, job: Job):
        script = job.params.get("script")
        job_id = job.id

        publish_log(job_id, "Starting Python script...")

        proc = subprocess.Popen(
            ["python", "-u", "-m", "runtime.script_runner", job_id],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        running_processes[job_id] = proc

        # 把脚本写入 stdin
        proc.stdin.write(script)
        proc.stdin.close()

        # 异步读取 stdout
        def stream_output():
            for line in proc.stdout:
                publish_log(job_id, line.strip(), "INFO")

        def stream_error():
            for line in proc.stderr:
                publish_log(job_id, line.strip(), "ERROR")

        t1 = threading.Thread(target=stream_output)
        t2 = threading.Thread(target=stream_error)

        t1.start()
        t2.start()

        proc.wait()

        t1.join()
        t2.join()

        running_processes.pop(job_id, None)

        if proc.returncode != 0:
            raise Exception(f"Script failed with code {proc.returncode}")

        publish_log(job_id, "Script completed successfully")

        return "OK"
    
    def cancel_job(self, job_id: str) -> bool:
        proc = running_processes.get(job_id)
        if proc:
            proc.kill()
            return True
        return False
    
# import time
# import json
# from core.job import JobType, Job
# from executors.base import register_executor
# import subprocess
# import tempfile
# from loguru import logger
# from textwrap import indent


# def log(job_id, message, level="INFO"):
#     redis.publish(f"log:{job_id}", json.dumps({
#         "level": level,
#         "msg": message,
#         "ts": time.time()
#     }))


# def stream_process_output(proc, job_id):
#     for line in proc.stdout:
#         log(job_id, line.strip())

#     err = proc.stderr.read()
#     if err:
#         log(job_id, err, "ERROR")

#     return proc.wait()


# def build_wrapper_script(user_script, job_id):
#     return f"""
# from task_sdk import net, storage, log

# log.init("{job_id}")

# try:
# {indent(user_script, "    ")}
# except Exception as e:
#     log.error(str(e))
#     raise
# """


# def run_user_script(script: str, job_id: str):
#     with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
#         f.write(build_wrapper_script(script, job_id))
#         path = f.name

#     proc = subprocess.Popen(
#         ["python", path],
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#         text=True
#     )

#     return stream_process_output(proc, job_id)


# @register_executor(JobType.PYTHON_SCRIPT.value)
# class PythonScriptExecutor:

#     def execute(self, job: Job):
#         script = job.params.get("script")
#         job_id = job.id

#         if script:
#             return run_user_script(script, job_id)
#         else:
#             logger.warning("script is missing in job!")