import subprocess
import threading
from core.job import Job
from executors.base import register_executor
from core.log_stream import publish_log
from core.job import JobType, JobStatus
from .base import ExecutorBase
from core.task_manager import task_manager
from typing import Dict
import os
from utils.common import is_running_in_docker, create_file_with_dirs


running_processes: Dict[str, subprocess.Popen] = {}

SCRIPT_RUNNER_PATH = "/temp" if is_running_in_docker() else "./.temp"
SCRIPT_RUNNER_PATH = f"{SCRIPT_RUNNER_PATH}/script_runner.py"
create_file_with_dirs(SCRIPT_RUNNER_PATH)

if not os.path.exists(SCRIPT_RUNNER_PATH):
    raise FileNotFoundError(f"script_runner not found: {SCRIPT_RUNNER_PATH}")


@register_executor(JobType.PYTHON_SCRIPT.value)
class PythonScriptExecutor(ExecutorBase):

    def execute_job(self, job: Job):
        script = job.params.get("script")
        job_id = job.id

        if not script:
            raise ValueError("script is empty")

        publish_log(job_id, "Starting Python script...")

        proc = subprocess.Popen(
            # ["python", "-u", "-m", "runtime.script_runner", job_id],
            ["python", "-u", SCRIPT_RUNNER_PATH, job_id],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # 行缓冲（关键！保证实时输出）
        )

        running_processes[job_id] = proc

        stdout_buffer = []
        stderr_buffer = []

        # ✅ stdout 实时读取
        def stream_output():
            try:
                for line in iter(proc.stdout.readline, ''):
                    if not line:
                        break
                    line = line.rstrip()
                    stdout_buffer.append(line)
                    publish_log(job_id, line, "INFO")
            finally:
                proc.stdout.close()

        # ✅ stderr 实时读取
        def stream_error():
            try:
                for line in iter(proc.stderr.readline, ''):
                    if not line:
                        break
                    line = line.rstrip()
                    stderr_buffer.append(line)
                    publish_log(job_id, line, "ERROR")
            finally:
                proc.stderr.close()

        t1 = threading.Thread(target=stream_output, daemon=True)
        t2 = threading.Thread(target=stream_error, daemon=True)

        t1.start()
        t2.start()

        try:
            # ✅ 一次性写入 + flush（关键）
            proc.stdin.write(script)
            proc.stdin.flush()
            proc.stdin.close()
        except Exception as e:
            publish_log(job_id, f"Failed to send script to process: {e}", "ERROR")
            proc.kill()
            raise

        # 等待进程结束
        proc.wait()

        t1.join()
        t2.join()

        running_processes.pop(job_id, None)

        # ❗关键：把 stderr 带出来
        if proc.returncode != 0:
            error_msg = "\n".join(stderr_buffer[-50:])  # 最多保留50行
            raise Exception(
                f"Script failed with code {proc.returncode}\n"
                f"==== STDERR ====\n{error_msg}"
            )

        publish_log(job_id, "Script completed successfully")

        return "OK"
    
    def cancel_job(self, job_id: str) -> bool:
        proc = running_processes.get(job_id)

        if not proc:
            return False

        publish_log(job_id, "Cancelling job...", "WARNING")

        try:
            # ✅ 如果还在运行
            if proc.poll() is None:
                # 先尝试优雅终止
                proc.terminate()

                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    publish_log(job_id, "Force killing job...", "WARNING")
                    proc.kill()
                    proc.wait()

            publish_log(job_id, "Job cancelled", "WARNING")

        except Exception as e:
            publish_log(job_id, f"Error cancelling job: {e}", "ERROR")

        finally:
            # ✅ 清理进程表
            running_processes.pop(job_id, None)

            # ✅ 更新状态
            job = task_manager.load_job(job_id)
            if job:
                task_manager.update_job_status(job, JobStatus.CANCELLED)

        return True

