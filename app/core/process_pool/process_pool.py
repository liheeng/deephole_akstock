import os
import sys
import time
import traceback
import importlib
import queue
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from typing import Optional

from loguru import logger
from core.process_pool.executor_task import ExectuorTaskCfg
from utils.task_util import TaskResult




# ========================
# 抽象基类（自带自动释放）
# ========================
class BaseExecutor(ABC):
    def __init__(self, pool: ProcessPoolExecutor):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_root not in sys.path:
            sys.path.append(project_root)
        self._pool = pool  # 仅主进程使用
        self._pool_ref: Optional[ExProcessExecutorPool] = None  # 用来归还worker

    def set_pool_ref(self, pool_ref: "ExProcessExecutorPool"):
        self._pool_ref = pool_ref

    def release(self):
        """自动归还worker"""
        if self._pool_ref:
            self._pool_ref.release(self)

    @abstractmethod
    def run(self, executor_task: ExectuorTaskCfg, timeout: Optional[int] = None) -> TaskResult:
        pass


# ========================
# 动态执行器
# ========================
class DynamicTaskExecutor(BaseExecutor):
    def run(
        self,
        executor_task: ExectuorTaskCfg,
        timeout: Optional[int] | None = None
    ) -> TaskResult:
        if not executor_task.task_module_file or not executor_task.task_class_name:
            raise ValueError("task_module_file or task_class_name is empty")

        try:
            future = self._pool.submit(
                self._task_runner,
                executor_task.task_module_file,
                executor_task.task_class_name,
                executor_task.task_params,
                executor_task.call_before_task,
                executor_task.call_after_task
            )
            return future.result(timeout=timeout)
        finally:
            # ✅ 自动归还！
            self.release()

    @staticmethod
    def _task_runner(
        task_module_file, task_class_name, task_params, call_before_task, call_after_task
    ) -> TaskResult:
        start_time = time.time()
        result = TaskResult(
            status=False
        )
        try:
            module = importlib.import_module(task_module_file)
            cls = getattr(module, task_class_name)
            instance = cls()
            if call_before_task:
                instance = call_before_task(instance, task_params)
            data = instance.run(task_params)
            result.status = True
            result.data = data
            result.message = "执行成功"
            if call_after_task:
                result = call_after_task(result, task_params)
        except Exception as e:
            result.status = False
            result.message = "执行失败"
            result.error = f"{str(e)}\n{traceback.format_exc()}"
            logger.exception(f"任务执行异常: {e}", exc_info=True)
        finally:
            result.time = round(time.time() - start_time, 3)
        return result


# ========================
# 预初始化执行器
# ========================
class PreInitTaskExecutor(BaseExecutor):
    def __init__(
        self,
        pool: ProcessPoolExecutor,
        executor_task: ExectuorTaskCfg
    ):
        super().__init__(pool)
        self.task_module_file = executor_task.task_module_file
        self.task_class_name = executor_task.task_class_name
        self.call_before_task = executor_task.call_before_task
        self.call_after_task = executor_task.call_after_task

    def run(self, executor_task: ExectuorTaskCfg, timeout: Optional[int] = None) -> TaskResult:
        try:
            future = self._pool.submit(
                self._task_runner,
                self.task_module_file,
                self.task_class_name,
                executor_task.task_params,
                self.call_before_task,
                self.call_after_task
            )
            return future.result(timeout=timeout)
        finally:
            # ✅ 自动归还！
            self.release()

    @staticmethod
    def _task_runner(task_module_file, task_class_name, task_params, call_before_task, call_after_task) -> TaskResult:
        if not hasattr(PreInitTaskExecutor._task_runner, "instance_cache"):
            PreInitTaskExecutor._task_runner.instance_cache = {}

        key = (task_module_file, task_class_name)
        instance = PreInitTaskExecutor._task_runner.instance_cache.get(key)

        start_time = time.time()
        result = TaskResult(
            status=False
        )

        try:
            if instance is None:
                logger.info(f"🔸 子进程初始化任务: {task_module_file}.{task_class_name}")
                module = importlib.import_module(task_module_file)
                cls = getattr(module, task_class_name)
                instance = cls()
                PreInitTaskExecutor._task_runner.instance_cache[key] = instance

            if call_before_task:
                instance = call_before_task(instance, task_params)

            data = instance.run(task_params)
            result.status = True
            result.data = data
            result.message = "执行成功"

            if call_after_task:
                result = call_after_task(result, task_params)
            
        except Exception as e:
            result.status = False
            result.message = "预初始化任务执行失败"
            result.error = f"{str(e)}\n{traceback.format_exc()}"
            logger.exception(f"预初始化任务执行异常: {e}", exc_info=True)
        finally:
            result.time = round(time.time() - start_time, 3)

        return result


# ========================
# 进程池
# ========================
class ExProcessExecutorPool:
    def __init__(self, max_workers: int = 3):
        self.max_workers = max_workers
        self.pool = ProcessPoolExecutor(max_workers=max_workers)
        self.idle_workers = queue.Queue(maxsize=max_workers)
        self.mode = ""
        self._shutdown = False

    def init_default_workers(self) -> bool:
        if self.mode:
            logger.warning(f"Cannot re-init, current is initialized as {self.mode}")
            return False

        for _ in range(self.max_workers):
            worker = DynamicTaskExecutor(self.pool)
            worker.set_pool_ref(self)
            self.idle_workers.put(worker)

        self.mode = "DynamicTaskExecutor"
        return True

    def init_preinit_workers(self, task_executor: ExectuorTaskCfg) -> bool:
        if self.mode:
            logger.warning(f"Cannot re-init, current is initialized as {self.mode}")
            return False

        for _ in range(self.max_workers):
            worker = PreInitTaskExecutor(self.pool, task_executor)
            worker.set_pool_ref(self)
            self.idle_workers.put(worker)

        self.mode = "PreInitTaskExecutor"
        return True

    def acquire(self, block=True) -> BaseExecutor:
        return self.idle_workers.get(block=block)

    def release(self, executor: BaseExecutor | None = None):
        try:
            if executor and not self._shutdown:
                self.idle_workers.put(executor, block=False)
        except Exception:
            pass

    def shutdown(self, wait=True):
        if self.pool and not self._shutdown:
            self._shutdown = True
            self.pool.shutdown(wait=wait)
            self.pool = None

    def __del__(self):
        self.shutdown(wait=False)


# ========================
# 使用示例（无需手动 release！）
# ========================
if __name__ == "__main__":
    # 动态执行器
    executor_pool = ExProcessExecutorPool(max_workers=3)
    executor_pool.init_default_workers()

    task_cfg = ExectuorTaskCfg(
        task_module_file="sources.akshare.akshare_worker_download",
        task_class_name="AkshareWorkerDownload",
        task_params={"symbol": "sz000001", "start_date": "2026-03-01", "adjust": "qfq"}
    )
    executor = executor_pool.acquire()
    result = executor.run(task_cfg, timeout=1500)
    print(result)
    # ✅ 不需要手动 executor_pool.release(executor)
    executor_pool.shutdown()

    # 预初始化执行器
    executor_pool2 = ExProcessExecutorPool(max_workers=1)
    task_cfg = ExectuorTaskCfg(
        task_module_file="sources.akshare.akshare_worker_download",
        task_class_name="AkshareWorkerDownload",
    )
    executor_pool2.init_preinit_workers(task_cfg)

    executor2 = executor_pool2.acquire()
    task_cfg2 = ExectuorTaskCfg(
        task_params={"symbol": "sh601899", "start_date": "2026-03-01", "adjust": "qfq"}
    )
    result2 = executor2.run(task_cfg2, timeout=1500)
    print("------------- SH601899")
    print(result2)

    executor2 = executor_pool2.acquire()
    task_cfg2 = ExectuorTaskCfg(
        task_params={"symbol": "sh688090", "start_date": "2026-03-01", "adjust": "qfq"}
    )
    result2 = executor2.run(task_cfg2, timeout=1500)
    print("------------- SH688090")
    print(result2)

    # ✅ 不需要手动 release
    executor_pool2.shutdown()