import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List, Dict, Any, Generic, TypeVar, Callable, Optional
from utils.log_manager import get_logger
import threading

T = TypeVar("T")
R = TypeVar("R")

logger = get_logger(__name__)


# =============================================================================
# ParallelJob：任务自身包含所有回调、参数、名称
# =============================================================================
@dataclass(eq=False)
class ParallelJob(Generic[T]):
    name: str
    job_callback: Callable[["ParallelJob[T]"], T]
    job_params: Dict[str, Any]
    job_result_extra_callback: Optional[Callable[[T, "ParallelJob[T]"], T]] = None
    extra_params: Optional[Dict[str, Any]] = None

    def __eq__(self, other):
        if not isinstance(other, ParallelJob):
            return False
        return id(self) == id(other) and self.name == other.name

    def __hash__(self):
        return hash(id(self))


# =============================================================================
# 【升级】并行执行器：支持 shutdown / 取消任务 / 安全停止线程池
# =============================================================================
class ParallelJobExecutor(Generic[T, R]):
    def __init__(
        self,
        job_result_assemble_callback: Callable[[Dict[ParallelJob[T], T]], R],
        max_workers: int = 5,
        max_retry: int = 3,
        retry_interval: int = 1
    ):
        self.job_result_assemble_callback = job_result_assemble_callback
        self.max_workers = max_workers
        self.max_retry = max_retry
        self.retry_interval = retry_interval

        # === 新增：线程池 + 关闭控制 ===
        self.executor: Optional[ThreadPoolExecutor] = None
        self._shutdown_event = threading.Event()  # 停止信号
        self._futures: List[tuple[ParallelJob, Future]] = []  # 保存所有任务

    def _run_job_with_retry(self, job: ParallelJob[T]) -> T | None:
        retries = 0
        while retries < self.max_retry:
            # === 安全点：收到 shutdown 立即停止 ===
            if self._shutdown_event.is_set():
                logger.warning(f"[shutdown] Job [{job.name}] 已取消")
                return None

            try:
                return job.job_callback(job)
            except Exception as e:
                retries += 1
                logger.exception(f"Job [{job.name}] 失败 {retries}/{self.max_retry} | 错误: {str(e)}")

                # 停止信号检查
                if self._shutdown_event.is_set():
                    return None
                time.sleep(self.retry_interval)

        logger.error(f"Job [{job.name}] 最终失败！")
        return None

    def execute(self, jobs: List[ParallelJob[T]]) -> R:
        """启动并行执行"""
        # 重置状态
        self._shutdown_event.clear()
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self._futures.clear()
        success_results: Dict[ParallelJob[T], T] = {}

        try:
            # 提交任务
            for job in jobs:
                if self._shutdown_event.is_set():
                    break
                future = self.executor.submit(self._run_job_with_retry, job)
                self._futures.append((job, future))

            # 收集结果
            for job, future in self._futures:
                if self._shutdown_event.is_set():
                    future.cancel()
                    continue

                try:
                    result = future.result()
                    if result is not None:
                        if job.job_result_extra_callback:
                            result = job.job_result_extra_callback(result, job)
                        success_results[job] = result

                except Exception as e:
                    logger.exception(f"Job [{job.name}] 执行异常: {e}")

        finally:
            # 无论如何都关闭线程池
            self.executor.shutdown(wait=False)
            self.executor = None

        return self.job_result_assemble_callback(success_results)

    # =========================================================================
    # ✅ 你要的：SHUTDOWN 方法
    # =========================================================================
    def shutdown(self, wait: bool = True, cancel_pending: bool = True):
        """
        安全关闭执行器：
        - 停止新任务
        - 取消排队任务
        - 让正在执行的任务安全退出
        """
        logger.warning("[ParallelJobExecutor] 开始 shutdown...")

        # 1. 触发停止信号
        self._shutdown_event.set()

        # 2. 取消所有未开始的任务
        if cancel_pending and self._futures:
            for job, future in self._futures:
                if not future.done():
                    future.cancel()

        # 3. 关闭线程池
        if self.executor:
            self.executor.shutdown(wait=wait)

        logger.warning("[ParallelJobExecutor] shutdown 完成")

# ===================================
# Example:
# import pandas as pd

# # 1. 任务执行
# def fetch_job(params):
#     return pd.DataFrame({"code": [params["code"]]})

# # 2. 任务完成处理
# def on_job_done(result, job):
#     result["job_name"] = job.name
#     return result  # 返回新result

# # 3. 合并函数（接收字典）
# def assemble_results(results_dict: Dict[ParallelJob, pd.DataFrame]) -> pd.DataFrame:
#     dfs = list(results_dict.values())
#     return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# # ----------------------
# # 创建任务
# # ----------------------
# job1 = ParallelJob(
#     name="股票000001",
#     parameters={"code": "000001"},
#     job_callback=fetch_job,
#     job_result_extra_callback=on_job_done
# )

# # ----------------------
# # 执行
# # ----------------------
# executor = ParallelJobExecutor(
#     job_result_assemble_callback=assemble_results,
#     max_workers=5
# )

# df = executor.execute([job1])