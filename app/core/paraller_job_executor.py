import time
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Generic, TypeVar, Callable, Optional
from utils.log_manager import get_logger

T = TypeVar("T")  # 单个Job返回类型
R = TypeVar("R")  # 最终合并结果类型

logger = get_logger(__name__)


# =============================================================================
# ✅  ParallelJob：每个任务自带 名称、参数、回调、额外参数（完全自包含）
# =============================================================================
@dataclass
class ParallelJob(Generic[T]):
    name: str
    parameters: Dict[str, Any]
    job_callback: Callable[[Dict[str, Any]], T]
    job_result_process_callback: Optional[Callable[[T, "ParallelJob[T]"], None]] = None
    extra_params: Optional[Dict[str, Any]] = None


# =============================================================================
# ✅ 并行执行器：只负责并发、重试、合并
# =============================================================================
class ParallelJobExecutor(Generic[T, R]):
    def __init__(
        self,
        job_result_assemble_callback: Callable[[List[T]], R],
        max_workers: int = 5,
        max_retry: int = 3,
        retry_interval: int = 1
    ):
        self.job_result_assemble_callback = job_result_assemble_callback
        self.max_workers = max_workers
        self.max_retry = max_retry
        self.retry_interval = retry_interval

    def _run_job_with_retry(self, job: ParallelJob[T]) -> T | None:
        retries = 0
        while retries < self.max_retry:
            try:
                return job.job_callback(job.parameters)
            except Exception as e:
                retries += 1
                logger.warning(f"⚠️ Job [{job.name}] 失败 {retries}/{self.max_retry} | 错误: {str(e)}")
                time.sleep(self.retry_interval)

        logger.error(f"❌ Job [{job.name}] 最终失败！")
        return None

    def execute(self, jobs: List[ParallelJob[T]]) -> R:
        futures = []
        success_results: List[T] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for job in jobs:
                future = executor.submit(self._run_job_with_retry, job)
                futures.append((job, future))

            for job, future in futures:
                try:
                    result = future.result()
                    if result is not None:
                        success_results.append(result)

                        # ✅ 每个 Job 自己的回调
                        if job.job_result_process_callback:
                            job.job_result_process_callback(result, job)

                except Exception as e:
                    logger.error(f"❌ Job [{job.name}] 执行异常: {str(e)}")

        return self.job_result_assemble_callback(success_results)

# ===================================
# Example:
# import pandas as pd

# # ----------------------
# # 1. 定义回调
# # ----------------------
# def fetch_stock(params: Dict[str, Any]) -> pd.DataFrame:
#     return pd.DataFrame({"code": [params["code"]], "close": [10]})

# def on_job_done(df: pd.DataFrame, job: ParallelJob):
#     print(f"✅ Job [{job.name}] 完成！")
#     # 可以直接访问 job.extra_params

# # ----------------------
# # 2. 组装独立 Job
# # ----------------------
# job1 = ParallelJob(
#     name="平安银行",
#     parameters={"code": "000001"},
#     job_callback=fetch_stock,
#     job_result_process_callback=on_job_done,
#     extra_params={"key": "xxx"}
# )

# job2 = ParallelJob(
#     name="万科A",
#     parameters={"code": "000002"},
#     job_callback=fetch_stock,
#     job_result_process_callback=on_job_done
# )

# # ----------------------
# # 3. 执行器只负责合并
# # ----------------------
# def assemble(dfs: List[pd.DataFrame]) -> pd.DataFrame:
#     return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# executor = ParallelJobExecutor(assemble, max_workers=5)
# result = executor.execute([job1, job2])