import pandas as pd
from loguru import logger
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceApiName, StartPriority
from sources.datasource_adapter import SymbolConverter
from datetime import datetime
from core.paraller_job_executor import ParallelJobExecutor, ParallelJob
from utils.task_util import TaskResult
from typing import Dict, Callable, List




class ParallelHistFetcher():
    pje: ParallelJobExecutor
    parallel_jobs: List[ParallelJob]

    def __init__(self, 
                 symbols_str: str, 
                 options: dict,
                 api_type: DataSourceApiName,
                 symbol_converter: SymbolConverter,
                 executor_callback: Callable[[ParallelJob], TaskResult],
                 noremalize_callback: Callable[[pd.DataFrame, str], pd.DataFrame] | None = None,
                 max_workers=10,
                 max_retry=3,
                 retry_interval=1) -> None:
        self.pje = ParallelJobExecutor(
            job_result_assemble_callback=self._on_job_concate,
            max_workers=max_workers,
            max_retry=max_retry,
            retry_interval=retry_interval
        )
        self.api_type = api_type
        self.executor_callback = executor_callback
        self.noremalize_callback = noremalize_callback
        self.parallel_jobs = self._init_jobs(symbols_str, options, symbol_converter)

    def _init_jobs(self, 
                   symbols_str: str, 
                   options: dict, 
                   symbol_converter: SymbolConverter): 
        parallel_jobs = []
        symbols_last_dates = options.get("symbols_last_dates", {})

        for origin_symbol in symbols_str.split(","):
            code = symbol_converter.convert(origin_symbol)
            start_date = options.get("start", pd.to_datetime("1900-01-01"))
            if options.start_priority == StartPriority.DATABASE:
                last_date = symbols_last_dates.get(origin_symbol)
                start_date = last_date if last_date else start_date

            end_date = options.get("end", datetime.now())
            adjust = options.get("adjust", "qfq")
            parallel_jobs.append(
                ParallelJob(
                    name=f"{self.api_type.value} - {origin_symbol}",
                    job_params={"origin_symbol": origin_symbol, "symbol": code, "start_date": start_date, "end_date": end_date, "adjust": adjust},
                    job_callback=self._on_job
                    # job_result_extra_callback=on_job_done,
                    # extra_params={"origin_symbol": origin_symbol}
                )
            )
        self.parallel_jobs = parallel_jobs
        return parallel_jobs

    def _on_job(self, job: ParallelJob) -> pd.DataFrame | None:
        origin_symbol = job.job_params["origin_symbol"]
        # executor = executor_pool.acquire(block=True)
        # task_cfg.task_params = job.job_params.copy()
        # result = executor.run(task_cfg, timeout=10)
        result = self.executor_callback(job)
        if result.status is False:
            logger.error(f"❌ Job [{job.name}] 执行异常: {result.error}")
            return None
        
        df = result.data

        if df is not None and not df.empty:
            print(f"{self.api_type.value} success: {origin_symbol}")
            logger.info(f"{self.api_type.value} success: {origin_symbol}")
            return self.noremalize_callback(df, origin_symbol) if self.noremalize_callback else df
        else:
            print(f"{self.api_type.value} failed: {origin_symbol}")
            logger.error(f"{self.api_type.value} failed: {origin_symbol}")
            return None
    
    def _on_job_concate(self, results: Dict[ParallelJob, pd.DataFrame]) -> Dict[str, pd.DataFrame] | None:
        new_his_data = {}
        for job in results.keys():
            new_his_data[job.job_params["origin_symbol"]] = results[job]
        return new_his_data
    
    def run(self) -> Dict[str, pd.DataFrame] | None:
        return self.pje.execute(self.parallel_jobs)
    
    def stop(self):
        self.pje.shutdown(wait=True)
