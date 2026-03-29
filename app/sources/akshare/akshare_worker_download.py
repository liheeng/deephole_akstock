from core.process_pool.executor_task import AbstractExectuorTask, ExecutorTaskResult
import akshare as ak
from typing import Dict, Any
from utils.log_manager import get_logger
import pandas as pd
from utils.http import patch_requests

logger = get_logger(__name__)


class AkshareWorkerDownload(AbstractExectuorTask):
    def run(self, params: Dict[str, Any]) -> pd.DataFrame | None:
        patch_requests()

        symbol = params["symbol"]
        start_date = params["start_date"]
        adjust = params["adjust"]
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            adjust=adjust)

        return df

