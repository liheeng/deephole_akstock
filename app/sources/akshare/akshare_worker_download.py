from core.process_pool.executor_task import AbstractExectuorTask
import akshare as ak
from typing import Dict, Any
from utils.log_manager import get_logger

logger = get_logger(__name__)


class AkshareWorkerDownload(AbstractExectuorTask):
    def run(self, params: Dict[str, Any]) -> Any:
        symbol = params["symbol"]
        start = params["start"]
        adjust = params["adjust"]
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start,
            adjust=adjust)

        return {
            "status": "ok",
            "symbol": symbol,
            "rows": len(df),
            "result": df
        }
