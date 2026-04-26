from core.process_pool.executor_task import AbstractExectuorTask
import akshare as ak
from typing import Dict, Any
from loguru import logger
import pandas as pd
from datetime import datetime
from utils.http import patch_requests
from timeout_decorator import timeout  # 导入超时装饰器




class WorkerAkshareAHistSinaFetcher(AbstractExectuorTask):
    def run(self, params: Dict[str, Any]) -> pd.DataFrame | None:
        patch_requests()
        try:
            symbol = params["symbol"]
            start_date = params["start_date"].strftime("%Y%m%d")
            end_date = params.get("end_date", datetime.now()).strftime("%Y%m%d")
            adjust = params.get("adjust", "qfq")
            df = self._fetch(symbol, start_date, end_date, adjust)

            # 判空
            if df is None or df.empty:
                logger.warning(f"[AKShare Worker] 无数据：{symbol}")
                return None

            logger.info(f"[AKShare Worker] 获取成功：{symbol} 数据量 {len(df)}")
            return df

        except Exception as e:
            logger.error(f"[AKShare Worker] 失败 {symbol}：{str(e)}", exc_info=True)
            return None

    @timeout(10)
    def _fetch(self, symbol, start_date, end_date, adjust):
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust)
            
        return df

