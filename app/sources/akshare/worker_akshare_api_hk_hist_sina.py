from core.process_pool.executor_task import AbstractExectuorTask
import akshare as ak
from typing import Dict, Any
from utils.log_manager import get_logger
import pandas as pd
from datetime import datetime
from utils.http import patch_requests
from timeout_decorator import timeout

logger = get_logger(__name__)


class WorkerAkshareHKHistSinaFetcher(AbstractExectuorTask):
    def run(self, params: Dict[str, Any]) -> pd.DataFrame | None:
        patch_requests()
        try:
            symbol = params["symbol"]
            adjust = params.get("adjust", "")
            start_date = params.get("start_date", pd.to_datetime("19000101"))
            end_date = params.get("end_date", datetime.now())
            
            # 获取全量港股数据
            df = self._fetch(symbol, adjust)

            # 判空
            if df is None or df.empty:
                logger.warning(f"[AKShare HK Worker] 无数据：{symbol}")
                return None
            
            # 过滤日期范围
            df['date'] = pd.to_datetime(df['date'])
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            df_filtered = df[mask].copy()
            
            if df_filtered.empty:
                logger.warning(f"[AKShare HK Worker] {symbol} 在指定日期范围[{start_date}~{end_date}]内无数据")
                return None

            logger.info(f"[AKShare HK Worker] 获取成功：{symbol} 过滤后数据量 {len(df_filtered)}")
            return df_filtered

        except Exception as e:
            logger.error(f"[AKShare HK Worker] 失败 {symbol}：{str(e)}", exc_info=True)
            return None

    @timeout(10)
    def _fetch(self, symbol, adjust):
        """核心获取港股数据方法，带超时控制"""
        df = ak.stock_hk_daily(
            symbol=symbol,
            adjust=adjust
        )
        return df