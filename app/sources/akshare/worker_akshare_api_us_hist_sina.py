from core.process_pool.executor_task import AbstractExectuorTask
import akshare as ak
from typing import Dict, Any
from loguru import logger
import pandas as pd
from datetime import datetime
from utils.http import patch_requests
from timeout_decorator import timeout
from sources.datasource_adapter import fix_preferred_symbol




class WorkerAkshareUSHistSinaFetcher(AbstractExectuorTask):
    def run(self, params: Dict[str, Any]) -> pd.DataFrame | None:
        patch_requests()
        try:
            # 任务参数
            symbol = params["symbol"]
            adjust = params.get("adjust", "")
            start_date = params.get("start_date", pd.to_datetime("1900-01-01"))
            end_date = params.get("end_date", datetime.now())

            # 获取全量美股日线数据
            df = self._fetch(symbol, adjust)
            if df is None or df.empty:
                logger.warning(f"[AKShare US Worker] 无数据: {symbol}")
                return None

            # 本地过滤日期
            df['date'] = pd.to_datetime(df['date'])
            mask = (df['date'] >= start_date) & (df['date'] <= end_date)
            df_filtered = df[mask].copy()

            if df_filtered.empty:
                logger.warning(f"[AKShare US Worker] 日期范围内无数据: {symbol} [{start_date}~{end_date}]")
                return None

            logger.info(f"[AKShare US Worker] 成功: {symbol} 数据量 {len(df_filtered)}")
            return df_filtered

        except Exception as e:
            logger.error(f"[AKShare US Worker] 执行失败 {symbol}: {str(e)}", exc_info=True)
            return None

    @timeout(15)
    def _fetch(self, symbol, adjust):
        """新浪美股接口获取数据"""
        df = ak.stock_us_daily(
            symbol=symbol,
            adjust=adjust
        )
        return df