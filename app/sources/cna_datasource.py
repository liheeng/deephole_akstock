import akshare as ak
import pandas as pd
from utils.log_manager import get_logger
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceAPI, DataSource
from sources.datasource_adapter import convert_symbol
from sources.ifind.ifind_api import IfindApi, HIS_BATCH_SIZE_LIMIT, HIS_BATCH_SYMBOLS_LIMIT
from datetime import datetime
from typing import Dict, List
from core.paraller_job_executor import ParallelJobExecutor, ParallelJob

logger = get_logger(__name__)


class AKshareSinaCNASource:
    source_api_type: DataSourceAPI = DataSourceAPI.AKSHARE_SINA_API
    
    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        新浪格式转换
        """
        df = df.copy()

        df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
            "turnover": "turnover",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "turnover"
        ]]

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        end = end or datetime.now()
        _days = (end.date() - start.date()).days
        _days = 1 if _days == 0 else _days
        count = HIS_BATCH_SIZE_LIMIT // _days
        count = HIS_BATCH_SYMBOLS_LIMIT if count > HIS_BATCH_SYMBOLS_LIMIT else count
        last_index = next_index + count
        if last_index > len(symbols):
            last_index = len(symbols)
        symbols_str = ""
        for symbol in symbols[next_index:last_index]:
            symbols_str = symbol if len(symbols_str) == 0 else f"{symbols_str},{symbol}"

        return next_index + count, symbols_str
    
    def fetch_daily(self, symbols_str, start: datetime) -> Dict[str, pd.DataFrame] | pd.DataFrame | None:

        # code = symbol.split(".")[-1].lower()  + symbol.split(".")[0]
        # code = convert_symbol(symbols_str, DataSourceType.AKSHARE, Region.CN, self.source_api_type)
        start = start.strftime("%Y%m%d")

        def on_job(job: ParallelJob) -> pd.DataFrame | None:
            df = ak.stock_zh_a_daily(
                symbol=job.parameters["symbol"],
                start_date=job.parameters["start_date"],
                adjust=job.parameters["adjust"])
            if df is not None and not df.empty:
                print(f"[SINA] success: {symbols_str}")
                logger.info(f"[SINA] success: {symbols_str}")
                return self.normalize(df, symbols_str)
            else:
                print(f"[SINA] failed: {symbols_str}")
                logger.error(f"[SINA] failed: {symbols_str}")
                return None
        
        def on_job_done(df: pd.DataFrame | None, job: ParallelJob) -> pd.DataFrame | None:
            if df is None:
                return None
            logger.info(f"✅ Job [{job.name}] 完成！")
            return self.normalize(df, job.extra_params["origin_symbol"])
            
        def on_job_conate(results: Dict[ParallelJob, pd.DataFrame]) -> Dict[str, pd.DataFrame] | None:
            new_his_data = {}
            for job in paraller_jobs:
                new_his_data[job.extra_params["origin_symbol"]] = results[job]
            return new_his_data

        pje = ParallelJobExecutor(
            job_result_assemble_callback=on_job_conate,
            max_workers=20,
            max_retry=3,
            retry_interval=1
        )
            
        codes_str = ""
        origin_symbols_list = []
        paraller_jobs = []
        for symbol in symbols_str.split(","):
            origin_symbols_list.append(symbol)
            symbol = convert_symbol(symbols_str, DataSourceType.AKSHARE, Region.CN, self.source_api_type)
            codes_str = symbol if len(codes_str) == 0 else f"{codes_str},{symbol}"
            paraller_jobs.append(
                ParallelJob(
                    name=f"AKshareSinaCNASource - {symbol}",
                    parameters={"symbol": symbol, "start_date": start, "adjust": "qfq"},
                    job_callback=on_job,
                    job_result_process_callback=on_job_done,
                    extra_params={"origin_symbol": symbol}
                )
            )

        return pje.execute(paraller_jobs)

        # # 🥇 尝试新浪
        # try:
        #     df = ak.stock_zh_a_daily(
        #         symbol=code,
        #         start_date=start,
        #         adjust="qfq"
        #     )

        #     if df is not None and not df.empty:
        #         print(f"[SINA] success: {symbols_str}")
        #         logger.info(f"[SINA] success: {symbols_str}")
        #         return self.normalize(df, symbols_str)

        # except Exception as e:
        #     print(f"[SINA] failed: {symbols_str}, error={e}")
        #     logger.error(f"[SINA] failed: {symbols_str}, error={e}")  
        #     raise e  # 上层重试


class AKshareTencentCNASource:
    source_api_type: DataSourceAPI = DataSourceAPI.AKSHARE_TENCENT_API
    
    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        腾讯格式转换
        """
        df = df.copy()

        df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "amount": "amount",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "amount"
        ]]

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        return next_index + 1, symbols[next_index]

    def fetch_daily(self, symbols_str, start: datetime) -> pd.DataFrame | None:

        # code = symbol.split(".")[-1].lower()  + symbol.split(".")[0]
        code = convert_symbol(symbols_str, DataSourceType.AKSHARE, Region.CN, self.source_api_type)
        start = start.strftime("%Y%m%d")
        # 尝试腾讯
        try:
            df = ak.stock_zh_a_hist_tx(
                symbol=code,
                start_date=start,
                adjust="qfq"
            )

            if df is not None and not df.empty:
                print(f"[TENCENT] success: {symbols_str}")
                logger.info(f"[TENCENT] success: {symbols_str}")
                return self.normalize(df, symbols_str)

        except Exception as e:
            print(f"[TENCENT] failed: {symbols_str}, error={e}")
            logger.error(f"[TENCENT] failed: {symbols_str}, error={e}")
            raise e  # 上层重试


class IFinDCNASource:
    """ 同花顺iFinD
    """
    source_api_type: DataSourceAPI = DataSourceAPI.IFIND_API

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        iFinD格式转换
        """
        df = df.copy()

        df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "amount",
            "changeRatio": "pct",
            "turnoverRatio": "turnover",
        }, inplace=True)

        df["symbol"] = symbol

        return df[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "pct", "turnover"    # noqa
        ]]
    
    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        end = end or datetime.now()
        _days = (end.date() - start.date()).days
        _days = 1 if _days == 0 else _days
        count = HIS_BATCH_SIZE_LIMIT // _days
        count = HIS_BATCH_SYMBOLS_LIMIT if count > HIS_BATCH_SYMBOLS_LIMIT else count
        last_index = next_index + count
        if last_index > len(symbols):
            last_index = len(symbols)
        symbols_str = ""
        for symbol in symbols[next_index:last_index]:
            symbols_str = symbol if len(symbols_str) == 0 else f"{symbols_str},{symbol}"

        return next_index + count, symbols_str

    def fetch_daily(self, symbols_str: str, start: datetime) -> pd.DataFrame | Dict[str, pd.DataFrame] | None:
        
        if IfindApi.instance() is None or not IfindApi.instance().is_available():
            raise Exception("iFinD is not available")

        codes_str = ""
        origin_symbols_list = []
        for symbol in symbols_str.split(","):
            origin_symbols_list.append(symbol)
            symbol = convert_symbol(symbol, DataSourceType.IFIND, Region.CN)
            codes_str = symbol if len(codes_str) == 0 else f"{codes_str},{symbol}"    
            
        # iFinD
        try:
            his_data: Dict[str, pd.DataFrame] = IfindApi.instance().get_historical_data(
                codes=codes_str,
                start=start.strftime("%Y-%m-%d")
            ) # type: ignore

            if his_data is None or len(his_data) == 0:
                return None
            
            print(f"[iFinD] success: {symbols_str}")
            logger.info(f"[iFinD] success: {symbols_str}")
            new_his_data = {}
            his_data_keys = list(his_data.keys())
            for i in range(len(his_data_keys)):
                new_his_data[origin_symbols_list[i]] = self.normalize(his_data[his_data_keys[i]], origin_symbols_list[i])
            
            return new_his_data

        except Exception as e:
            print(f"[iFinD] failed: {symbols_str}, error={e}")
            # logger.error(f"[iFinD] failed: {symbols_str}, error={e}")
            raise e  # 上层重试


class CNAStockSource(DataSource):
    source_api_list = []
    source_api_cursor: int

    def __init__(self):
        super().__init__()
        self.source_api_list = [
            # IFinDCNASource,
            AKshareSinaCNASource,
            AKshareTencentCNASource
        ]
        self.source_api_cursor = -1

    def prepare_fetch(self):
        # Reset cursor to use first source API
        self.source_api_cursor = -1

    def candidate_symbols(self,
                          symbols: List[str],
                          next_index: int,
                          start: datetime,
                          end: datetime | None) -> tuple[int, str]:
        cursor = self.source_api_cursor + 1
        if cursor >= len(self.source_api_list):
            cursor = 0

        return self.source_api_list[cursor]().candidate_symbols(symbols, next_index, start, end)
    
    def fetch_daily(self, symbols_str, start: datetime) -> pd.DataFrame | Dict[str, pd.DataFrame] | None:
        self.source_api_cursor += 1
        if self.source_api_cursor >= len(self.source_api_list):
            self.source_api_cursor = 0
    
        source_api = self.source_api_list[self.source_api_cursor]
        
        try:
            instance = source_api()
            source_api_name = instance.source_api_type.value
            logger.info(f"trying {source_api_name} API for {symbols_str} daily data since {start}")
            return instance.fetch_daily(symbols_str, start)
        except Exception as e:
            logger.exception(f"trying from {source_api_name} failed: {symbols_str}, error={e}")
            raise e
