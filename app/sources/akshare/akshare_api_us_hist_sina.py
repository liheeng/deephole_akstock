import akshare as ak
import pandas as pd
from utils.log_manager import get_logger
from utils.common import ResultStatus
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceApiName, AbstractDataSourceAPI, FetchResult, QueryOptions
from sources.datasource_adapter import SymbolConverter, fix_preferred_symbol
from datetime import datetime
from core.paraller_job_executor import ParallelJob
from core.process_pool.process_pool import ExProcessExecutorPool
from core.process_pool.executor_task import ExectuorTaskCfg
from sources.parallel_hist_fetcher import ParallelHistFetcher

logger = get_logger(__name__)


class AKshareApiUSHistoricSina(AbstractDataSourceAPI):
    source_api_type: DataSourceApiName = DataSourceApiName.AKSHARE_SINA_API
    name = DataSourceApiName.AKSHARE_SINA_API.value

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """新浪美股日线数据标准化（和港股/A股保持一致）"""
        df_new = df.copy()
        df_new["symbol"] = symbol
        return df_new[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "turnover"
        ]]

    def filter_date_range(self, df: pd.DataFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """过滤日期范围：美股接口返回全量数据，本地过滤"""
        if df is None or df.empty:
            return df

        df['date'] = pd.to_datetime(df['date'])
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df_filtered = df[mask].copy()
        return df_filtered

    def fetch_hist(self, symbols_str: str, options: QueryOptions | None = None) -> FetchResult | None:
        if options is None:
            options = QueryOptions(start=pd.to_datetime("1900-01-01"))

        symbol_converter = SymbolConverter(DataSourceType.AKSHARE, Region.US, self.source_api_type, fix_preferred_symbol)
        failed_symbols = []
        result_data = {}
        symbols = symbols_str.split(",")

        # 少量代码串行，大量并行
        if len(symbols) < 20:
            for symbol in symbols:
                try:
                    # 美股代码转换 + 优先代码修复
                    code = symbol_converter.convert(symbol)
                    
                    adjust = options.get("adjust", "")
                    # 新浪美股日线接口（无起止日期，获取全量）
                    df = ak.stock_us_daily(
                        symbol=code,
                        adjust=adjust
                    )

                    # 本地过滤日期
                    if df is not None and not df.empty:
                        start_date = options.get("start", pd.to_datetime("1900-01-01"))
                        end_date = options.get("end", datetime.now())
                        df = self.filter_date_range(df, start_date, end_date)

                    # 数据标准化
                    if df is not None and not df.empty:
                        logger.info(f"{self.name} 美股获取成功: {symbol}")
                        df = self.normalize(df, symbol)
                        result_data[symbol] = df
                    else:
                        result_data[symbol] = None
                        failed_symbols.append(symbol)

                except Exception as e:
                    failed_symbols.append(symbol)
                    logger.exception(f"{self.name} 美股获取失败: {symbol}, error={e}")

            # 构造返回结果
            fr = FetchResult(
                status=ResultStatus.SUCCESS,
                data=result_data,
                failed_symbols=failed_symbols
            )
            if len(failed_symbols) == len(symbols):
                fr.status = ResultStatus.FAILED
            elif len(failed_symbols) > 0:
                fr.status = ResultStatus.PARTIAL_SUCCESS
            return fr

        else:
            # 大量代码使用并行
            return self.fetch_hist_parallel(symbols_str, options, symbol_converter)

    def fetch_hist_parallel(self, symbols_str: str, options: QueryOptions, symbol_converter: SymbolConverter) -> FetchResult | None:
        executor_pool = None
        hist_fetcher = None
        try:
            executor_pool = ExProcessExecutorPool(max_workers=4)
            task_cfg = ExectuorTaskCfg(
                task_module_file="sources.akshare.worker_akshare_api_us_hist_sina",
                task_class_name="WorkerAkshareUSHistSinaFetcher",
            )
            executor_pool.init_preinit_workers(task_cfg)

            def executor_call(job: ParallelJob):
                executor = executor_pool.acquire(block=True)
                task_cfg.task_params = job.job_params.copy()
                return executor.run(task_cfg, timeout=15)

            hist_fetcher = ParallelHistFetcher(
                symbols_str=symbols_str,
                options=options,
                api_type=self.source_api_type,
                symbol_converter=symbol_converter,
                executor_callback=executor_call,
                noremalize_callback=self.normalize,
                max_workers=4,
                max_retry=3,
                retry_interval=1
            )

            result_data = hist_fetcher.run()
            failed_symbols = [sym for sym, df in result_data.items() if df is None or df.empty]

            fr = FetchResult(
                status=ResultStatus.SUCCESS,
                data=result_data,
                failed_symbols=failed_symbols
            )
            if len(failed_symbols) == len(symbols_str.split(",")):
                fr.status = ResultStatus.FAILED
            elif len(failed_symbols) > 0:
                fr.status = ResultStatus.PARTIAL_SUCCESS

            return fr

        except Exception as e:
            logger.exception(f"并行获取美股历史数据失败: {e}")
            all_symbols = symbols_str.split(",")
            return FetchResult(
                status=ResultStatus.FAILED,
                data={},
                failed_symbols=all_symbols,
                error=str(e)
            )

        finally:
            if hist_fetcher:
                hist_fetcher.stop()
            if executor_pool:
                executor_pool.shutdown()