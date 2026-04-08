import akshare as ak
import pandas as pd
from utils.log_manager import get_logger
from utils.common import ResultStatus
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceApiName, AbstractDataSourceAPI, FetchResult, QueryOptions
from sources.datasource_adapter import SymbolConverter
from datetime import datetime
from core.paraller_job_executor import ParallelJob
from core.process_pool.process_pool import ExProcessExecutorPool
from core.process_pool.executor_task import ExectuorTaskCfg
from sources.parallel_hist_fetcher import ParallelHistFetcher

logger = get_logger(__name__)


class AKshareApiHKHistricSina(AbstractDataSourceAPI):
    source_api_type: DataSourceApiName = DataSourceApiName.AKSHARE_SINA_API
    name = DataSourceApiName.AKSHARE_SINA_API.value

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """新浪港股格式转换"""
        df_new = df.copy()
        df_new["symbol"] = symbol
        # 保持和A股一致的列顺序
        return df_new[[
            "symbol", "date", "open", "high", "low", "close", "volume", "amount", "turnover"
        ]]

    def filter_date_range(self, df: pd.DataFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """过滤指定日期范围的数据"""
        if df is None or df.empty:
            return df
        
        # 确保date列为datetime类型
        df['date'] = pd.to_datetime(df['date'])
        # 过滤日期
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df_filtered = df[mask].copy()
        return df_filtered

    def fetch_hist(self, symbols_str: str, options: QueryOptions | None = None) -> FetchResult | None:
        if options is None:
            options = QueryOptions(start=pd.to_datetime("19000101"))

        symbol_converter = SymbolConverter(DataSourceType.AKSHARE, Region.HK, self.source_api_type)
        failed_symbols = []
        result_data = {}
        symbols = symbols_str.split(",")
        if not options.support_parallel or len(symbols) < 20:
            for symbol in symbols:
                try:
                    # 转换港股代码
                    code = symbol_converter.convert(symbol)
                    adjust = options.get("adjust", "")
                    # 获取全量港股数据
                    df = ak.stock_hk_daily(
                        symbol=code,
                        adjust=adjust
                    )

                    # 处理日期过滤
                    if df is not None and not df.empty:
                        start_date = options.get("start", pd.to_datetime("19000101"))
                        end_date = options.get('end', datetime.now())
                        df = self.filter_date_range(df, start_date, end_date)

                    # 数据判空和标准化
                    if df is not None and not df.empty:
                        print(f"{self.name} success: {symbol}")
                        logger.info(f"{self.name} success: {symbol}")
                        df = self.normalize(df, symbol)
                        result_data[symbol] = df  # 仅成功时赋值
                    else:
                        failed_symbols.append(symbol)  # 加入失败列表

                except Exception as e:
                    failed_symbols.append(symbol)
                    print(f"{self.name} failed: {symbol}, error={e}")
                    logger.exception(f"{self.name} failed: {symbol}, error={e}")
                    
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
            # 并行处理
            return self.fetch_hist_parallel(symbols_str, options, symbol_converter)

    def fetch_hist_parallel(self, symbols_str: str, options: QueryOptions, symbol_converter: SymbolConverter) -> FetchResult | None:
        executor_pool = None
        hist_fetcher = None
        try:
            executor_pool = ExProcessExecutorPool(max_workers=4)
            task_cfg = ExectuorTaskCfg(
                task_module_file="sources.akshare.worker_akshare_api_hk_hist_sina",
                task_class_name="WorkerAkshareHKHistSinaFetcher",
            )
            executor_pool.init_preinit_workers(task_cfg)

            def executor_call(job: ParallelJob):
                executor = executor_pool.acquire(block=True)
                task_cfg.task_params = job.job_params.copy()
                return executor.run(task_cfg, timeout=10)

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
            result_data = hist_fetcher.run()  # 获取并行执行结果
            
            # 统计失败的symbol
            failed_symbols = [sym for sym, df in result_data.items() if df is None or df.empty]
            success_data = {sym: df for sym, df in result_data.items() if df is not None and not df.empty}

            # 构建FetchResult返回
            fr = FetchResult(
                status=ResultStatus.SUCCESS,
                data=success_data,
                failed_symbols=failed_symbols
            )
            if len(failed_symbols) == len(symbols_str.split(",")):
                fr.status = ResultStatus.FAILED
            elif len(failed_symbols) > 0:
                fr.status = ResultStatus.PARTIAL_SUCCESS
                
            return fr
        
        except Exception as e:
            logger.exception(f"并行获取港股历史数据失败: {e}")
            # 构建失败的FetchResult
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