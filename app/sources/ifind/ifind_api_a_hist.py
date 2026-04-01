
import os
from typing import Dict
import pandas as pd
from utils.log_manager import get_logger
from utils.common import ResultStatus
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceApiName, AbstractDataSourceAPI, FetchResult, QueryOptions
from sources.datasource_adapter import SymbolConverter
from sources.ifind.ifind_api import IFinDApi, convert_function_params

logger = get_logger(__name__)


class IFinDApiAHistoric(AbstractDataSourceAPI):
    source_api_type: DataSourceApiName = DataSourceApiName.IFIND_API
    name = DataSourceApiName.IFIND_API.value

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

    def fetch_hist(self, symbols_str: str, options: QueryOptions | None = None) -> FetchResult | None:
        if options is None:
            options = QueryOptions(start=pd.to_datetime("19000101"))

        symbol_converter = SymbolConverter(DataSourceType.IFIND, Region.CN)
        
        ifind_instance = IFinDApi.instance()
        if ifind_instance is None or not ifind_instance.is_available():
            raise Exception("iFinD is not available")

        codes_str = ""
        origin_symbols_list = []
        convert_symbols = []
        for symbol in symbols_str.split(","):
            origin_symbols_list.append(symbol)
            convert_symbols.append(symbol_converter.convert(symbol))
            
        # iFinD
        codes_str = ",".join(convert_symbols)
        start = options.get("start", pd.to_datetime("1900-01-01"))
        try:
            his_data: Dict[str, pd.DataFrame] = ifind_instance.get_historical_data(
                codes=codes_str,
                start=start.strftime("%Y-%m-%d"),
                func_params=convert_function_params(options))  # type: ignore

            if his_data is None or len(his_data) == 0:
                return None
            
            # print(f"[iFinD] success: {symbols_str}")
            # logger.info(f"[iFinD] success: {symbols_str}")
            new_his_data = {}
            his_data_keys = list(his_data.keys())
            for i in range(len(his_data_keys)):
                new_his_data[origin_symbols_list[i]] = self.normalize(his_data[his_data_keys[i]], origin_symbols_list[i])
            
            return FetchResult(status=ResultStatus.SUCCESS, data=new_his_data)

        except Exception as e:
            print(f"[iFinD] failed: {symbols_str}, error={e}")
            raise e


if __name__ == "__main__":
    try:
        IFinDApi(os.getenv("IFIND_REFRESH_TOKEN", ""))
        api = IFinDApiAHistoric()
        result = api.fetch_hist("000001.SZ,000002.SZ,603885.SH,601899.SH", QueryOptions(start=pd.to_datetime("2026-01-01")))
        print(result)
    except Exception as e:
        print(f"Error occurred: {e}")
