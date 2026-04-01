import os
from typing import Dict
import pandas as pd
from utils.log_manager import get_logger
from utils.common import ResultStatus
from markets.market import Region
from sources.data_source import DataSourceType, DataSourceApiName, AbstractDataSourceAPI, FetchResult, QueryOptions
from sources.datasource_adapter import SymbolConverter
from sources.ifind.ifind_api import IFinDApi, IFinDFuncCurrency, convert_function_params

logger = get_logger(__name__)


# ============================
# 父类：通用iFinD历史数据拉取（核心逻辑只写一次）
# ============================
class BaseIFindHistoric(AbstractDataSourceAPI):
    source_api_type: DataSourceApiName = DataSourceApiName.IFIND_API
    name = DataSourceApiName.IFIND_API.value

    # 【子类必须实现】市场区域
    @property
    def region(self) -> Region:
        raise NotImplementedError("子类必须实现 region 属性")

    # 【子类可重写】iFinD专用参数转换
    def convert_params(self, options: QueryOptions) -> dict:
        return convert_function_params(options)

    def normalize(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """统一字段格式化（A/港/美通用）"""
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
        return df[["symbol", "date", "open", "high", "low", "close", "volume", "amount", "pct", "turnover"]]

    def fetch_hist(self, symbols_str: str, options: QueryOptions | None = None) -> FetchResult | None:
        if options is None:
            options = QueryOptions(start=pd.to_datetime("19000101"))

        # 自动根据子类 region 创建转换器
        symbol_converter = SymbolConverter(DataSourceType.IFIND, self.region)
        ifind_instance = IFinDApi.instance()

        if ifind_instance is None or not ifind_instance.is_available():
            raise Exception("iFinD is not available")

        # 标的转换
        origin_symbols = []
        convert_symbols = []
        for s in symbols_str.split(","):
            origin_symbols.append(s.strip())
            convert_symbols.append(self.convert_symbol(symbol_converter, s))

        codes_str = ",".join(convert_symbols)
        start = options.start.strftime("%Y-%m-%d")

        try:
            # 调用iFinD（参数由子类提供）
            his_data: dict[str, pd.DataFrame] | None = ifind_instance.get_historical_data(
                codes=codes_str,
                start=start,
                func_params=self.convert_params(options)
            )

            if not his_data:
                logger.warning(f"[iFinD] 没有数据 {symbols_str} | 参数: {self.convert_params(options)}")
                return FetchResult(status=ResultStatus.SUCCESS, data={})

            # 映射回原始标的代码
            result_data = {}
            for ori, conv in zip(origin_symbols, convert_symbols):
                if conv in his_data:
                    result_data[ori] = self.normalize(his_data[conv], ori)

            return FetchResult(status=ResultStatus.SUCCESS, data=result_data)

        except Exception as e:
            logger.error(f"[iFinD] 拉取失败 {symbols_str} | {str(e)}")
            raise e

    def convert_symbol(self, symbol_converter, s):
        _symbol = symbol_converter.convert(s.strip())
        return _symbol   # 转换失败则返回原始标的

# ============================
# 子类 1：A股历史数据（继承父类）
# ============================
class IFindAHistoric(BaseIFindHistoric):
    @property
    def region(self) -> Region:
        return Region.CN

    # A股使用默认 convert_function_params


# ============================
# 子类 2：港股历史数据
# ============================
class IFindHKHistoric(BaseIFindHistoric):
    @property
    def region(self) -> Region:
        return Region.HK

    def convert_params(self, options: QueryOptions) -> dict:
        """港股专用参数（你按实际接口修改）"""
        # if options.get("currency") is None or options.get("currency") == "":
            # options["currency"] = IFinDFuncCurrency.HKD.value  # 默认港股使用HKD

        params = convert_function_params(options)
        return params

    def convert_symbol(self, symbol_converter, s):
        _symbol = super().convert_symbol(symbol_converter, s)
        if _symbol.startswith("0"):
            _symbol = _symbol[1:]  # 去掉前导零
        return _symbol   # 转换失败则返回原始标的

# ============================
# 子类 3：美股历史数据
# ============================
class IFindUSHistoric(BaseIFindHistoric):
    @property
    def region(self) -> Region:
        return Region.US

    def convert_params(self, options: QueryOptions) -> dict:
        """美股专用参数（你按实际接口修改）"""
        # if options.get("currency") is None or options.get("currency") == "":
        #     options["currency"] = IFinDFuncCurrency.USD.value  # 默认美股使用USD

        params = convert_function_params(options)
        return params


# ============================
# 测试调用
# ============================
if __name__ == "__main__":
    IFinDApi(os.getenv("IFIND_REFRESH_TOKEN", ""))
    start = QueryOptions(start=pd.to_datetime("2026-01-01"))

    # A股
    # api = IFindAHistoric()
    # res = api.fetch_hist("000001.SZ,600036.SH", start)

    # 港股
    api = IFindHKHistoric()
    # res = api.fetch_hist("00700.HK,00001.HK", start)
    res = api.fetch_hist("1456.HK,1458.HK,1459.HK,1460.HK,1461.HK,1463.HK", start)
    # 美股
    api = IFindUSHistoric()
    # res = api.fetch_hist("AAPL.O,MSFT.O", start)

    print(res)