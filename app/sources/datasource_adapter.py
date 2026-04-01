from sources.code_mapping import build_symbol
from markets.market import Region   
from sources.data_source import DataSourceType, DataSourceApiName
from utils.symbol import fix_preferred_symbol
from typing import Optional, Callable


def convert_symbol(
    symbol: str,
    data_source: DataSourceType,
    region_type: Region,
    api_type: DataSourceApiName | None = None
):
    code = build_symbol(symbol, data_source, region_type, api_type)
    if region_type == Region.US:
        if data_source == DataSourceType.YFINANCE:
            code = fix_preferred_symbol(code, "-")
        elif data_source == DataSourceType.IFIND:
            code = fix_preferred_symbol(code, ".")
    return code


class SymbolConverter:
    def __init__(
        self,
        data_source: DataSourceType,
        region_type: Region,
        api_type: Optional[DataSourceApiName] = None,
        aftercall: Callable[[str], str] | None = None
    ):
        """
        初始化符号转换器（除 symbol 外的参数作为构造参数）
        :param data_source: 数据源类型
        :param region_type: 市场区域
        :param api_type: API 类型（可选）
        """
        self.data_source = data_source
        self.region_type = region_type
        self.api_type = api_type
        self.aftercall = aftercall

    def convert(self, symbol: str, aftercall: Callable[[str], str] | None = None) -> str:
        """
        转换单个股票代码（symbol 作为方法参数）
        :param symbol: 原始股票代码
        :return: 转换后的代码
        """
        # 调用你原有的构建方法
        code = build_symbol(symbol, self.data_source, self.region_type, self.api_type)
        
        # 美股特殊处理
        if self.region_type == Region.US:
            if self.data_source == DataSourceType.YFINANCE:
                code = fix_preferred_symbol(code, "-")
            elif self.data_source == DataSourceType.IFIND:
                code = fix_preferred_symbol(code, ".")
        
        if self.aftercall:
            code = self.aftercall(code)
            
        return aftercall(code) if aftercall else code
