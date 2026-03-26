import re
from sources.data_source import DataSourceType, DataSourceAPI
from markets.market import Region, ExchangeType
from utils.common import safe_format
# 数据源格式定义（已修复缩进 + 结构）
DATA_SOURCE_FORMAT = {
    "ifind": {
        "cn": "{code}.{exchange}",
        "hk": "{code}.HK",
        "us": "{code}.{suffix}",
        "us_suffix": {"NASDAQ": "O", "NYSE": "N", "AMEX": "A"},
    },
    "yfinance": {
        "cn": "{code}.{suffix}",
        "cn_suffix": {"SH": "SS", "SZ": "SZ"},
        "hk": "{code}.HK",
        "us": "{code}",
    },
    "akshare": {
        "cn": "{api}",
        "cn_api": {
            "eastmoney": "{code}",
            "sina": "{exchange_lower}{code}",
            "tencent": "{exchange_lower}{code}"
        },
        "hk_api": {
            "eastmoney": "{code}",
            "sina": "{code}"
        },
        "us_api": {
            "eastmoney": "{code}",
            "sina": "{code}"
        }
    },
    "eastquotation": {
        "hk": "{code}"
    }
}

PATTERN = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def get_placeholders(format_str: str) -> list[str]:
    return PATTERN.findall(format_str)


def build_symbol(
    symbol: str,
    data_source: DataSourceType,
    region_type: Region,
    api_type: DataSourceAPI | None = None
):
    # 拆解 code 和 exchange
    code = symbol.split(".")[0]
    exchange = symbol.split(".")[-1]
    source = data_source.value.lower()
    region = region_type.value.lower()
    api = api_type.value.split(".")[-1].lower() if api_type else None

    if ExchangeType(exchange) is None:
        raise Exception(f"未知股票代码：{symbol}, 非法的交易所：{exchange}")
    
    # ========================
    # 第一步：基础格式化
    # ========================
    fmt_str = DATA_SOURCE_FORMAT[source][region]
    result = safe_format(fmt_str, code=code, exchange=exchange)

    # ========================
    # 解析占位符
    # ========================
    placeholders = get_placeholders(result)
    if not placeholders:
        return result

    # ========================
    # 处理 suffix / api 占位符
    # ========================
    for ph in placeholders:
        if ph == "suffix":
            suffix_key = f"{region}_suffix"
            suffix_val = DATA_SOURCE_FORMAT[source][suffix_key][exchange]
            result = safe_format(result, suffix=suffix_val)

        elif ph == "api":
            api_key = f"{region}_api"
            api_val = DATA_SOURCE_FORMAT[source][api_key][api]
            result = safe_format(result, api=api_val)

    # ========================
    # 最后处理 exchange_lower
    # ========================
    return safe_format(result, code=code, exchange=exchange, exchange_lower=exchange.lower())
