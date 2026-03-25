# coding=utf-8

import enum


class DataSource(enum.Enum):
    AKSHARE = "akshare"     # akshare, support CN-A, HK, US
    YFINANCE = "yfinance"   # yahoo finance, support US, also support CN-A and HK
    IFIND = "ifind"     # ifind, support CN-A, HK and US
    EASTQUOTATION = "eastquotation"     # eastquotation, support HK


class DataSourceAPI(enum.Enum):
    AKSHARE_EASTMONEY_API = f"{DataSource.AKSHARE.value}.eastmoney"
    AKSHARE_SINA_API = f"{DataSource.AKSHARE.value}.sina"
    AKSHARE_TENCENT_API = f"{DataSource.AKSHARE.value}.tencent"
    
    CN_SSE_API = "cn.sse"   # 上交所
    CN_SZSE_API = "cn.szse"     # 深交所
    CN_SINA_API = "cn.sina"     # 新浪
    EASTMONEY_API = "eastmoney"     # 东方财富
    XUEQIU_API = "xueqiu"   # 雪球
    HKEX_API = "hkex"   # 港交所
    NYSE_API = "nyse"   # 纽交所
    NASDAQ_API = "nasdaq"   # 纳斯达克
    YAHOO_FINANCE_API = "yahoo_finance"     # 雅虎财经
    TENCENT_API = "tencent"     # 腾讯财经
    EAST_QUOTATION_API = "eastquotation"    # 东方量化
    YFINANCE_API = "yfinance"   # yfinance
    IFIND_API = "ifind"
