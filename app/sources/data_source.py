# coding=utf-8 

import enum
class DataSource(enum.Enum):
    AKSHARE = "akshare" # akshare, support CN-A, HK, US
    YFINANCE = "yfinance"  # yahoo finance, support US, also support CN-A and HK
    IFIND = "ifind" # ifind, support CN-A, HK and US
    EASTQUOTATION = "eastquotation" # eastquotation, support HK

class AKShareAPIType(enum.Enum):
    EASTMONEY = "eastmoney" # akshare eastmoney api
    SINA = "sina" # akshare sina api
    TENCENT = "tencent" # akshare tencent api