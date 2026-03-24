# 市场归属（自动判断是哪个市场）
# STOCK_MARKET = {
#     # A 股
#     "600036": "SH",
#     "000001": "SZ",
#     "000858": "SZ",
#     "601318": "SH",

#     # 港股
#     "00700": "HK",
#     "00005": "HK",
#     "00998": "HK",

#     # 美股
#     "AAPL": "NASDAQ",
#     "MSFT": "NASDAQ",
#     "TSLA": "NASDAQ",
#     "JPM": "NYSE",
#     "V": "NYSE",
# }
# 数据源格式定义（已按要求重命名）
DATA_SOURCE_FORMAT = {
    "ifind": {
        "cn_a": "{code}.{market}",       # 600036.SH
        "hk": "{code}.HK",              # 00700.HK
        "us": "{code}.{suffix}",        # AAPL.O
        "us_suffix": {"NASDAQ": "O", "NYSE": "N", "AMEX": "A"},
    },
    "yfinance": {
        "cn_a": "{code}.{suffix}",      # 600036.SS
        "cn_a_suffix": {"SH": "SS", "SZ": "SZ"},  # 已修改
        "hk": "{code}.HK",              # 00700.HK
        "us": "{code}",                 # AAPL
    },
    "akshare": {
        "eastmoney": {                  # 东财接口：不带后缀
            "cn_a": "{code}",
            "hk": "{code}",
            "us": "{code}",
        },
        "sina": {                       # 新浪接口：加前缀
            "cn_a": "{market_lower}{code}",
            "hk": "{code}",
            "us": "{code}"
        },
        "tencent": {                    # 腾讯接口：加前缀
            "cn_a": "{market_lower}{code}"
        }
    },
    "eastquotation": {              # EastQuotation：不带后缀
        "hk": "{code}"
    }
}