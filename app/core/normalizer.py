# COLUMN_MAP_CN = {
#     "日期": "date",
#     "开盘": "open",
#     "收盘": "close",
#     "最高": "high",
#     "最低": "low",
#     "成交量": "volume",
#     "成交额": "amount",
#     "涨跌幅": "pct",
#     "换手率": "turnover",
# }

def normalize(df, symbol, market):

    # df = df.rename(columns=COLUMN_MAP_CN)

    df["symbol"] = symbol
    df["market"] = market

    df["date"] = df["date"].astype("datetime64[ns]")
    
    # Add missing optional columns with NaN if they don't exist
    if "pct" not in df.columns: 
        df["pct"] = None
    if "turnover" not in df.columns: # 东财没有换手率数据
        df["turnover"] = None
    if "volume" not in df.columns: # 腾讯没有成交量数据
        df["volume"] = None
    if "amount" not in df.columns: # 腾讯没有成交额数据
        df["amount"] = None

    return df[
        ["symbol", "market", "date",
         "open", "high", "low", "close",
         "volume", "amount", "pct", "turnover"]
    ]
