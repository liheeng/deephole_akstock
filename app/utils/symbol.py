def fix_preferred_symbol(symbol):
    """
    专门给 yfinance 使用的股票代码格式化
    官方规则：
    - 优先股 ^ → 替换为 -
    - 指数 ^ 开头 → 保留不变
    """
    if not symbol:
        return symbol

    # 1. 指数：以 ^ 开头 → 原样返回
    if symbol.startswith("^"):
        return symbol

    # 2. 优先股：中间包含 ^ → 替换成 -
    if "^" in symbol:
        return symbol.replace("^", "-")

    # 普通股票 → 不变
    return symbol