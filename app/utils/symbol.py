def fix_preferred_symbol(symbol):
    if "^" in symbol:
        # 标准格式：ABR-D 或者 ABR.PR.D
        return symbol.replace("^", "-")
    return symbol