import dis
import types


def lambda_to_str(func: types.FunctionType) -> str:
    if not isinstance(func, types.FunctionType) or func.__name__ != "<lambda>":
        raise ValueError("必须是 lambda 函数")

    # =================================
    # 1. 解析参数名 + 默认值
    # =================================
    code = func.__code__
    arg_names = code.co_varnames[:code.co_argcount]
    defaults = func.__defaults__ or ()
    default_idx = len(arg_names) - len(defaults)

    parts = []
    for i, name in enumerate(arg_names):
        if i >= default_idx:
            val = defaults[i - default_idx]
            parts.append(f"{name}={repr(val)}")
        else:
            parts.append(name)
    args_part = ", ".join(parts)

    # =================================
    # 2. 反汇编字节码，提取函数名
    # =================================
    last_func = None
    instructions = list(dis.get_instructions(func))
    for instr in instructions:
        if instr.opname in ("LOAD_GLOBAL", "LOAD_NAME"):
            last_func = instr.argval

    # =================================
    # 3. 拼接完整 lambda 字符串
    # =================================
    if last_func:
        call_args = ", ".join(arg_names)
        body = f"{last_func}({call_args})"
    else:
        body = "..."

    return f"lambda {args_part}: {body}"