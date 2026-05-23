from typing import Callable, Dict, List


class GroupFunctionRegistry:
    def __init__(self):
        # 核心存储：key=分组名，value=分组下的函数列表
        self._group_map: Dict[str, List[Callable]] = {}

    def register(self, group: str = "default"):
        """
        装饰器：注册函数到指定分组
        :param group: 分组名称，默认 default
        """
        def decorator(func: Callable) -> Callable:
            # 分组不存在则创建
            if group not in self._group_map:
                self._group_map[group] = []
            # 函数加入对应分组
            self._group_map[group].append(func)
            return func
        return decorator

    def batch_run_group(self, group: str, *args, **kwargs) -> List[tuple]:
        """
        批量执行【指定分组】的所有函数
        :param group: 要执行的分组名
        :return: 列表 [(函数名, 返回值), ...]
        """
        if group not in self._group_map:
            print(f"⚠️ 分组 [{group}] 不存在，无函数执行")
            return []

        results = []
        for func in self._group_map[group]:
            res = func(*args, **kwargs)
            results.append((func.__name__, res))
        return results

    def batch_run_all(self, *args, **kwargs) -> Dict[str, List[tuple]]:
        """批量执行【所有分组】的所有函数"""
        all_results = {}
        for group_name in self._group_map:
            all_results[group_name] = self.batch_run_group(group_name, *args, **kwargs)
        return all_results

    def get_all_groups(self) -> list:
        """获取所有注册的分组名"""
        return list(self._group_map.keys())


# ====================== 使用示例 ======================
if __name__ == '__main__':
    # 1. 创建注册表实例
    reg = GroupFunctionRegistry()

    # 2. 分组注册函数（核心：指定 group 参数）
    @reg.register(group="数学计算")
    def add(a, b):
        return a + b

    @reg.register(group="数学计算")
    def mul(a, b):
        return a * b

    @reg.register(group="字符串处理")
    def join_str(a, b):
        return f"{a}_{b}"

    @reg.register(group="字符串处理")
    def upper_str(s):
        return s.upper()

    @reg.register()  # 不指定分组 → 默认分组 default
    def default_func():
        return "我是默认分组"

    # ==============================================
    # 3. 指定分组调用（你最需要的功能！）
    # ==============================================
    print("=== 执行 【数学计算】 分组 ===")
    res1 = reg.batch_run_group("数学计算", 10, 5)
    for name, val in res1:
        print(f"{name}: {val}")

    print("\n=== 执行 【字符串处理】 分组 ===")
    res2 = reg.batch_run_group("字符串处理", "hello", "duckdb")
    for name, val in res2:
        print(f"{name}: {val}")

    print("\n=== 执行 【默认分组】 ===")
    res3 = reg.batch_run_group("default")
    print(res3)

GroupFuncReg = GroupFunctionRegistry()