from dataclasses import dataclass, field
from typing import List, Dict, Any
from vectorbt_test.utils.misc import lambda_to_str

@dataclass
class NodeParam:
    name: str
    type: str
    default: Any = None
    desc: str = ""


@dataclass
class NodeMeta:
    name: str
    group: str
    desc: str = ""
    params: List[NodeParam] = field(default_factory=list)


class NodeRegistry:

    _factories: Dict[str, Any] = {}
    _groups: Dict[str, List[str]] = {}
    _meta: Dict[str, NodeMeta] = {}

    # =========================
    # 注册
    # =========================
    @classmethod
    def register(cls, name: str, factory, meta: NodeMeta):
        cls._factories[name] = factory
        cls._meta[name] = meta

        cls._groups.setdefault(meta.group, []).append(name)

    # =========================
    # 创建 Node
    # =========================
    @classmethod
    def create(cls, __node_name, *args, **kwargs):
        if __node_name not in cls._factories.keys():
            raise ValueError(f"{__node_name} not registered\n{cls._factories.keys()}")

        factory = cls._factories[__node_name]

        try:
            return factory(*args, **kwargs)
        except TypeError as e:
            raise TypeError(
                f"Error creating node '{__node_name}': args={args}, kwargs={kwargs}"
            ) from e

    # =========================
    # 获取 Meta
    # =========================
    @classmethod
    def get_meta(cls, name: str) -> NodeMeta:
        return cls._meta.get(name)

    # =========================
    # 获取分组
    # =========================
    @classmethod
    def list_groups(cls):
        return cls._groups

    @classmethod
    def get_group(cls, group: str):
        return cls._groups.get(group, [])

    # =========================
    # 前端输出（重要）
    # =========================
    @classmethod
    def to_dict(cls):
        result = {}
    
        for group, names in cls._groups.items():
            result[group] = []

            for name in names:
                meta = cls._meta[name]

                result[group].append({
                    "name": meta.name,
                    "factory": lambda_to_str(cls._factories[name]),
                    "desc": meta.desc,
                    "params": [
                        {
                            "name": p.name,
                            "type": p.type,
                            "default": p.default,
                            "desc": p.desc
                        }
                        for p in meta.params
                    ]
                })

        return result
