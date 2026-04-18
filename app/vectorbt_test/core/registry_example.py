from vectorbt_test.core.registry import NodeRegistry, NodeMeta, NodeParam
from vectorbt_test.engine.init import load_register_nodes


if __name__ == "__main__":
    load_register_nodes()
    nodes = NodeRegistry().to_dict()
    print(nodes)