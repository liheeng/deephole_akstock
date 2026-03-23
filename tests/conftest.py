# tests/conftest.py
import sys
from pathlib import Path

# 把 app 目录加入 PYTHONPATH，这是你真正的源码根目录
root_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(root_dir))