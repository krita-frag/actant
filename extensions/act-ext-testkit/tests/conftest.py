"""让未安装的 testkit 包在仓库内可直接被 pytest 导入并加载插件。"""

from __future__ import annotations

import sys
from pathlib import Path

_PKG_ROOT = Path(__file__).resolve().parents[1]
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

pytest_plugins = ["act_ext_testkit.pytest_plugin"]
