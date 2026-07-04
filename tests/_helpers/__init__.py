"""共享测试辅助模块。

提供跨测试层复用的 fixture 和工具函数，避免重复代码。
"""

from __future__ import annotations

from tests._helpers.network import (
    connect_peers,
    run_node_in_thread,
    wait_for_peers,
)
from tests._helpers.payload import pack_upstream_prefix
from tests._helpers.tasks import make_task, make_task_ref

__all__ = [
    "connect_peers",
    "make_task",
    "make_task_ref",
    "pack_upstream_prefix",
    "run_node_in_thread",
    "wait_for_peers",
]
