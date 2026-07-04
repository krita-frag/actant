"""集成测试 fixtures：单进程多任务工作流。

提供：
- single_node: 单进程 _Node（worker + 提交），后台线程运行
- submit_and_wait: 提交 flow 并同步等待结果的便捷函数
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from actant._node import _Node
from actant.config import NetworkConfig

if TYPE_CHECKING:
    from actant.result import WorkflowResult


@pytest.fixture
def single_node(tmp_path):
    """单进程 _Node：纯内存模式，端口自动分配。

    启动后既提交也执行任务。每个测试独立节点。
    """
    node = _Node(
        name="test-node",
        _executing=True,
        network=NetworkConfig(preset="local"),
        port=0,
        data_dir=None,  # 纯内存，避免 LMDB 锁冲突
        signing_key="test-key",
    )
    ready = threading.Event()
    from tests._helpers.network import run_node_in_thread

    thread = run_node_in_thread(node, ready_event=ready, timeout_s=30.0)
    try:
        yield node
    finally:
        node.shutdown(timeout=5.0)
        thread.join(timeout=5.0)


@pytest.fixture
def submit_and_wait(single_node):
    """提交 flow 并同步等待结果的便捷函数。

    Returns:
        callable(flow, *args, timeout=10.0, **kwargs) -> WorkflowResult
    """

    def _submit_and_wait(
        flow: Any,
        *args: Any,
        timeout: float = 15.0,
        **kwargs: Any,
    ) -> WorkflowResult:
        result = single_node.submit(flow, *args, **kwargs)
        # 轮询 ready，避免 get_sync 在某些环境下阻塞
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if result.ready():
                break
            time.sleep(0.05)
        else:
            pytest.fail(f"workflow {result.workflow_id} did not complete within {timeout}s")
        return result.get_sync(timeout=1.0)

    return _submit_and_wait
