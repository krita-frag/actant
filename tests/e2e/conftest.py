"""e2e 测试 fixtures：多节点真实分布式集群。

提供：
- two_node_cluster: 两个互联的 _Node（一个提交+执行，一个执行）
- three_node_cluster: 三个互联的 _Node
- submit_via: 在指定节点提交并等待结果
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from actant._node import _Node
from actant.config import NetworkConfig
from tests._helpers.network import (
    connect_peers,
    run_node_in_thread,
    wait_for_peers,
)

if TYPE_CHECKING:
    from actant.result import WorkflowResult


def _make_node(name: str) -> _Node:
    """创建纯内存节点，端口自动分配。"""
    return _Node(
        name=name,
        _executing=True,
        network=NetworkConfig(preset="local"),
        port=0,
        data_dir=None,
        signing_key="test-key",
    )


def _start_node(node: _Node) -> threading.Thread:
    """启动节点并等待就绪。"""
    ready = threading.Event()
    return run_node_in_thread(node, ready_event=ready, timeout_s=30.0)


def _teardown(node: _Node, thread: threading.Thread) -> None:
    """清理节点。"""
    try:
        node.shutdown(timeout=5.0)
    finally:
        thread.join(timeout=5.0)


@pytest.fixture
def two_node_cluster():
    """两个互联的 _Node。

    拓扑：A ←→ B（双向连接 + gossip 互发现）
    A 和 B 都执行任务；测试可在 A 上提交并期望 B 也能执行。
    """
    a = _make_node("e2e-node-a")
    b = _make_node("e2e-node-b")
    ta = _start_node(a)
    tb = _start_node(b)
    try:
        # 在 A 的事件循环中连接 B
        loop_a = a._event_loop
        asyncio.run_coroutine_threadsafe(connect_peers(a, b), loop_a).result(timeout=10.0)
        # 等待双方互发现
        ok_a = asyncio.run_coroutine_threadsafe(
            wait_for_peers(a, min_peers=1, timeout_s=10.0), loop_a
        ).result(timeout=15.0)
        loop_b = b._event_loop
        ok_b = asyncio.run_coroutine_threadsafe(
            wait_for_peers(b, min_peers=1, timeout_s=10.0), loop_b
        ).result(timeout=15.0)
        assert ok_a and ok_b, "节点未能在超时内互相发现"
        yield a, b
    finally:
        _teardown(a, ta)
        _teardown(b, tb)


@pytest.fixture
def three_node_cluster():
    """三个互联的 _Node：A ←→ B ←→ C ←→ A（全互联）。"""
    a = _make_node("e2e-tri-a")
    b = _make_node("e2e-tri-b")
    c = _make_node("e2e-tri-c")
    ta, tb, tc = _start_node(a), _start_node(b), _start_node(c)
    try:
        loop_a = a._event_loop
        # A-B
        asyncio.run_coroutine_threadsafe(connect_peers(a, b), loop_a).result(timeout=10.0)
        # A-C（B、C 通过 gossip 间接发现，但显式连接更可靠）
        asyncio.run_coroutine_threadsafe(connect_peers(a, c), loop_a).result(timeout=10.0)
        # 等待三方互发现
        ok_a = asyncio.run_coroutine_threadsafe(
            wait_for_peers(a, min_peers=2, timeout_s=15.0), loop_a
        ).result(timeout=20.0)
        loop_b = b._event_loop
        ok_b = asyncio.run_coroutine_threadsafe(
            wait_for_peers(b, min_peers=2, timeout_s=15.0), loop_b
        ).result(timeout=20.0)
        loop_c = c._event_loop
        ok_c = asyncio.run_coroutine_threadsafe(
            wait_for_peers(c, min_peers=2, timeout_s=15.0), loop_c
        ).result(timeout=20.0)
        assert ok_a and ok_b and ok_c, "三节点未能在超时内全互联发现"
        yield a, b, c
    finally:
        _teardown(a, ta)
        _teardown(b, tb)
        _teardown(c, tc)


@pytest.fixture
def submit_via():
    """在指定节点提交并等待结果。

    Returns:
        callable(node, flow, *args, timeout=20.0, **kwargs) -> WorkflowResult
    """

    def _submit_via(
        node: _Node,
        flow: Any,
        *args: Any,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> WorkflowResult:
        result = node.submit(flow, *args, **kwargs)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if result.ready():
                break
            time.sleep(0.1)
        else:
            pytest.fail(f"workflow {result.workflow_id} did not complete within {timeout}s")
        return result.get_sync(timeout=2.0)

    return _submit_via
