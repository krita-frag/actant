"""Python 基准测试共享 fixture。

- 强制 ``ACTANT_DISCOVERY=none`` 避免 iroh DNS 阻塞
- 提供 session-scoped ``runtime`` fixture（复用 Runtime 减少 iroh 启动开销）
- 提供 ``noop_task`` / ``payload_task`` 等共享任务工厂
"""

from __future__ import annotations

import os

# 必须在 import actant 之前设置，避免 Rust 运行时初始化时进行 DNS Pkarr 解析。
os.environ.setdefault("ACTANT_DISCOVERY", "none")

import pytest

import actant


@pytest.fixture(scope="session")
def runtime():
    """session 级 Runtime：所有基准测试复用，减少 iroh endpoint 启动开销。"""
    with actant.Runtime.with_defaults() as rt:
        yield rt


@pytest.fixture
def noop_task():
    """零负载任务，仅返回 42。用于测量纯调度开销。"""
    @actant.task
    def _noop() -> int:
        return 42
    return _noop


@pytest.fixture
def add_task():
    """简单加法任务。"""
    @actant.task
    def _add(a: int, b: int) -> int:
        return a + b
    return _add


@pytest.fixture
def payload_task():
    """按字节大小生成 payload 的任务。"""
    @actant.task
    def _make_payload(size: int) -> bytes:
        return b"x" * size
    return _make_payload


@pytest.fixture
def slow_task():
    """模拟 IO bound 慢任务（sleep 50ms）。"""
    import time

    @actant.task
    def _slow(ms: int = 50) -> int:
        time.sleep(ms / 1000)
        return ms
    return _slow
