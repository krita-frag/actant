"""``@aio.flow``：协程化等待点复用同步 flow 的 journal 契约。

覆盖 PR-5 的 API 面：``ctx.sleep`` / ``ctx.wait_signal`` / ``ctx.submit``，
以及异步重放在新 Runtime 上的恢复能力。并发规模上限的 10k 用例见
``test_async_driver.py``。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from actant import (
    Runtime,
    aio,
    host_flows,
    start_flow,
    task,
    use_runtime,
)
from actant.driver import AsyncDriver


@task(name="async_echo")
def _echo(value: int) -> int:
    return value


@task(name="async_side_effect")
def _write_marker(marker: str) -> str:
    with open(marker, "a") as fh:
        fh.write("x")
    return "recorded"


@task(name="async_capture")
def _capture(value: str, out: str) -> None:
    with open(out, "a") as fh:
        fh.write(f"{value}\n")


@aio.flow(name="async-basic")
async def _basic(ctx, value: int) -> int:
    pending = _echo.submit(value)  # 直接 submit：验证异步上下文中的 Task 路径
    first = await ctx.submit(_echo, pending)  # AsyncResult 依赖由 await 解析
    return await ctx.submit(_echo, first + 1)


@aio.flow(name="async-sleep")
async def _sleep_then_return(ctx, seconds: float) -> str:
    await ctx.sleep(seconds)
    return "slept"


def test_async_flow_can_await_ctx_methods() -> None:
    """协程体内 ``await ctx.submit`` 返回任务结果。"""
    with Runtime() as rt, use_runtime(rt):
        assert asyncio.run(_basic(41)) == 42


def test_ctx_sleep_uses_persistent_timer() -> None:
    """``ctx.sleep`` 复用持久化 timer 等待点并能正常结束。"""
    with Runtime() as rt, use_runtime(rt):
        started = time.monotonic()
        assert asyncio.run(_sleep_then_return(0.01)) == "slept"
        assert time.monotonic() - started < 10


@aio.flow(name="async-detached-gate")
async def _detached_gate(ctx, tag: str) -> str:
    payload = await ctx.wait_signal("go")
    return f"{tag}:{payload.decode() if payload else 'none'}"


def test_signal_before_host_is_buffered() -> None:
    """等待点注册前递交的信号由 Rust 缓冲，异步重放时立即取回。"""
    with Runtime() as rt, use_runtime(rt):
        handle = start_flow("async-detached-gate", "tag", runtime=rt)
        rt.signal_wait_point(handle.workflow_id, "go", b"early")

        with AsyncDriver() as driver:
            driven = host_flows(runtime=rt, once=True, driver=driver)
            driver.join()

        assert driven == [handle.workflow_id]
        assert rt.get_workflow_result(handle.workflow_id) == "tag:early"


@aio.flow(name="async-restart")
async def _restart_flow(ctx, marker: str, out: str) -> str:
    value = await ctx.submit(_write_marker, marker)
    payload = await ctx.wait_signal("go")
    await ctx.submit(_capture, value, out)
    return payload.decode() if payload else "none"


def test_async_flow_resumes_from_journal_on_new_runtime(tmp_path: Path) -> None:
    """仅凭 journal：换一个 Runtime 实例仍能驱动同一 async workflow。"""
    data_dir = str(tmp_path / "data")
    marker = str(tmp_path / "marker.txt")
    out = str(tmp_path / "out.txt")

    first = Runtime(name="async-node", data_dir=data_dir)
    first.start()
    try:
        handle = start_flow("async-restart", marker, out, runtime=first)
    finally:
        first.stop()

    second = Runtime(name="async-node", data_dir=data_dir)
    second.start()
    try:
        with use_runtime(second), AsyncDriver() as driver:
            host_flows(runtime=second, once=True, driver=driver)
            second.signal_wait_point(handle.workflow_id, "go", b"resumed")
            driver.join()

        assert second.get_workflow_result(handle.workflow_id) == "resumed"
    finally:
        second.stop()

    assert Path(marker).read_text() == "x", "任务节点应恰好执行一次"
    assert Path(out).read_text().splitlines() == ["recorded"]


@aio.flow(name="async-broken")
async def _broken(ctx) -> None:
    raise RuntimeError("async boom")


def test_async_driver_reraises_flow_failure() -> None:
    """函数体异常经驱动 ``join`` 重抛，而不是被 event loop 吞掉。"""
    with Runtime() as rt, use_runtime(rt):
        start_flow("async-broken", runtime=rt)
        with AsyncDriver() as driver:
            host_flows(runtime=rt, once=True, driver=driver)
            with pytest.raises(RuntimeError, match="async boom"):
                driver.join()
