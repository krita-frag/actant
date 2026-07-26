"""顶层 pytest 配置：环境隔离、计时报告、分层标记。

测试体系分层（位于 ``tests/python/``）：
- ``unit/``        纯单元测试，无网络无 I/O，零 Rust 运行时依赖
- ``integration/`` 单进程集成测试，真实 Rust 运行时，单节点
- ``e2e/``         多节点真实分布式，gossip 通信，故障注入

标记：
- ``@pytest.mark.unit``        单元测试（默认）
- ``@pytest.mark.integration`` 集成测试
- ``@pytest.mark.e2e``         端到端测试
- ``@pytest.mark.regression``  回归测试（针对已发现 bug）
- ``@pytest.mark.slow``        慢测试（>5s）
"""

from __future__ import annotations

import contextlib
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest

# ---------------------------------------------------------------------------
# 提前加载 PyO3 模块：避免 coverage import hook 干扰 pyo3-log 初始化
# ---------------------------------------------------------------------------


def pytest_load_initial_conftests(early_config, parser, args):
    """在 coverage 启动 import hook 之前预加载 actant.actant。

    coverage 的 sysmon import hook 会干扰 pyo3-log 的 SetLoggerError 初始化，
    导致 ActorCore 导入时 panic。预加载确保 Rust 模块在 coverage hook 之前
    完成初始化。
    """
    with contextlib.suppress(Exception):  # pragma: no cover - 调试环境可能无 Rust 扩展
        import actant.actant  # noqa: F401

if TYPE_CHECKING:
    from pytest import CallInfo, Item, Session

# ---------------------------------------------------------------------------
# 测试标记注册
# ---------------------------------------------------------------------------

MARKERS = (
    "unit: 纯单元测试，无网络无 I/O",
    "integration: 单进程集成测试，真实 Rust 运行时",
    "e2e: 多节点真实分布式端到端测试",
    "regression: 针对已发现 bug 的回归测试",
    "slow: 执行时间 >5s 的测试",
)


def pytest_configure(config: pytest.Config) -> None:
    """注册自定义标记，避免 pytest warning。"""
    for marker in MARKERS:
        config.addinivalue_line("markers", marker)


# ---------------------------------------------------------------------------
# 环境隔离：强制离线发现，避免 iroh DNS 阻塞
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True, scope="session")
def _force_offline_discovery():
    """强制离线发现 for 整个测试 session。

    设置 ``ACTANT_DISCOVERY=none`` 让 Rust 运行时使用 iroh Minimal preset
    （loopback bind，无 DNS Pkarr，无 relay）。否则默认 local preset 在
    sandbox/CI 环境会阻塞 15s ready timeout。

    需要 real networking 的测试可用 ``@pytest.mark.online`` opt-out。
    """
    prev = os.environ.get("ACTANT_DISCOVERY")
    os.environ["ACTANT_DISCOVERY"] = "none"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("ACTANT_DISCOVERY", None)
        else:
            os.environ["ACTANT_DISCOVERY"] = prev


# ---------------------------------------------------------------------------
# Runtime 隔离：每个测试后强制 GC + 等待 iroh/tokio 资源释放
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_runtime_between_tests():
    """每个测试前后强制隔离 Runtime 资源。

    问题：连续多个测试创建/销毁 Runtime 时，前一个 stop() 触发的 iroh endpoint
    关闭与 tokio runtime drop 是异步的，下一个测试启动时若资源未完全释放
    （socket TIME_WAIT、tokio worker thread 未退出），新 Runtime 的 Worker
    启动会延迟，导致 TaskStarted 事件迟迟不到 → `_wait_for_state` 超时。

    策略：
    1. 测试前：清除 thread-local Runtime 引用，防止上一个测试泄漏。
    2. 测试后：强制 ``gc.collect()`` 释放 Python 端引用 + 短暂 sleep
       让 iroh/tokio 后台线程完成退出。
    """
    # 测试前：清除线程局部 Runtime 残留（防上一个测试 with 块异常退出泄漏）
    try:
        from actant._runtime import _runtime_local

        if getattr(_runtime_local, "runtime", None) is not None:
            _runtime_local.runtime = None
    except Exception:
        pass

    yield

    # 测试后：强制 GC + 轮询等待 tokio worker 线程退出 + socket 释放缓冲
    import gc as _gc
    import threading as _threading

    # 多次 collect 处理循环引用（Runtime ↔ _RuntimeCore ↔ dispatch handler 闭包）。
    # 单次 gc.collect() 不保证回收带 __del__ 的循环引用，两次确保彻底释放。
    _gc.collect()
    _gc.collect()

    def _has_background_threads() -> bool:
        return any(
            "tokio" in t.name or "iroh" in t.name or "actant" in t.name
            for t in _threading.enumerate()
            if t is not _threading.main_thread()
        )

    # 诊断：记录测试后残留的后台线程（帮助定位资源泄漏）
    _leaked = [
        t.name for t in _threading.enumerate()
        if t is not _threading.main_thread()
        and ("tokio" in t.name or "iroh" in t.name or "actant" in t.name)
    ]
    if _leaked:
        # 残留线程会在后续等待中退出；仅在等待超时时才警告。
        pass

    # iroh endpoint close 与 tokio runtime drop 在独立线程完成。
    # Runtime.stop() 已通过 _RuntimeCore::shutdown 等待 network.shutdown()
    # 完成（15s 软超时），正常情况下 stop() 返回时 endpoint 已关闭。
    # 但 tokio worker 线程退出和 Python 端引用释放是异步的，仍需短暂等待。
    # 纯单元测试无这些线程，立即跳过零开销。
    if _has_background_threads():
        _deadline = time.monotonic() + 2.0
        while time.monotonic() < _deadline:
            if not _has_background_threads():
                break
            time.sleep(0.02)
        if _has_background_threads():
            # 等待超时：后台线程未退出，可能影响下一个测试的 Runtime 启动。
            # 打印警告帮助定位，但不 fail（某些场景 tokio worker 退出确实慢）。
            import sys as _sys
            _leaked_names = [
                t.name for t in _threading.enumerate()
                if t is not _threading.main_thread()
                and ("tokio" in t.name or "iroh" in t.name or "actant" in t.name)
            ]
            print(
                f"\n[conftest] WARNING: {len(_leaked_names)} background thread(s) "
                f"still alive after 2s: {_leaked_names[:3]}",
                file=_sys.stderr,
            )
        # 线程退出后 QUIC socket 仍有内核级 TIME_WAIT，短暂 sleep 让内核回收。
        time.sleep(0.2)
    # 无后台线程的纯单元测试：极短 sleep 让 Python 侧引用计数更新。
    else:
        time.sleep(0.02)


# ---------------------------------------------------------------------------
# 分层超时：e2e 测试允许更长
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(items: list[Item]) -> None:
    """根据测试目录应用分层超时。

    - unit/        60s（默认）
    - integration/ 120s
    - e2e/         180s
    """
    for item in items:
        # 根据 path 推断层级
        path_str = str(item.fspath).replace("\\", "/")
        if "/e2e/" in path_str:
            item.add_marker(pytest.mark.timeout(180))
            item.add_marker(pytest.mark.e2e)
        elif "/integration/" in path_str:
            item.add_marker(pytest.mark.timeout(120))
            item.add_marker(pytest.mark.integration)
        elif "/unit/" in path_str:
            item.add_marker(pytest.mark.unit)


# ---------------------------------------------------------------------------
# 计时报告：session 结束时打印按耗时排序的测试列表
# ---------------------------------------------------------------------------


@dataclass
class _TimingRecord:
    name: str
    duration_ms: float


@dataclass
class _TimingCollector:
    records: list[_TimingRecord] = field(default_factory=list)

    def add(self, name: str, duration_ms: float) -> None:
        self.records.append(_TimingRecord(name=name, duration_ms=duration_ms))

    def report(self) -> str:
        if not self.records:
            return ""
        sorted_records = sorted(self.records, key=lambda r: r.duration_ms, reverse=True)
        max_name_len = max(len(r.name) for r in sorted_records)
        lines = [
            "",
            "=" * 72,
            "TEST TIMING REPORT (sorted by duration, slowest first)",
            "=" * 72,
            f"{'test name':<{max_name_len}}  {'ms':>10}  {'label':>6}",
            "-" * (max_name_len + 20),
        ]
        for r in sorted_records:
            label = "SLOW" if r.duration_ms > 5000 else ("warn" if r.duration_ms > 1000 else "")
            lines.append(f"{r.name:<{max_name_len}}  {r.duration_ms:>10.1f}  {label:>6}")
        total = sum(r.duration_ms for r in sorted_records)
        lines.append("-" * (max_name_len + 20))
        lines.append(f"{'TOTAL':<{max_name_len}}  {total:>10.1f}")
        lines.append("=" * 72)
        return "\n".join(lines)


_collector = _TimingCollector()


def pytest_runtest_logreport(report: CallInfo[Item]) -> None:
    """记录 call 阶段耗时。"""
    if report.when == "call":
        _collector.add(report.nodeid, report.duration * 1000)


def pytest_sessionfinish(session: Session, exitstatus: int) -> None:
    """session 结束打印计时报告。"""
    report = _collector.report()
    if report:
        reporter = session.config.pluginmanager.getplugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(report)
        else:  # pragma: no cover
            print(report)


# ---------------------------------------------------------------------------
# 通用同步辅助
# ---------------------------------------------------------------------------


def wait_until(predicate, timeout_s: float = 10.0, interval_s: float = 0.05):
    """轮询同步 predicate 直到返回真或超时。

    返回 predicate 最后一次结果。用于非异步场景等待状态就绪。
    """
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_s)
    return last


@pytest.fixture(name="wait_until")
def wait_until_fixture():
    """提供 wait_until 同步轮询辅助。"""
    return wait_until
