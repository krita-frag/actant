"""顶层 pytest 配置：环境隔离、计时报告、分层标记。

测试体系分层：
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
