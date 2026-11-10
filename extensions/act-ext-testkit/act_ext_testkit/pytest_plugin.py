"""pytest 集成：fixtures、markers 与失败现场落盘。

启用方式（任选其一）：

- 在 ``conftest.py`` 中设置 ``pytest_plugins = ["act_ext_testkit.pytest_plugin"]``；
- 安装包后 pytest 自动加载 ``pytest11`` entry point。

提供：

- ``testkit_seed``：可复现随机种子（``--testkit-seed`` / ``ACTANT_TEST_SEED``）；
- ``sandbox``：函数级隔离目录，失败时按需保留；
- ``cluster_factory``：创建并自动清理 ``TestCluster``，失败时把报告写入
  ``testkit-artifacts/``；
- markers：``p2p`` / ``deterministic`` / ``chaos`` / ``consistency`` /
  ``recovery`` / ``performance`` / ``topology`` / ``netem`` / ``nat`` /
  ``adversarial`` / ``slow``；对应"故障是常态"下的分层覆盖；
- 未启用外部 NAT 环境时，``@pytest.mark.nat`` 用例被**显式跳过**并给出原因。

后端与保真度可全局选择：

- ``--testkit-backend NAME``：选择已注册的后端（默认 ``actant``）；
- ``--testkit-app module:factory``：从外部模块加载并注册后端（衍生应用）；
- ``--testkit-profile``：``dev`` / ``ci`` / ``staging`` / ``prod_like``；
- ``--testkit-require``：追加场景最低要求，如
  ``isolation=process``、``faults=crash,freeze``、``resources``。
"""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pytest

from .backends import register_backend
from .cluster import TestCluster
from .contract import (
    ClockMode,
    FaultKind,
    IsolationLevel,
    NetworkFidelity,
    ScenarioRequirements,
)
from .nat import NAT_ENV, nat_enabled
from .sandbox import Sandbox

_MARKERS = {
    "testkit": "act-ext-testkit 相关测试",
    "p2p": "P2P 发现、对等连接与收敛",
    "deterministic": "确定性模拟/可复现故障计划",
    "chaos": "混沌故障注入（需要较长执行时间）",
    "consistency": "一致性断言（唯一终态、无重复执行）",
    "recovery": "故障恢复与重新收敛",
    "performance": "性能观测（只采样、不设阈值）",
    "topology": "拓扑动态（join/leave/split/heal）",
    "netem": "弱网语义模拟测试",
    "nat": f"需要真实 NAT/relay 环境（设置 {NAT_ENV}=1 启用）",
    "adversarial": "对抗行为/信任边界测试",
    "slow": "耗时较长的集成测试",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("act-ext-testkit")
    group.addoption(
        "--testkit-seed",
        action="store",
        default=None,
        help="混沌/弱网测试随机种子（默认读取 ACTANT_TEST_SEED 或 0）",
    )
    group.addoption(
        "--testkit-artifacts",
        action="store",
        default="testkit-artifacts",
        help="失败现场报告输出目录",
    )
    group.addoption(
        "--testkit-keep-sandbox",
        action="store_true",
        default=False,
        help="测试失败时保留沙箱目录",
    )
    group.addoption(
        "--testkit-backend",
        action="store",
        default=None,
        help="节点后端名称（默认 actant；需已通过 entry point 或 --testkit-app 注册）",
    )
    group.addoption(
        "--testkit-app",
        action="store",
        default=None,
        help="从 'module:factory' 加载外部被测应用后端并注册",
    )
    group.addoption(
        "--testkit-profile",
        action="store",
        default=None,
        help="保真预设：dev / ci / staging / prod_like",
    )
    group.addoption(
        "--testkit-require",
        action="append",
        default=None,
        metavar="REQ",
        help="场景最低要求，可重复，如 isolation=process 或 faults=crash,freeze",
    )


def pytest_configure(config: pytest.Config) -> None:
    for name, description in _MARKERS.items():
        config.addinivalue_line("markers", f"{name}: {description}")
    _load_app_backend(config)
    _requirements(config)


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    if nat_enabled():
        return
    skip = pytest.mark.skip(reason=f"real NAT/relay tests require {NAT_ENV}=1")
    for item in items:
        if item.get_closest_marker("nat") is not None:
            item.add_marker(skip)


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]) -> Iterator[None]:
    outcome = yield
    if outcome is None:
        return
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


def _seed(config: pytest.Config) -> int:
    value = config.getoption("--testkit-seed")
    if value is None:
        value = os.environ.get("ACTANT_TEST_SEED", "0")
    return int(value)


def _artifacts_dir(config: pytest.Config) -> Path:
    return Path(config.getoption("--testkit-artifacts"))


def _load_app_backend(config: pytest.Config) -> None:
    """加载 ``--testkit-app module:factory`` 并注册为可选后端。"""
    spec = config.getoption("--testkit-app")
    if not spec:
        return
    module_name, separator, attr = str(spec).partition(":")
    if not separator or not module_name or not attr:
        raise pytest.UsageError("--testkit-app must be 'module:factory'")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise pytest.UsageError(f"cannot import --testkit-app module {module_name!r}: {exc}") from exc
    factory = getattr(module, attr, None)
    if factory is None:
        raise pytest.UsageError(f"{module_name!r} has no attribute {attr!r}")
    backend = factory() if callable(factory) else factory
    name = getattr(backend, "name", None)
    if not name or not hasattr(backend, "launch"):
        raise pytest.UsageError(f"--testkit-app {spec!r} did not produce a Backend")
    register_backend(str(name), backend)
    config._testkit_app_backend = str(name)  # type: ignore[attr-defined]


def _backend_name(config: pytest.Config) -> str:
    selected = config.getoption("--testkit-backend")
    if selected:
        return str(selected)
    return str(getattr(config, "_testkit_app_backend", None) or "actant")


def _profile(config: pytest.Config) -> str | None:
    value = config.getoption("--testkit-profile") or os.environ.get("ACTANT_TEST_PROFILE")
    return str(value) if value else None


def _bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _requirements(config: pytest.Config) -> ScenarioRequirements | None:
    """把 ``--testkit-require`` / ``ACTANT_TEST_REQUIRE`` 解析为需求对象。"""
    specs = list(config.getoption("--testkit-require") or [])
    env = os.environ.get("ACTANT_TEST_REQUIRE")
    if env:
        specs.extend(part for part in env.split(";") if part.strip())
    if not specs:
        return None
    isolation: IsolationLevel | None = None
    network: NetworkFidelity | None = None
    clock: ClockMode | None = None
    faults: set[FaultKind] = set()
    resources = False
    black_box = False
    try:
        for spec in specs:
            for token in str(spec).split(";"):
                token = token.strip()
                if not token:
                    continue
                key, _, value = token.partition("=")
                key = key.strip().lower().replace("-", "_")
                value = value.strip()
                if key == "isolation":
                    isolation = IsolationLevel(value)
                elif key == "network":
                    network = NetworkFidelity(value)
                elif key == "clock":
                    clock = ClockMode(value)
                elif key in {"fault", "faults"}:
                    faults.update(FaultKind(item.strip()) for item in value.split(",") if item.strip())
                elif key in {"resources", "resource_limits"}:
                    resources = True if not value else _bool(value)
                elif key in {"black_box", "blackbox"}:
                    black_box = True if not value else _bool(value)
                else:
                    raise ValueError(f"unknown requirement key {key!r}")
    except ValueError as exc:
        raise pytest.UsageError(f"invalid --testkit-require value: {exc}") from exc
    return ScenarioRequirements(
        isolation=isolation,
        network=network,
        clock=clock,
        faults=frozenset(faults),
        resources=resources,
        black_box=black_box,
    )


@pytest.fixture(scope="function")
def testkit_seed(request: pytest.FixtureRequest) -> int:
    """本次测试使用的确定性种子。"""
    return _seed(request.config)


@pytest.fixture()
def sandbox(request: pytest.FixtureRequest, testkit_seed: int) -> Iterator[Sandbox]:
    """函数级沙箱；失败时按 ``--testkit-keep-sandbox`` 保留。"""
    box = Sandbox(request.node.nodeid.replace("::", "-"), env={"ACTANT_TEST_SEED": str(testkit_seed)})
    try:
        yield box
    finally:
        failed = getattr(request.node, "rep_call", None) is not None and getattr(
            request.node.rep_call, "failed", False
        )
        box.keep = box.keep or (
            failed and bool(request.config.getoption("--testkit-keep-sandbox"))
        )
        if failed:
            _dump_sandbox(request.config, request.node.nodeid, box)
        box.cleanup()


class ClusterFactory:
    """``cluster_factory`` fixture 的返回值。"""

    def __init__(self, request: pytest.FixtureRequest, seed: int) -> None:
        self._request = request
        self._seed = seed
        self._clusters: list[TestCluster] = []

    def __call__(
        self,
        namespace: str,
        nodes: Iterable[dict[str, Any]] | None = None,
        *,
        topology: Any = "mesh",
        start: bool = True,
        connect_timeout_s: float = 20.0,
        **cluster_kwargs: Any,
    ) -> TestCluster:
        """创建集群；``nodes`` 为 ``add_node`` 参数字典序列。"""
        cluster_kwargs.setdefault("seed", self._seed)
        cluster_kwargs.setdefault("backend", _backend_name(self._request.config))
        profile = _profile(self._request.config)
        if profile is not None:
            cluster_kwargs.setdefault("profile", profile)
        cluster = TestCluster(namespace, **cluster_kwargs)
        self._clusters.append(cluster)
        if nodes is not None:
            for spec in nodes:
                params = dict(spec)
                name = params.pop("name")
                cluster.add_node(name, **params)
        requirements = _requirements(self._request.config)
        if requirements is not None:
            cluster.require(requirements)
        if start:
            cluster.start(topology=topology, connect_timeout_s=connect_timeout_s)
        return cluster

    def stop_all(self) -> None:
        for cluster in reversed(self._clusters):
            cluster.stop()


@pytest.fixture()
def cluster_factory(request: pytest.FixtureRequest, testkit_seed: int) -> Iterator[ClusterFactory]:
    """创建自动清理的 ``TestCluster``；失败时落盘报告。"""
    factory = ClusterFactory(request, testkit_seed)
    try:
        yield factory
    finally:
        failed = getattr(request.node, "rep_call", None) is not None and getattr(
            request.node.rep_call, "failed", False
        )
        if failed:
            for index, cluster in enumerate(factory._clusters):
                _dump_cluster(request.config, request.node.nodeid, cluster, index)
        factory.stop_all()


def _dump_cluster(config: pytest.Config, nodeid: str, cluster: TestCluster, index: int) -> None:
    try:
        cluster.report.set_manifest(cluster.manifest())
        target = _artifacts_dir(config) / _safe(nodeid) / f"cluster-{index}-{cluster.namespace}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        cluster.report.dump(target)
    except Exception:  # 报告落盘失败不能掩盖原始测试失败
        return


def _dump_sandbox(config: pytest.Config, nodeid: str, box: Sandbox) -> None:
    try:
        target = _artifacts_dir(config) / _safe(nodeid) / "sandbox.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(box.manifest(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:  # 报告落盘失败不能掩盖原始测试失败
        return


def _safe(value: str) -> str:
    return "".join(ch if (ch.isalnum() or ch in "-_.") else "-" for ch in value)[:120]
