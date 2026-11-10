"""多节点集群编排：同进程与进程外节点的统一入口。

每个节点拥有独立 ``data_dir``、独立 iroh endpoint、独立 Worker/进程池。
集群负责拉起、按拓扑组网、收敛等待、故障注入与清理，不改变 Actant 核心
语义。

隔离契约：

- **命名空间**：Runtime 名称统一加 ``namespace`` 前缀，避免同进程多集群
  的任务/日志混淆。
- **data_dir 隔离**：每节点目录位于 :class:`Sandbox` 根内，禁止共享 LMDB。
- **拓扑显式化**：连接关系由 :class:`Topology` 描述，支持启动时预置、
  运行中应用新拓扑、动态加入/离开。
- **故障显式化**：分区/崩溃/冻结/心跳延迟都通过句柄控制面执行并写入报告。
"""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from .backends import get_backend
from .contract import (
    Backend,
    BackendCapabilities,
    CapabilityError,
    LaunchContext,
    NetworkFidelity,
    NodeHandle,
    NodeSpec,
    Profile,
    ResourceLimits,
    ScenarioRequirements,
    resolve_profile,
)
from .report import ClusterReport
from .sandbox import Sandbox
from .topology import Topology, TopologyChange, diff_topology

NodeMode = str


class ClusterError(RuntimeError):
    """集群编排/收敛失败。"""


class TestCluster:
    """多节点测试集群。

    用法::

        with TestCluster("my-test") as c:
            c.add_node("a", labels={"role": "submit"}, max_concurrent_tasks=0)
            c.add_node("b", labels={"engine": "a"})
            c.start(topology="mesh")
            c.wait_converged()

    ``start()`` 拉起全部节点并按拓扑组网；退出上下文时逆序停止并清理
    沙箱（``data_root`` 由调用方提供时不删除）。
    """

    # 供 pytest 识别：这不是测试类。
    __test__ = False

    def __init__(
        self,
        namespace: str,
        *,
        discovery: str = "local",
        heartbeat_interval_ms: int = 500,
        failure_timeout_ms: int = 3_000,
        starvation_timeout_ms: int = 5_000,
        data_root: str | Path | None = None,
        sandbox: Sandbox | None = None,
        seed: int | None = None,
        keep_data: bool = False,
        backend: str | Backend = "actant",
        profile: str | Profile | None = None,
        fabric: Any | None = None,
        strict_profile: bool = True,
        strict_env: bool = False,
        env_passthrough: Sequence[str] | None = None,
    ) -> None:
        if not namespace:
            raise ValueError("namespace must be non-empty")
        self.namespace = namespace
        self.discovery = discovery
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.failure_timeout_ms = failure_timeout_ms
        self.starvation_timeout_ms = starvation_timeout_ms
        self.seed = seed
        self._backend: Backend = (
            get_backend(backend) if isinstance(backend, str) else backend
        )
        self._profile = resolve_profile(profile)
        self._fabric = fabric
        self._strict_profile = strict_profile
        self._owns_sandbox = sandbox is None
        self._sandbox = sandbox or Sandbox(
            namespace,
            root=data_root,
            keep=keep_data,
            strict_env=strict_env,
            env_passthrough=env_passthrough,
        )
        self._root = self._sandbox.root
        self._specs: dict[str, NodeSpec] = {}
        self._handles: dict[str, NodeHandle] = {}
        self._node_ids: dict[str, str] = {}
        self._data_dirs: dict[str, Path] = {}
        self._started = False
        self._stopped = False
        self._topology: Topology | None = None
        self._connected_pairs: set[tuple[str, str]] = set()
        self._report = ClusterReport(namespace=namespace, seed=seed)
        self._sandbox.track_cleanup(self._stop_all_handles)
        self._report.set_manifest(
            {
                "namespace": namespace,
                "seed": seed,
                "discovery": discovery,
                "backend": self._backend.name,
                "capabilities": self.capabilities.to_dict(),
                "profile": self._profile.to_dict() if self._profile else None,
                "sandbox": self._sandbox.manifest(),
                "config": {
                    "heartbeat_interval_ms": heartbeat_interval_ms,
                    "failure_timeout_ms": failure_timeout_ms,
                    "starvation_timeout_ms": starvation_timeout_ms,
                },
            }
        )

    # ------------------------------------------------------------------
    # 能力协商
    # ------------------------------------------------------------------

    @property
    def backend(self) -> Backend:
        return self._backend

    @property
    def capabilities(self) -> BackendCapabilities:
        provider = getattr(self._backend, "capabilities_for", None)
        if provider is not None:
            return cast(BackendCapabilities, provider(list(self._specs.values())))
        return self._backend.capabilities

    @property
    def profile(self) -> Profile | None:
        return self._profile

    def require(self, requirements: ScenarioRequirements) -> None:
        """断言后端满足场景要求；不满足时抛 :class:`CapabilityError`。"""
        missing = self.capabilities.missing(requirements)
        if missing:
            raise CapabilityError(
                f"backend {self._backend.name!r} does not satisfy scenario: "
                + "; ".join(missing)
            )

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def add_node(
        self,
        name: str,
        *,
        labels: dict[str, str] | None = None,
        max_concurrent_tasks: int | None = None,
        mode: NodeMode = "in-process",
        config: Any | None = None,
        image: str | None = None,
        command: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        resources: ResourceLimits | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        """登记一个节点（必须在 ``start()`` 之前调用；动态加入用 ``join()``）。"""
        if self._started:
            raise ClusterError("cannot add node after cluster started; use join()")
        if name in self._specs:
            raise ClusterError(f"duplicate node name: {name}")
        self._specs[name] = NodeSpec(
            name=name,
            labels=dict(labels or {}),
            max_concurrent_tasks=max_concurrent_tasks,
            mode=mode,
            config=config,
            image=image,
            command=tuple(command) if command is not None else None,
            env=dict(env or {}),
            resources=resources,
            options=dict(options or {}),
        )

    def start(
        self,
        *,
        connect_timeout_s: float = 20.0,
        topology: Topology | str | None = None,
    ) -> None:
        """拉起全部节点、按拓扑组网并等待收敛。"""
        if self._started:
            raise ClusterError("cluster already started")
        if not self._specs:
            raise ClusterError("no nodes registered")
        if self._profile is not None and self._strict_profile:
            self.require(self._profile.requirements())
        self._started = True
        names = list(self._specs)
        self._report.set_manifest(
            {
                "capabilities": self.capabilities.to_dict(),
                "profile": self._profile.to_dict() if self._profile else None,
            }
        )
        resolved = self._resolve_topology(topology, names)
        self._topology = resolved

        for spec in self._specs.values():
            self._start_node(spec)
        self._connect_pairs(_pairs(resolved.edges))
        self.wait_converged(
            timeout_s=connect_timeout_s,
            expected_peers={name: resolved.neighbours(name) for name in names},
        )
        self._record_topology("started")

    def _resolve_topology(self, topology: Topology | str | None, names: list[str]) -> Topology:
        if topology is None:
            return Topology.mesh(names)
        if isinstance(topology, str):
            return Topology.from_name(cast(Any, topology), names)
        if set(topology.nodes) != set(names):
            raise ClusterError(
                f"topology nodes {sorted(topology.nodes)} do not match cluster nodes {sorted(names)}"
            )
        return topology

    def _start_node(self, spec: NodeSpec) -> None:
        data_dir = self._sandbox.node_dir(spec.name)
        runtime_name = f"{self.namespace}-{spec.name}"
        context = LaunchContext(
            namespace=self.namespace,
            node_name=spec.name,
            runtime_name=runtime_name,
            data_dir=data_dir,
            sandbox=self._sandbox,
            defaults={
                "discovery": self.discovery,
                "heartbeat_interval_ms": self.heartbeat_interval_ms,
                "failure_timeout_ms": self.failure_timeout_ms,
                "starvation_timeout_ms": self.starvation_timeout_ms,
                "stderr_path": str(self._sandbox.log_path(spec.name)),
            },
        )
        handle = self._backend.launch(spec, context)
        self._handles[spec.name] = handle
        self._node_ids[spec.name] = handle.node_id
        self._data_dirs[spec.name] = data_dir
        self._report.record(
            "node_started",
            node=spec.name,
            mode=spec.mode,
            runtime_name=runtime_name,
            node_id=handle.node_id,
            isolation=self.capabilities.isolation.value,
            config=handle.describe().get("config"),
        )

    # ------------------------------------------------------------------
    # 拓扑与动态成员
    # ------------------------------------------------------------------

    def _connect_pairs(self, pairs: Iterable[tuple[str, str]]) -> None:
        for pair in sorted(set(pairs)):
            self._ensure_pair(pair, force=False)

    def _ensure_pair(self, pair: tuple[str, str], *, force: bool) -> None:
        left, right = pair
        left_handle = self._handle(left)
        right_handle = self._handle(right)
        if not left_handle.alive or not right_handle.alive:
            return
        canonical = (left, right) if left < right else (right, left)
        if not force and canonical in self._connected_pairs:
            return
        left_handle.dial(right_handle.listen_addresses()["endpoint_addr"])
        right_handle.add_gossip_peer(left_handle.peer_id)
        self._connected_pairs.add(canonical)

    def apply_topology(self, topology: Topology, *, timeout_s: float = 20.0) -> TopologyChange:
        """运行中应用新拓扑（自动补连/断开边），返回差分。"""
        if not self._started:
            raise ClusterError("cluster not started")
        if set(topology.nodes) != set(self._handles):
            raise ClusterError("topology nodes must match active cluster nodes")
        previous = self._topology or Topology.mesh(list(self._handles))
        change = diff_topology(previous, topology)
        for pair in _pairs(change.removed_edges):
            self._cut_pair(pair)
        for pair in _pairs(change.added_edges):
            self._ensure_pair(pair, force=True)
        self._topology = topology
        self._report.record("topology_applied", topology=topology.to_dict(), change=change.to_dict())
        if timeout_s > 0:
            self.wait_converged(
                timeout_s=timeout_s,
                expected_peers={name: topology.neighbours(name) for name in topology.nodes},
            )
        return change

    def join(
        self,
        name: str,
        *,
        labels: dict[str, str] | None = None,
        max_concurrent_tasks: int | None = None,
        mode: NodeMode = "in-process",
        config: Any | None = None,
        image: str | None = None,
        command: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        resources: ResourceLimits | None = None,
        options: dict[str, Any] | None = None,
        connect_to: Iterable[str] | None = None,
        bidirectional: bool = True,
        connect_timeout_s: float = 20.0,
    ) -> NodeHandle:
        """动态加入一个节点并连接到指定节点（默认连接全部现有节点）。"""
        if not self._started:
            raise ClusterError("cluster not started; use add_node() before start")
        if name in self._handles:
            raise ClusterError(f"duplicate node name: {name}")
        spec = NodeSpec(
            name=name,
            labels=dict(labels or {}),
            max_concurrent_tasks=max_concurrent_tasks,
            mode=mode,
            config=config,
            image=image,
            command=tuple(command) if command is not None else None,
            env=dict(env or {}),
            resources=resources,
            options=dict(options or {}),
        )
        self._specs[name] = spec
        self._start_node(spec)

        existing = [n for n in self._handles if n != name]
        targets = list(connect_to) if connect_to is not None else existing
        unknown = set(targets) - set(existing)
        if unknown:
            raise ClusterError(f"connect_to references unknown nodes: {sorted(unknown)}")
        topology = self._topology or Topology.mesh(existing)
        edges = set(topology.edges)
        for target in targets:
            edges.add((name, target))
            if bidirectional:
                edges.add((target, name))
        self._topology = Topology.custom((*topology.nodes, name), edges, name=topology.name)
        self._connect_pairs([(name, target) for target in targets])
        if connect_timeout_s > 0:
            expected = {name: set(targets)}
            for target in targets:
                expected.setdefault(target, set()).add(name)
            self.wait_converged(timeout_s=connect_timeout_s, expected_peers=expected)
        self._record_topology("joined", node=name)
        return self._handle(name)

    def leave(self, name: str, *, timeout_s: float = 30.0) -> None:
        """断开并停止节点（保留其 id 供后续断言）。"""
        handle = self._handle(name)
        topology = self._topology
        if topology is not None:
            neighbours = {t for s, t in topology.edges if s == name}
            neighbours |= {s for s, t in topology.edges if t == name}
            for peer in sorted(neighbours):
                if peer in self._handles:
                    self._cut_pair((name, peer))
            self._topology = Topology.custom(
                [n for n in topology.nodes if n != name],
                [e for e in topology.edges if name not in e],
                name=topology.name,
            )
        handle.stop(timeout_s=timeout_s)
        self._report.record("node_left", node=name)

    def _cut_pair(self, pair: tuple[str, str]) -> None:
        left, right = pair
        canonical = (left, right) if left < right else (right, left)
        if canonical not in self._connected_pairs:
            return
        left_handle = self._handles.get(left)
        right_handle = self._handles.get(right)
        if left_handle is not None and left_handle.alive:
            left_handle.close_peer(right_handle.node_id if right_handle else "")
        if right_handle is not None and right_handle.alive:
            right_handle.close_peer(left_handle.node_id if left_handle else "")
        self._connected_pairs.discard(canonical)
        self._report.record("pair_cut", left=left, right=right)

    def _record_topology(self, action: str, **fields: Any) -> None:
        topology = self._topology
        if topology is None:
            return
        self._report.record(action, topology=topology.to_dict(), **fields)
        self._report.set_manifest({"topology": topology.to_dict()})

    # ------------------------------------------------------------------
    # 句柄访问
    # ------------------------------------------------------------------

    def _handle(self, name: str) -> NodeHandle:
        try:
            return self._handles[name]
        except KeyError:
            raise ClusterError(f"unknown node: {name}") from None

    def handle(self, name: str) -> NodeHandle:
        """取节点句柄（同进程/进程外统一控制面）。"""
        return self._handle(name)

    def handles(self) -> list[NodeHandle]:
        """全部节点句柄（含已停止节点）。"""
        return list(self._handles.values())

    def node_names(self) -> list[str]:
        return list(self._handles)

    def node(self, name: str) -> Any:
        """取同进程节点暴露的运行时对象；进程外节点请用 ``handle()``。"""
        handle = self._handle(name)
        runtime = handle.runtime
        if runtime is None:
            raise ClusterError(
                f"node {name!r} runs out-of-process; use handle({name!r}) for control operations"
            )
        return runtime

    def node_ids(self) -> dict[str, str]:
        """``name -> Actant node_id`` 快照（节点被 kill 后仍可查）。"""
        return dict(self._node_ids)

    def data_dir(self, name: str) -> Path:
        try:
            return self._data_dirs[name]
        except KeyError:
            raise ClusterError(f"unknown node: {name}") from None

    # ------------------------------------------------------------------
    # 收敛
    # ------------------------------------------------------------------

    def wait_converged(
        self,
        *,
        timeout_s: float = 20.0,
        expected_labels: dict[str, dict[str, str]] | None = None,
        expected_peers: dict[str, set[str]] | None = None,
    ) -> None:
        """等待节点看到预期 peer；默认使用当前拓扑的邻居集。"""
        topology = self._topology
        if expected_peers is None:
            if topology is None:
                expected_peers = {
                    name: set(self._handles) - {name} for name in self._handles
                }
            else:
                expected_peers = {
                    name: topology.neighbours(name) for name in self._handles
                }
        node_ids = self.node_ids()

        def converged() -> bool:
            for name, expected_names in expected_peers.items():
                handle = self._handles.get(name)
                if handle is None or not handle.alive:
                    if expected_names:
                        return False
                    continue
                peers = {p["node_id"]: p for p in handle.peers()}
                expected_ids = {node_ids[o] for o in expected_names if o in node_ids}
                if not expected_ids.issubset(peers):
                    return False
                if expected_labels:
                    for other, labels in expected_labels.items():
                        if other == name or other not in node_ids:
                            continue
                        expected_id = node_ids[other]
                        if expected_id in expected_ids and peers.get(expected_id, {}).get(
                            "labels"
                        ) != labels:
                            return False
            return True

        wait_until(converged, timeout_s=timeout_s, description="cluster convergence")
        self._report.record("converged", expected_peers=_serialise_peers(expected_peers))

    def wait_peer_lost(self, name: str, peer_name: str, *, timeout_s: float = 15.0) -> None:
        """等待 ``name`` 的 peer 视图中不再包含 ``peer_name``。"""
        peer_id = self.node_ids()[peer_name]
        handle = self._handle(name)

        def lost() -> bool:
            return all(p["node_id"] != peer_id for p in handle.peers())

        wait_until(lost, timeout_s=timeout_s, description=f"{name} loses {peer_name}")
        self._report.record("peer_lost", node=name, peer=peer_name)

    def wait_peer_visible(self, name: str, peer_name: str, *, timeout_s: float = 15.0) -> None:
        peer_id = self.node_ids()[peer_name]
        handle = self._handle(name)

        def visible() -> bool:
            return any(p["node_id"] == peer_id for p in handle.peers())

        wait_until(visible, timeout_s=timeout_s, description=f"{name} sees {peer_name}")
        self._report.record("peer_visible", node=name, peer=peer_name)

    # ------------------------------------------------------------------
    # 故障注入（薄封装：真实故障语义在 Runtime 钩子）
    # ------------------------------------------------------------------

    def kill(self, name: str) -> None:
        """立即关闭节点（不 drain 在途任务），模拟进程崩溃。"""
        handle = self._handle(name)
        handle.kill()
        self._report.record("node_killed", node=name)

    def crash(self, name: str) -> None:
        """真实崩溃（SIGKILL），仅进程/容器后端支持。"""
        handle = self._handle(name)
        crash = getattr(handle, "crash", None)
        if crash is None:
            raise ClusterError(
                f"backend {self._backend.name!r} does not support real crash"
            )
        crash()
        self._report.record("node_crashed", node=name)

    def freeze(self, name: str) -> None:
        """SIGSTOP 冻结进程外节点（同进程节点不支持）。"""
        self._handle(name).freeze()
        self._report.record("node_frozen", node=name)

    def thaw(self, name: str) -> None:
        self._handle(name).thaw()
        self._report.record("node_thawed", node=name)

    def partition(self, source: str, target: str) -> None:
        """模拟 ``source`` 到 ``target`` 的单向网络分区。"""
        self._fabric_partition(source, target)
        self._handle(source).close_peer(self.node_ids()[target])
        self._report.record("partitioned", source=source, target=target)

    def heal(self, source: str, target: str) -> None:
        """解除单向分区。"""
        self._fabric_heal(source, target)
        self._handle(source).reopen_peer(self.node_ids()[target])
        self._report.record("healed", source=source, target=target)

    def isolate(self, name: str) -> None:
        """隔离节点：切断其与所有已连接邻居的双向链路。"""
        neighbours = {
            (s, t) for s, t in self._connected_pairs if s == name or t == name
        }
        for left, right in sorted(neighbours):
            self._fabric_partition(left, right)
            self._cut_pair((left, right))
        self._report.record("node_isolated", node=name, links=len(neighbours))

    def split(self, groups: list[list[str]]) -> None:
        """按分组切断跨组链路（组内保持连通）。"""
        group_of: dict[str, int] = {}
        for index, group in enumerate(groups):
            for node in group:
                group_of[node] = index
        cut = 0
        for left, right in sorted(self._connected_pairs):
            if group_of.get(left) != group_of.get(right):
                self._fabric_partition(left, right)
                self._cut_pair((left, right))
                cut += 1
        self._report.record("network_split", groups=groups, cut=cut)

    @property
    def _fabric_controls_links(self) -> bool:
        """``SemanticNetem`` 的分区就是委托给集群，避免递归。"""
        return self._fabric is not None and bool(
            getattr(self._fabric, "controls_links", True)
        )

    def _fabric_partition(self, source: str, target: str) -> None:
        fabric = self._fabric
        if fabric is not None and self._fabric_controls_links:
            fabric.partition(source, target)

    def _fabric_heal(self, source: str, target: str) -> None:
        fabric = self._fabric
        if fabric is not None and self._fabric_controls_links:
            fabric.heal(source, target)

    def heal_all(self, *, timeout_s: float = 20.0) -> None:
        """按当前拓扑恢复全部连接（重新拨号 + 重开 peer）。"""
        topology = self._topology
        if topology is None:
            raise ClusterError("cluster has no topology")
        for left, right in sorted(self._connected_pairs):
            left_handle = self._handles.get(left)
            right_handle = self._handles.get(right)
            if left_handle is not None and left_handle.alive and right_handle is not None:
                left_handle.reopen_peer(right_handle.node_id)
            if right_handle is not None and right_handle.alive and left_handle is not None:
                right_handle.reopen_peer(left_handle.node_id)
        for pair in sorted(_pairs(topology.edges)):
            self._fabric_heal(*pair)
            left_handle = self._handles.get(pair[0])
            right_handle = self._handles.get(pair[1])
            if left_handle is not None and left_handle.alive and right_handle is not None:
                left_handle.reopen_peer(right_handle.node_id)
            if right_handle is not None and right_handle.alive and left_handle is not None:
                right_handle.reopen_peer(left_handle.node_id)
            self._ensure_pair(pair, force=True)
        self._report.record("network_healed")
        self.wait_converged(
            timeout_s=timeout_s,
            expected_peers={name: topology.neighbours(name) for name in topology.nodes},
        )

    def pause_heartbeat(self, name: str, ms: int) -> None:
        """让节点在 ``ms`` 毫秒内不广播心跳（弱网模拟复用同一入口）。"""
        self._handle(name).delay_heartbeat(ms)
        self._report.record("heartbeat_paused", node=name, ms=ms)

    def drift_clock(self, name: str, ms: int) -> None:
        """给节点 HLC 注入时钟偏移（毫秒）。"""
        self._handle(name).inject_hlc_drift(ms)
        self._report.record("clock_drift", node=name, ms=ms)

    def set_max_concurrent_tasks(self, name: str, new_max: int) -> None:
        self._handle(name).set_max_concurrent_tasks(new_max)
        self._report.record("max_concurrent_changed", node=name, new_max=new_max)

    def metrics_text(self, name: str) -> str:
        return self._handle(name).metrics_text()

    # ------------------------------------------------------------------
    # 停止 / 清理
    # ------------------------------------------------------------------

    def _stop_all_handles(self) -> None:
        for handle in reversed(list(self._handles.values())):
            if handle.alive:
                handle.stop()

    def stop(self, *, timeout_s: float = 30.0) -> None:
        """逆序停止全部节点并清理沙箱（幂等）。"""
        if self._stopped:
            return
        self._stopped = True
        for name, handle in reversed(list(self._handles.items())):
            try:
                if handle.alive:
                    handle.stop(timeout_s=timeout_s)
            finally:
                self._report.record("node_stopped", node=name)
        self._report.set_manifest({"sandbox": self._sandbox.manifest()})
        if self._owns_sandbox:
            self._sandbox.cleanup()

    def __enter__(self) -> TestCluster:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # 断言 / 报告
    # ------------------------------------------------------------------

    @property
    def report(self) -> ClusterReport:
        return self._report

    @property
    def sandbox(self) -> Sandbox:
        return self._sandbox

    @property
    def topology(self) -> Topology | None:
        return self._topology

    @property
    def fabric(self) -> Any | None:
        """可选的链路级网络 Fabric（``SemanticNetem`` / ``TcNetemFabric`` 等）。"""
        return self._fabric

    def use_fabric(self, fabric: Any) -> None:
        """装配 Fabric；``SemanticNetem`` 需要集群实例，故构造后再挂载。"""
        self._fabric = fabric
        self._report.set_manifest(
            {
                "fabric": {
                    "type": type(fabric).__name__,
                    "fidelity": getattr(fabric, "fidelity", NetworkFidelity.NONE).value,
                }
            }
        )

    def shape_network(self, profile: Any, *, duration_s: float | None = None) -> None:
        """通过已配置的 Fabric 应用弱网参数；未配置时给出明确指引。"""
        if self._fabric is None:
            raise ClusterError(
                "no network fabric configured; pass fabric=SemanticNetem(...) "
                "or fabric=TcNetemFabric(...) to TestCluster"
            )
        missing = self._fabric.supports(profile)
        if missing:
            raise ClusterError(
                "network fabric cannot express: " + ", ".join(sorted(missing))
            )
        self._fabric.apply(profile, duration_s=duration_s)
        self._report.record(
            "network_shaped",
            fidelity=self._fabric.fidelity.value,
            profile=getattr(profile, "to_dict", lambda: str(profile))(),
            duration_s=duration_s,
        )

    def manifest(self) -> dict[str, Any]:
        """完整环境清单（可供 ``pytest`` 失败时落盘）。"""
        return {
            "namespace": self.namespace,
            "seed": self.seed,
            "backend": self._backend.name,
            "capabilities": self.capabilities.to_dict(),
            "profile": self._profile.to_dict() if self._profile else None,
            "fidelity": self._delivered_fidelity(),
            "fabric": (
                {
                    "type": type(self._fabric).__name__,
                    "fidelity": getattr(self._fabric, "fidelity", NetworkFidelity.NONE).value,
                }
                if self._fabric is not None
                else None
            ),
            "topology": self._topology.to_dict() if self._topology else None,
            "nodes": [handle.describe() for handle in self._handles.values()],
            "sandbox": self._sandbox.manifest(),
        }

    def _delivered_fidelity(self) -> dict[str, Any]:
        capabilities = self.capabilities
        modes = {spec.mode for spec in self._specs.values()}
        return {
            "isolation": capabilities.isolation.value,
            "network": capabilities.network.value,
            "clock": capabilities.clock.value,
            "resources": capabilities.resources,
            "node_modes": sorted(modes),
        }


def _pairs(edges: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    """把有向边规范化为无向对（去重、排序）。"""
    seen: set[tuple[str, str]] = set()
    for left, right in edges:
        if left == right:
            continue
        seen.add((left, right) if left < right else (right, left))
    return sorted(seen)


def _serialise_peers(peers: dict[str, set[str]] | None) -> dict[str, list[str]] | None:
    if peers is None:
        return None
    return {name: sorted(items) for name, items in peers.items()}


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float,
    description: str,
    interval_s: float = 0.2,
) -> None:
    """轮询 ``predicate`` 直到为真；超时抛 ``ClusterError``。"""
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:
            last_error = exc
        time.sleep(interval_s)
    detail = f"; last error: {last_error}" if last_error else ""
    raise ClusterError(f"timeout after {timeout_s}s waiting for {description}{detail}")


@contextmanager
def cluster(namespace: str, **kwargs: Any) -> Iterator[TestCluster]:
    """``TestCluster`` 的上下文管理器快捷方式。"""
    c = TestCluster(namespace, **kwargs)
    try:
        yield c
    finally:
        c.stop()


def cleanup_root(path: str | Path) -> None:
    """显式删除外部提供的 ``data_root``（沙箱不拥有时不自动删除）。"""
    shutil.rmtree(path, ignore_errors=True)
