"""被测应用（A.U.T.）无关的测试台契约。

本模块只定义"测试台需要什么"，不依赖 ``actant`` 或任何具体运行时：

- :class:`NodeSpec` / :class:`ResourceLimits`：可序列化的节点规格；
- :class:`NodeHandle`：编排器所需的最小控制面（由后端实现）；
- :class:`EventCodec`：把后端事件转换为统一的 trace 字典；
- :class:`Backend`：节点拉起 + 事件解码 + 能力声明；
- :class:`BackendCapabilities` / :class:`ScenarioRequirements`：隔离与故障能力协商；
- :class:`Profile`：``dev`` / ``ci`` / ``staging`` / ``prod_like`` 保真预设。

设计原则：**能力不足必须显式失败，而不是静默降级**。测试声明自己需要的
隔离等级、网络保真度、时钟模式与故障类型，后端在用例开始时协商；无法满足
时抛 :class:`CapabilityError`，并用报告记录"请求 vs 实际"。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


class TestkitError(RuntimeError):
    """testkit 通用错误基类。"""


class CapabilityError(TestkitError):
    """后端能力不满足场景需求。"""


class BackendError(TestkitError):
    """后端拉起/控制失败。"""


class BackendUnavailable(BackendError):
    """后端所需的外部能力（docker/netns/tc 等）不可用。"""


class IsolationLevel(str, enum.Enum):
    """节点进程/资源隔离等级（由弱到强）。"""

    NONE = "none"
    PROCESS = "process"
    CONTAINER = "container"
    NETNS = "netns"


class NetworkFidelity(str, enum.Enum):
    """网络故障模拟保真度（由弱到强）。"""

    NONE = "none"
    SEMANTIC = "semantic"
    PROXY = "proxy"
    KERNEL = "kernel"


class ClockMode(str, enum.Enum):
    """节点时钟隔离模式（由弱到强）。"""

    SHARED = "shared"
    HLC = "hlc"
    REAL = "real"


class FaultKind(str, enum.Enum):
    """故障类型。后端通过 :attr:`BackendCapabilities.faults` 声明支持范围。"""

    CUSTOM = "custom"
    KILL = "kill"
    CRASH = "crash"
    FREEZE = "freeze"
    THAW = "thaw"
    PARTITION = "partition"
    HEAL = "heal"
    ISOLATE = "isolate"
    SPLIT = "split"
    PAUSE_HEARTBEAT = "pause_heartbeat"
    CLOCK_DRIFT = "clock_drift"
    PACKET_LOSS = "packet_loss"
    PACKET_DELAY = "packet_delay"
    BANDWIDTH = "bandwidth"
    DUPLICATE = "duplicate"
    REORDER = "reorder"
    OOM = "oom"
    DISK_FULL = "disk_full"
    CPU_THROTTLE = "cpu_throttle"


_ISOLATION_RANK = {
    IsolationLevel.NONE: 0,
    IsolationLevel.PROCESS: 1,
    IsolationLevel.CONTAINER: 2,
    IsolationLevel.NETNS: 3,
}
_NETWORK_RANK = {
    NetworkFidelity.NONE: 0,
    NetworkFidelity.SEMANTIC: 1,
    NetworkFidelity.PROXY: 2,
    NetworkFidelity.KERNEL: 3,
}
_CLOCK_RANK = {
    ClockMode.SHARED: 0,
    ClockMode.HLC: 1,
    ClockMode.REAL: 2,
}


@dataclass(frozen=True)
class ResourceLimits:
    """节点资源上限。``None`` 表示不限制。

    仅容器/进程后端可以真正执行；``in-process`` 后端声明不支持时会在
    协商阶段失败，而不是忽略。
    """

    cpu_cores: float | None = None
    memory_mb: int | None = None
    pids: int | None = None
    open_files: int | None = None
    cpu_seconds: int | None = None
    disk_mb: int | None = None

    def is_empty(self) -> bool:
        return all(
            value is None
            for value in (
                self.cpu_cores,
                self.memory_mb,
                self.pids,
                self.open_files,
                self.cpu_seconds,
                self.disk_mb,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpu_cores": self.cpu_cores,
            "memory_mb": self.memory_mb,
            "pids": self.pids,
            "open_files": self.open_files,
            "cpu_seconds": self.cpu_seconds,
            "disk_mb": self.disk_mb,
        }


@dataclass
class NodeSpec:
    """节点规格（拉起前的静态描述）。

    ``config`` 与 ``options`` 是后端私有字段：Actant 后端把 ``config`` 解释为
    :class:`act_ext_testkit.node.NodeConfig`，其他后端可以自由使用
    ``options``。公共编排逻辑只读取 ``name`` / ``labels`` / ``mode`` /
    ``resources``。
    """

    name: str
    labels: dict[str, str] = field(default_factory=dict)
    max_concurrent_tasks: int | None = None
    mode: str = "in-process"
    config: Any = None
    image: str | None = None
    command: tuple[str, ...] | None = None
    env: dict[str, str] = field(default_factory=dict)
    resources: ResourceLimits | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "labels": dict(self.labels),
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "mode": self.mode,
            "image": self.image,
            "command": list(self.command) if self.command else None,
            "env": dict(self.env),
            "resources": self.resources.to_dict() if self.resources else None,
            "options": dict(self.options),
        }


@runtime_checkable
class NodeHandle(Protocol):
    """编排器所需的最小节点控制面。

    句柄可以额外实现 :class:`Observation` 等能力；编排器只依赖这里声明的
    方法，因此其他后端无需实现 Actant 专有的 gossip/HLC 操作。
    """

    name: str
    mode: str

    @property
    def node_id(self) -> str: ...

    @property
    def peer_id(self) -> str: ...

    @property
    def alive(self) -> bool: ...

    @property
    def runtime(self) -> Any | None: ...

    def listen_addresses(self) -> dict[str, Any]: ...

    def peers(self) -> list[dict[str, Any]]: ...

    def dial(self, addr: str) -> None: ...

    def add_gossip_peer(self, peer_id: str) -> None: ...

    def close_peer(self, node_id: str) -> None: ...

    def reopen_peer(self, node_id: str) -> None: ...

    def delay_heartbeat(self, ms: int) -> None: ...

    def inject_hlc_drift(self, ms: int) -> None: ...

    def set_max_concurrent_tasks(self, new_max: int) -> None: ...

    def metrics_text(self) -> str: ...

    def stop(self, *, timeout_s: float = 30.0) -> None: ...

    def kill(self) -> None: ...

    def freeze(self) -> None: ...

    def thaw(self) -> None: ...

    def describe(self) -> dict[str, Any]: ...


class EventCodec(Protocol):
    """把后端原始事件解码为 ``TraceRecorder`` 可消费的字典。

    返回值字段与 :class:`act_ext_testkit.trace.TraceEvent` 对齐；返回
    ``None`` 表示该事件与 trace 无关。
    """

    def lifecycle(self, raw: Any) -> dict[str, Any] | None: ...

    def log(self, raw: Any) -> dict[str, Any] | None: ...

    def worker(self, raw: Any) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class BackendCapabilities:
    """后端实际提供的隔离/模拟能力。"""

    isolation: IsolationLevel = IsolationLevel.NONE
    network: NetworkFidelity = NetworkFidelity.NONE
    clock: ClockMode = ClockMode.SHARED
    faults: frozenset[FaultKind] = frozenset()
    resources: bool = False
    black_box: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "isolation": self.isolation.value,
            "network": self.network.value,
            "clock": self.clock.value,
            "faults": sorted(kind.value for kind in self.faults),
            "resources": self.resources,
            "black_box": self.black_box,
        }

    def missing(self, requirements: ScenarioRequirements) -> list[str]:
        """返回未满足的需求描述（空列表 = 全部满足）。"""
        missing: list[str] = []
        if requirements.isolation is not None and _ISOLATION_RANK[
            requirements.isolation
        ] > _ISOLATION_RANK[self.isolation]:
            missing.append(
                f"isolation>={requirements.isolation.value} "
                f"(backend={self.isolation.value})"
            )
        if requirements.network is not None and _NETWORK_RANK[
            requirements.network
        ] > _NETWORK_RANK[self.network]:
            missing.append(
                f"network>={requirements.network.value} "
                f"(backend={self.network.value})"
            )
        if requirements.clock is not None and _CLOCK_RANK[
            requirements.clock
        ] > _CLOCK_RANK[self.clock]:
            missing.append(
                f"clock>={requirements.clock.value} (backend={self.clock.value})"
            )
        if requirements.resources and not self.resources:
            missing.append("resource limits")
        if requirements.black_box and not self.black_box:
            missing.append("black-box launch")
        unsupported = sorted(
            kind.value for kind in requirements.faults if kind not in self.faults
        )
        if unsupported:
            missing.append("faults=" + ",".join(unsupported))
        return missing


@dataclass(frozen=True)
class ScenarioRequirements:
    """一个测试场景对隔离能力的最低要求。"""

    isolation: IsolationLevel | None = None
    network: NetworkFidelity | None = None
    clock: ClockMode | None = None
    faults: frozenset[FaultKind] = frozenset()
    resources: bool = False
    black_box: bool = False

    @classmethod
    def from_profile(cls, profile: Profile) -> ScenarioRequirements:
        return cls(
            isolation=profile.isolation,
            network=profile.network,
            clock=profile.clock,
            faults=profile.faults,
            resources=profile.resources,
        )


@dataclass(frozen=True)
class Profile:
    """保真预设：声明"这组测试需要多真实的环境"。"""

    name: str
    isolation: IsolationLevel
    network: NetworkFidelity
    clock: ClockMode
    resources: bool = False
    faults: frozenset[FaultKind] = frozenset()

    def requirements(self) -> ScenarioRequirements:
        return ScenarioRequirements.from_profile(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "isolation": self.isolation.value,
            "network": self.network.value,
            "clock": self.clock.value,
            "resources": self.resources,
            "faults": sorted(kind.value for kind in self.faults),
        }


_SEMANTIC_FAULTS = frozenset(
    {
        FaultKind.KILL,
        FaultKind.PARTITION,
        FaultKind.HEAL,
        FaultKind.ISOLATE,
        FaultKind.SPLIT,
        FaultKind.PAUSE_HEARTBEAT,
        FaultKind.CLOCK_DRIFT,
    }
)
_PROCESS_FAULTS = _SEMANTIC_FAULTS | {
    FaultKind.CRASH,
    FaultKind.FREEZE,
    FaultKind.THAW,
}
_KERNEL_FAULTS = _PROCESS_FAULTS | {
    FaultKind.PACKET_LOSS,
    FaultKind.PACKET_DELAY,
    FaultKind.BANDWIDTH,
    FaultKind.DUPLICATE,
    FaultKind.REORDER,
    FaultKind.OOM,
    FaultKind.DISK_FULL,
    FaultKind.CPU_THROTTLE,
}

PROFILES: dict[str, Profile] = {
    "dev": Profile(
        name="dev",
        isolation=IsolationLevel.NONE,
        network=NetworkFidelity.SEMANTIC,
        clock=ClockMode.HLC,
        faults=_SEMANTIC_FAULTS,
    ),
    "ci": Profile(
        name="ci",
        isolation=IsolationLevel.PROCESS,
        network=NetworkFidelity.SEMANTIC,
        clock=ClockMode.HLC,
        faults=_PROCESS_FAULTS,
    ),
    "staging": Profile(
        name="staging",
        isolation=IsolationLevel.CONTAINER,
        network=NetworkFidelity.PROXY,
        clock=ClockMode.REAL,
        resources=True,
        faults=_KERNEL_FAULTS - {FaultKind.OOM, FaultKind.DISK_FULL},
    ),
    "prod_like": Profile(
        name="prod_like",
        isolation=IsolationLevel.NETNS,
        network=NetworkFidelity.KERNEL,
        clock=ClockMode.REAL,
        resources=True,
        faults=_KERNEL_FAULTS,
    ),
}


def resolve_profile(profile: str | Profile | None) -> Profile | None:
    """把 ``None``/名称/对象统一为 :class:`Profile`。"""
    if profile is None:
        return None
    if isinstance(profile, Profile):
        return profile
    try:
        return PROFILES[profile]
    except KeyError:
        raise ValueError(
            f"unknown profile: {profile!r}; available: {sorted(PROFILES)}"
        ) from None


@dataclass(frozen=True)
class LaunchContext:
    """后端拉起节点时可见的编排上下文。"""

    namespace: str
    node_name: str
    runtime_name: str
    data_dir: Path
    sandbox: Any
    defaults: dict[str, Any] = field(default_factory=dict)


class Backend(Protocol):
    """被测应用后端。

    实现者负责：拉起节点、提供控制面句柄、声明能力、解码事件。
    编排器（``TestCluster``）只依赖该协议。
    """

    name: str
    capabilities: BackendCapabilities

    def launch(self, spec: NodeSpec, context: LaunchContext) -> NodeHandle: ...

    def codec(self) -> EventCodec: ...


class NetworkFabric(Protocol):
    """链路级网络控制面。

    ``SemanticNetem`` 是 ``fidelity='semantic'`` 的实现；``CommandFabric`` /
    ``TcNetemFabric`` 提供内核/代理级实现。``apply`` 的 profile 由
    ``act_ext_testkit.netem.NetworkProfile`` 提供，这里用 ``Any`` 避免
    契约层反向依赖。
    """

    fidelity: NetworkFidelity

    def supports(self, profile: Any) -> set[str]:
        """返回该 profile 中本 fabric 无法表达的参数名。"""
        ...

    def apply(self, profile: Any, *, duration_s: float | None = None) -> None: ...

    def clear(self) -> None: ...

    def partition(self, source: str, target: str) -> None: ...

    def heal(self, source: str, target: str) -> None: ...
