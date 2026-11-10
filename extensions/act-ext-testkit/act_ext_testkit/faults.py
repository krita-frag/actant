"""确定性混沌故障注入。

设计原则：**故障脚本是数据，不是线程里的副作用**。``FaultScript`` 先构造
一个可序列化、可按 ``(at_s, 插入顺序)`` 排序的计划，再由 ``FaultRun`` 按
相对时间执行。相同 seed + 相同节点集 → 相同计划，便于失败复现。

故障只调用 ``TestCluster`` 的公开故障原语（partition/heal/kill/...），
不直接操作 iroh/actor 内部状态。
"""

from __future__ import annotations

import hashlib
import json
import random
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, cast

from .contract import CapabilityError, FaultKind

if TYPE_CHECKING:
    from .cluster import TestCluster


class FaultError(RuntimeError):
    """故障脚本执行失败。"""


class FaultTarget(Protocol):
    """故障脚本所需的最小控制面（由 ``TestCluster`` 结构性满足）。

    故障只依赖这些原语，测试替身无需模拟整个集群即可验证脚本执行。
    """

    def partition(self, source: str, target: str) -> None: ...

    def heal(self, source: str, target: str) -> None: ...

    def kill(self, name: str) -> None: ...

    def crash(self, name: str) -> None: ...

    def freeze(self, name: str) -> None: ...

    def thaw(self, name: str) -> None: ...

    def pause_heartbeat(self, name: str, ms: int) -> None: ...

    def drift_clock(self, name: str, ms: int) -> None: ...

    def isolate(self, name: str) -> None: ...


# ----------------------------------------------------------------------
# 故障描述（不可变数据）
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Partition:
    """单向分区：``source`` 不再看到 ``target``。"""

    source: str
    target: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.PARTITION

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "partition", "source": self.source, "target": self.target, "at_s": self.at_s}


@dataclass(frozen=True)
class Heal:
    """解除 ``Partition``。"""

    source: str
    target: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.HEAL

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "heal", "source": self.source, "target": self.target, "at_s": self.at_s}


@dataclass(frozen=True)
class Kill:
    """节点关闭（不 drain 在途任务）。

    同进程后端只能做协作式关闭；需要真实崩溃请用 :class:`Crash`。
    """

    node: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.KILL

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "kill", "node": self.node, "at_s": self.at_s}


@dataclass(frozen=True)
class Crash:
    """真实进程崩溃（SIGKILL），仅进程/容器后端支持。"""

    node: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.CRASH

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "crash", "node": self.node, "at_s": self.at_s}


@dataclass(frozen=True)
class Freeze:
    """SIGSTOP 冻结节点进程，仅进程/容器后端支持。"""

    node: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.FREEZE

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "freeze", "node": self.node, "at_s": self.at_s}


@dataclass(frozen=True)
class Thaw:
    """恢复被 ``Freeze`` 的节点。"""

    node: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.THAW

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "thaw", "node": self.node, "at_s": self.at_s}


@dataclass(frozen=True)
class PauseHeartbeat:
    """心跳静默 ``ms`` 毫秒（弱网/抖动近似）。"""

    node: str
    ms: int
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.PAUSE_HEARTBEAT

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "pause_heartbeat", "node": self.node, "ms": self.ms, "at_s": self.at_s}


@dataclass(frozen=True)
class ClockDrift:
    """HLC 时钟偏移 ``ms`` 毫秒。"""

    node: str
    ms: int
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.CLOCK_DRIFT

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "clock_drift", "node": self.node, "ms": self.ms, "at_s": self.at_s}


@dataclass(frozen=True)
class Isolate:
    """隔离节点：切断与所有邻居的双向连接。"""

    node: str
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.ISOLATE

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "isolate", "node": self.node, "at_s": self.at_s}


@dataclass(frozen=True)
class CustomFault:
    """用户自定义动作（可序列化为标签，不序列化回调）。"""

    label: str
    action: Callable[[TestCluster], None]
    at_s: float = 0.0

    kind: ClassVar[FaultKind] = FaultKind.CUSTOM

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "custom", "label": self.label, "at_s": self.at_s}


Fault = (
    Partition
    | Heal
    | Kill
    | Crash
    | Freeze
    | Thaw
    | PauseHeartbeat
    | ClockDrift
    | Isolate
    | CustomFault
)


def apply_fault(cluster: FaultTarget, fault: Fault) -> None:
    """把一条故障描述应用到集群。"""
    if isinstance(fault, Partition):
        cluster.partition(fault.source, fault.target)
    elif isinstance(fault, Heal):
        cluster.heal(fault.source, fault.target)
    elif isinstance(fault, Kill):
        cluster.kill(fault.node)
    elif isinstance(fault, Crash):
        _call_optional(cluster, "crash", fault.node)
    elif isinstance(fault, Freeze):
        _call_optional(cluster, "freeze", fault.node)
    elif isinstance(fault, Thaw):
        _call_optional(cluster, "thaw", fault.node)
    elif isinstance(fault, PauseHeartbeat):
        cluster.pause_heartbeat(fault.node, fault.ms)
    elif isinstance(fault, ClockDrift):
        cluster.drift_clock(fault.node, fault.ms)
    elif isinstance(fault, Isolate):
        cluster.isolate(fault.node)
    elif isinstance(fault, CustomFault):
        # 自定义动作面向完整集群；协议对象仅在测试替身场景出现，此时不会构造
        # CustomFault（其回调签名本就是 TestCluster）。
        fault.action(cast("TestCluster", cluster))
    else:  # pragma: no cover - Union 已被静态检查覆盖，保留运行时兜底
        raise FaultError(f"unsupported fault type: {type(fault).__name__}")


def _call_optional(cluster: FaultTarget, method: str, node: str) -> None:
    callback = getattr(cluster, method, None)
    if callback is None:
        raise FaultError(
            f"{type(cluster).__name__} does not support {method}() "
            f"(fault requires a process/container backend)"
        )
    callback(node)


@dataclass(frozen=True)
class PlannedFault:
    """计划中的一条故障。"""

    at_s: float
    order: int
    fault: Fault

    def to_dict(self) -> dict[str, Any]:
        return {"at_s": self.at_s, "order": self.order, **self.fault.to_dict()}


# ----------------------------------------------------------------------
# 脚本与执行
# ----------------------------------------------------------------------


class FaultScript:
    """确定性的故障脚本。

    用法::

        script = FaultScript(seed=7)
        script.add(Partition("a", "c", at_s=0.5))
        script.add(Heal("a", "c", at_s=2.0))
        script.run(cluster).wait(timeout_s=5)
    """

    def __init__(self, seed: int = 0) -> None:
        self.seed = seed
        self._entries: list[PlannedFault] = []
        self._order = 0
        self._rng = random.Random(seed)

    @property
    def rng(self) -> random.Random:
        """脚本专属的确定性随机源（供调用方生成随机故障）。"""
        return self._rng

    def add(self, fault: Fault, *, at_s: float | None = None) -> FaultScript:
        """追加一条故障；``at_s`` 省略时使用故障自带的 ``at_s``。"""
        effective = fault.at_s if at_s is None else at_s
        if effective < 0:
            raise ValueError("fault time must be non-negative")
        self._entries.append(PlannedFault(float(effective), self._order, fault))
        self._order += 1
        return self

    def flap(
        self,
        source: str,
        target: str,
        *,
        at_s: float = 0.0,
        times: int = 3,
        period_s: float = 0.5,
        down_s: float = 0.1,
    ) -> FaultScript:
        """追加 ``times`` 次"断开-恢复"抖动。"""
        if times <= 0:
            raise ValueError("times must be positive")
        for i in range(times):
            start = at_s + i * period_s
            self.add(Partition(source, target, at_s=start), at_s=start)
            self.add(Heal(source, target, at_s=start + down_s), at_s=start + down_s)
        return self

    def plan(self) -> list[PlannedFault]:
        """按 ``(at_s, 插入顺序)`` 返回不可变执行计划。"""
        return sorted(self._entries, key=lambda item: (item.at_s, item.order))

    def fingerprint(self) -> str:
        """计划指纹（seed + 全部故障），用于断言确定性。"""
        payload = json.dumps(
            {"seed": self.seed, "plan": [item.to_dict() for item in self.plan()]},
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def run(
        self,
        cluster: FaultTarget,
        *,
        speed: float = 1.0,
        background: bool = True,
        strict: bool = False,
        require: Iterable[FaultKind] | None = None,
        strict_capabilities: bool = False,
    ) -> FaultRun:
        """执行脚本；``speed>1`` 表示加速（时间除以 speed）。

        ``require`` / ``strict_capabilities`` 会在执行前与后端声明的能力
        协商：不满足时立即抛 :class:`CapabilityError`，避免"看起来注入了
        真实故障，实际只是控制面近似"。
        """
        if speed <= 0:
            raise ValueError("speed must be positive")
        plan = self.plan()
        _require_fault_capabilities(
            cluster,
            plan,
            require=require,
            strict=strict_capabilities,
        )
        run = FaultRun(cluster, plan, speed=speed, strict=strict)
        run.start()
        if not background:
            run.wait()
        return run

    def __len__(self) -> int:
        return len(self._entries)


def _require_fault_capabilities(
    cluster: FaultTarget,
    plan: list[PlannedFault],
    *,
    require: Iterable[FaultKind] | None,
    strict: bool,
) -> None:
    capabilities = getattr(cluster, "capabilities", None)
    if capabilities is None:
        return
    required = set(require or ())
    if strict:
        required.update(
            item.fault.kind
            for item in plan
            if getattr(item.fault, "kind", None) is not None
        )
    required.discard(FaultKind.CUSTOM)
    missing = sorted(kind.value for kind in required if kind not in capabilities.faults)
    if missing:
        raise CapabilityError(
            "backend does not support required faults: "
            + ",".join(missing)
            + f"; supported: {sorted(kind.value for kind in capabilities.faults)}"
        )


class FaultRun:
    """后台执行的故障脚本句柄。"""

    def __init__(
        self,
        cluster: FaultTarget,
        plan: list[PlannedFault],
        *,
        speed: float,
        strict: bool,
    ) -> None:
        self._cluster = cluster
        self._plan = plan
        self._speed = speed
        self._strict = strict
        self._stop = threading.Event()
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="testkit-faults", daemon=True)
        self._applied: list[PlannedFault] = []
        self._errors: list[tuple[PlannedFault, BaseException]] = []
        self._lock = threading.Lock()

    def start(self) -> FaultRun:
        self._thread.start()
        return self

    def _loop(self) -> None:
        started = time.monotonic()
        try:
            for item in self._plan:
                deadline = started + item.at_s / self._speed
                while not self._stop.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._stop.wait(min(remaining, 0.05))
                if self._stop.is_set():
                    break
                try:
                    apply_fault(self._cluster, item.fault)
                except Exception as exc:  # 故障应用失败必须被记录；strict 模式下最终抛出
                    with self._lock:
                        self._errors.append((item, exc))
                    self._record_fault(item, applied=False, error=str(exc))
                    if self._strict:
                        break
                    continue
                with self._lock:
                    self._applied.append(item)
                self._record_fault(item, applied=True)
        finally:
            self._done.set()

    def _record_fault(self, item: PlannedFault, *, applied: bool, error: str = "") -> None:
        report = getattr(self._cluster, "report", None)
        if report is None:
            return
        report.record_fault({"applied": applied, "error": error, **item.to_dict()})

    @property
    def applied(self) -> list[PlannedFault]:
        with self._lock:
            return list(self._applied)

    @property
    def errors(self) -> list[tuple[PlannedFault, BaseException]]:
        with self._lock:
            return list(self._errors)

    def wait(self, timeout_s: float | None = None) -> list[PlannedFault]:
        """等待执行完成；超时或（strict 下）故障应用失败时抛错。"""
        if not self._done.wait(timeout=timeout_s):
            raise FaultError(f"fault script did not finish within {timeout_s}s")
        if self._errors:
            fault, error = self._errors[0]
            if self._strict:
                raise FaultError(f"fault {fault.to_dict()} failed: {error}") from error
        return self.applied

    def stop(self) -> None:
        """请求停止（不等待当前 sleep 以外的动作）。"""
        self._stop.set()

    @property
    def finished(self) -> bool:
        return self._done.is_set()


# ----------------------------------------------------------------------
# 混沌计划生成器
# ----------------------------------------------------------------------


@dataclass
class ChaosPlan:
    """按 seed 生成随机但可复现的故障脚本。

    每个 tick 独立采样：分区、心跳抖动、时钟漂移、节点崩溃。崩溃节点不会
    再收到后续故障；``min_alive`` 保证至少保留若干节点（默认 1）。
    """

    nodes: list[str]
    seed: int = 0
    duration_s: float = 10.0
    tick_s: float = 1.0
    partition_prob: float = 0.15
    heal_after_s: float = 1.0
    kill_prob: float = 0.02
    pause_prob: float = 0.1
    drift_prob: float = 0.05
    max_partitions_per_tick: int = 2
    min_alive: int = 1
    pause_ms_range: tuple[int, int] = (500, 2_000)
    drift_ms_range: tuple[int, int] = (-500, 500)

    def __post_init__(self) -> None:
        if not self.nodes:
            raise ValueError("chaos plan requires at least one node")
        if self.tick_s <= 0 or self.duration_s <= 0:
            raise ValueError("tick_s and duration_s must be positive")
        for name in ("partition_prob", "kill_prob", "pause_prob", "drift_prob"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if len(set(self.nodes)) != len(self.nodes):
            raise ValueError("nodes must be unique")

    def build(self) -> FaultScript:
        """生成确定性脚本。"""
        rng = random.Random(self.seed)
        script = FaultScript(seed=self.seed)
        alive = list(self.nodes)
        ticks = int(self.duration_s / self.tick_s)
        for tick in range(ticks):
            at = tick * self.tick_s
            if len(alive) >= 2 and rng.random() < self.partition_prob:
                for _ in range(rng.randint(1, self.max_partitions_per_tick)):
                    source, target = rng.sample(alive, 2)
                    script.add(Partition(source, target, at_s=at))
                    heal_at = at + self.heal_after_s
                    if heal_at <= self.duration_s:
                        script.add(Heal(source, target, at_s=heal_at))
            if rng.random() < self.pause_prob:
                node = rng.choice(alive)
                lo, hi = self.pause_ms_range
                script.add(PauseHeartbeat(node, rng.randint(lo, hi), at_s=at))
            if rng.random() < self.drift_prob:
                node = rng.choice(alive)
                lo, hi = self.drift_ms_range
                script.add(ClockDrift(node, rng.randint(lo, hi), at_s=at))
            if len(alive) > self.min_alive and rng.random() < self.kill_prob:
                victim = rng.choice(alive)
                alive.remove(victim)
                script.add(Kill(victim, at_s=at))
        return script

    def fingerprint(self) -> str:
        """计划指纹（不含执行结果）。"""
        return self.build().fingerprint()
