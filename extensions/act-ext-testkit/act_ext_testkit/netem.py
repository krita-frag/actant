"""弱网模拟：能力边界明确的**控制面语义近似**。

Actant 没有暴露 TCP 层整形接口，因此本模块不假装能设置内核 ``tc netem``。
它把网络参数映射到集群已有的故障原语：

============  ==========================================================
参数           语义映射
============  ==========================================================
latency/jitter 周期性 ``pause_heartbeat``（每个 tick 暂停心跳）
loss           按概率把有向链路 ``partition`` 一小段时间后 ``heal``
reorder        与 loss 合并为"乱序即重传/重连"的近似（文档化为近似）
duplicate      **不支持**（无法在控制面复制消息）→ 明确抛错
bandwidth      **不支持**（无法限速）→ 明确抛错
============  ==========================================================

不支持的能力绝不静默降级：构造/校验阶段直接抛 ``NetemUnsupported``。
需要真实内核级弱网时，在容器/虚拟机层使用 ``tc``/``toxiproxy``，再以
``NetworkProfile`` 仅作参数记录。
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from .contract import NetworkFidelity
from .faults import FaultRun, FaultScript, Heal, Partition, PauseHeartbeat

if TYPE_CHECKING:
    from .cluster import TestCluster


class NetemError(RuntimeError):
    """弱网模拟参数或生命周期错误。"""


class NetemUnsupported(NetemError):
    """请求的网络参数无法在控制面语义层表达。"""


@dataclass(frozen=True)
class NetworkProfile:
    """弱网参数（单位：毫秒 / 比例 / kbps）。

    ``loss``/``duplicate``/``reorder`` 取值 ``[0, 1]``。
    """

    latency_ms: int = 0
    jitter_ms: int = 0
    loss: float = 0.0
    duplicate: float = 0.0
    reorder: float = 0.0
    bandwidth_kbps: int = 0

    def __post_init__(self) -> None:
        if self.latency_ms < 0 or self.jitter_ms < 0:
            raise ValueError("latency_ms and jitter_ms must be non-negative")
        for name in ("loss", "duplicate", "reorder"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.bandwidth_kbps < 0:
            raise ValueError("bandwidth_kbps must be non-negative")

    def ensure_semantic(self) -> None:
        """校验是否可由控制面语义近似表达。"""
        unsupported: list[str] = []
        if self.duplicate > 0:
            unsupported.append("duplicate")
        if self.bandwidth_kbps > 0:
            unsupported.append("bandwidth_kbps")
        if unsupported:
            raise NetemUnsupported(
                "cannot emulate "
                + ", ".join(unsupported)
                + " at the control plane; use OS/container-level shaping "
                "(tc netem / toxiproxy) and keep this profile as metadata"
            )

    @property
    def effective_drop(self) -> float:
        """loss + reorder 的近似丢包率（上限 1.0）。"""
        return min(1.0, self.loss + self.reorder)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def describe(self) -> str:
        return (
            f"latency={self.latency_ms}ms jitter={self.jitter_ms}ms "
            f"loss={self.loss:.2f} reorder={self.reorder:.2f}"
        )


@dataclass(frozen=True)
class NetemAction:
    """一条确定性弱网动作。"""

    at_s: float
    kind: str
    source: str = ""
    target: str = ""
    node: str = ""
    ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_schedule(
    profile: NetworkProfile,
    nodes: list[str],
    *,
    duration_s: float,
    tick_s: float = 0.5,
    seed: int = 0,
    down_s: float = 0.1,
) -> list[NetemAction]:
    """生成确定性的弱网动作序列（不执行）。

    相同 ``(profile, nodes, duration_s, tick_s, seed)`` → 相同序列与指纹。
    """
    profile.ensure_semantic()
    if not nodes:
        raise ValueError("nodes must be non-empty")
    if tick_s <= 0 or duration_s <= 0:
        raise ValueError("tick_s and duration_s must be positive")
    if down_s <= 0:
        raise ValueError("down_s must be positive")
    rng = random.Random(seed)
    ordered = sorted(set(nodes))
    actions: list[NetemAction] = []
    ticks = int(duration_s / tick_s)
    drop = profile.effective_drop
    for tick in range(ticks):
        at = tick * tick_s
        if profile.latency_ms or profile.jitter_ms:
            for node in ordered:
                delay = profile.latency_ms
                if profile.jitter_ms:
                    delay += rng.randint(0, profile.jitter_ms)
                actions.append(NetemAction(at, "heartbeat_delay", node=node, ms=delay))
        if drop > 0:
            for source in ordered:
                for target in ordered:
                    if source == target:
                        continue
                    if rng.random() < drop:
                        actions.append(NetemAction(at, "partition", source=source, target=target))
                        heal_at = at + down_s
                        if heal_at <= duration_s:
                            actions.append(
                                NetemAction(heal_at, "heal", source=source, target=target)
                            )
    return sorted(actions, key=lambda a: (a.at_s, a.kind, a.source, a.target, a.node))


def schedule_fingerprint(actions: list[NetemAction]) -> str:
    """动作序列指纹。"""
    payload = json.dumps([a.to_dict() for a in actions], sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class SemanticNetem:
    """控制面弱网模拟器。

    用法::

        netem = SemanticNetem(cluster, NetworkProfile(latency_ms=300, loss=0.1), seed=7)
        netem.start(duration_s=5).wait(timeout_s=10)

    同时实现 :class:`act_ext_testkit.contract.NetworkFabric`：``apply`` 在
    后台启动模拟，``clear`` 恢复心跳。
    """

    fidelity = NetworkFidelity.SEMANTIC
    controls_links = False

    def __init__(
        self,
        cluster: TestCluster,
        profile: NetworkProfile,
        *,
        nodes: list[str] | None = None,
        seed: int = 0,
        tick_s: float = 0.5,
        down_s: float = 0.1,
    ) -> None:
        profile.ensure_semantic()
        self._cluster = cluster
        self.profile = profile
        self._nodes = list(nodes if nodes is not None else _cluster_node_names(cluster))
        self.seed = seed
        self.tick_s = tick_s
        self.down_s = down_s
        self._active: NetemRun | None = None

    @property
    def nodes(self) -> list[str]:
        return list(self._nodes)

    def supports(self, profile: NetworkProfile) -> set[str]:
        """返回控制面语义层无法表达的参数名。"""
        missing: set[str] = set()
        if profile.duplicate > 0:
            missing.add("duplicate")
        if profile.bandwidth_kbps > 0:
            missing.add("bandwidth_kbps")
        return missing

    def apply(self, profile: NetworkProfile, *, duration_s: float | None = None) -> None:
        if duration_s is None:
            raise NetemError(
                "SemanticNetem.apply requires duration_s; control-plane shaping "
                "is schedule-based, not a persistent kernel qdisc"
            )
        self.profile = profile
        self._active = self.start(duration_s=duration_s, background=True)

    def clear(self) -> None:
        if self._active is not None:
            self._active.stop()
            self._active = None

    def partition(self, source: str, target: str) -> None:
        self._cluster.partition(source, target)

    def heal(self, source: str, target: str) -> None:
        self._cluster.heal(source, target)

    def schedule(self, duration_s: float) -> list[NetemAction]:
        return build_schedule(
            self.profile,
            self._nodes,
            duration_s=duration_s,
            tick_s=self.tick_s,
            seed=self.seed,
            down_s=self.down_s,
        )

    def fingerprint(self, duration_s: float) -> str:
        return schedule_fingerprint(self.schedule(duration_s))

    def to_script(self, duration_s: float) -> FaultScript:
        """把动作序列翻译为 ``FaultScript``（可审计、可复用）。"""
        script = FaultScript(seed=self.seed)
        for action in self.schedule(duration_s):
            if action.kind == "heartbeat_delay":
                script.add(PauseHeartbeat(action.node, action.ms, at_s=action.at_s))
            elif action.kind == "partition":
                script.add(Partition(action.source, action.target, at_s=action.at_s))
            elif action.kind == "heal":
                script.add(Heal(action.source, action.target, at_s=action.at_s))
            else:  # pragma: no cover - build_schedule 只产生上述三种
                raise NetemError(f"unknown netem action: {action.kind!r}")
        return script

    def start(self, *, duration_s: float, background: bool = True) -> NetemRun:
        """开始弱网模拟；结束后自动恢复所有节点心跳。"""
        script = self.to_script(duration_s)
        run = script.run(self._cluster, background=True)
        handle = NetemRun(run, self._cluster, list(self._nodes))
        if not background:
            handle.wait(timeout_s=duration_s + 10.0)
        return handle


class NetemRun:
    """弱网模拟句柄（组合 ``FaultRun`` 并负责收尾恢复）。"""

    def __init__(self, run: FaultRun, cluster: TestCluster, nodes: list[str]) -> None:
        self._run = run
        self._cluster = cluster
        self._nodes = nodes
        self._restored = False

    @property
    def applied(self) -> list[Any]:
        return self._run.applied

    @property
    def errors(self) -> list[Any]:
        return self._run.errors

    @property
    def finished(self) -> bool:
        return self._run.finished

    def wait(self, *, timeout_s: float | None = None) -> None:
        try:
            self._run.wait(timeout_s=timeout_s)
        finally:
            self.restore()

    def stop(self) -> None:
        self._run.stop()
        self.restore()

    def restore(self) -> None:
        """恢复所有节点的正常心跳（幂等）。"""
        if self._restored:
            return
        self._restored = True
        errors: list[BaseException] = []
        for node in self._nodes:
            try:
                self._cluster.pause_heartbeat(node, 0)
            except Exception as exc:  # 节点可能已被 kill；记录后继续恢复其余节点
                errors.append(exc)
        if errors:
            raise NetemError(f"failed to restore heartbeat on {len(errors)} node(s): {errors[0]}")


def shaping_supported() -> tuple[bool, str]:
    """当前环境是否支持 OS 级整形（Linux ``tc`` 或 macOS dummynet）。"""
    import os
    import platform
    import shutil

    system = platform.system()
    if system == "Darwin":
        if shutil.which("dnctl") is None or shutil.which("pfctl") is None:
            return (False, "macOS dummynet needs 'dnctl' and 'pfctl' on PATH")
        if not os.path.exists("/dev/pf") or os.geteuid() != 0:
            return (False, "macOS dummynet requires root; see DnctlFabric")
        return (True, "macOS dummynet available; see DnctlFabric for port wiring")
    if system != "Linux":
        return (
            False,
            "OS-level traffic shaping requires Linux 'tc' or macOS dummynet; "
            "use CommandFabric or SemanticNetem",
        )
    if shutil.which("tc") is None:
        return (
            False,
            "the 'tc' binary is not on PATH; install iproute2 or use SemanticNetem",
        )
    return (True, "tc netem available; see TcNetemFabric for interface wiring")


def _cluster_node_names(cluster: TestCluster) -> list[str]:
    names = getattr(cluster, "node_names", None)
    if callable(names):
        return list(names())
    return list(getattr(cluster, "names", []))
