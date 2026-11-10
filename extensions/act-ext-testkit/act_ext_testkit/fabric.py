"""链路级网络 Fabric：把弱网参数下沉到操作系统/代理层。

``SemanticNetem`` 只能近似控制面；本模块提供两个**真实执行命令**的实现：

- :class:`CommandFabric`：由调用方提供 ``tc``/``iptables``/``toxiproxy`` 等
  命令模板，testkit 负责确定性执行与错误传播；
- :class:`TcNetemFabric`：Linux 下直接生成并执行 ``tc qdisc netem`` 与
  ``iptables`` 命令。

两者都支持 ``dry_run=True``：只记录命令、不执行，便于在没有 root/容器的
CI 上验证参数生成逻辑。能力不足时 :meth:`supports` 会报告缺失参数，由上层
决定跳过还是失败，绝不静默降级。
"""

from __future__ import annotations

import platform
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .contract import NetworkFidelity


class FabricError(RuntimeError):
    """网络 Fabric 命令执行失败。"""


class CommandFabric:
    """由调用方提供命令模板的通用 Fabric。

    ``apply_commands(profile)`` 返回要执行的命令列表；``partition_commands``
    与 ``heal_commands`` 同理。命令按顺序执行，任一非零退出即抛
    :class:`FabricError`。
    """

    controls_links = True

    def __init__(
        self,
        *,
        apply_commands: Callable[[Any], Sequence[Sequence[str]]],
        clear_commands: Sequence[Sequence[str]] | Callable[[], Sequence[Sequence[str]]] = (),
        partition_commands: Callable[[str, str], Sequence[Sequence[str]]] | None = None,
        heal_commands: Callable[[str, str], Sequence[Sequence[str]]] | None = None,
        fidelity: NetworkFidelity = NetworkFidelity.KERNEL,
        unsupported: set[str] | None = None,
        dry_run: bool = False,
        timeout_s: float = 20.0,
    ) -> None:
        self.fidelity = fidelity
        self._apply_commands = apply_commands
        self._clear_commands = clear_commands
        self._partition_commands = partition_commands
        self._heal_commands = heal_commands
        self._unsupported = set(unsupported or ())
        self._dry_run = dry_run
        self._timeout_s = timeout_s
        self.commands: list[list[str]] = []

    def supports(self, profile: Any) -> set[str]:
        requested = {
            name
            for name in ("latency_ms", "jitter_ms", "loss", "duplicate", "reorder", "bandwidth_kbps")
            if getattr(profile, name, 0)
        }
        return requested & self._unsupported

    def apply(self, profile: Any, *, duration_s: float | None = None) -> None:
        missing = self.supports(profile)
        if missing:
            raise FabricError(
                "CommandFabric cannot express: " + ", ".join(sorted(missing))
            )
        for command in self._apply_commands(profile):
            self._run(command)

    def clear(self) -> None:
        commands = (
            self._clear_commands()
            if callable(self._clear_commands)
            else self._clear_commands
        )
        for command in commands:
            self._run(command)

    def partition(self, source: str, target: str) -> None:
        if self._partition_commands is None:
            raise FabricError("CommandFabric was not configured for partitioning")
        for command in self._partition_commands(source, target):
            self._run(command)

    def heal(self, source: str, target: str) -> None:
        if self._heal_commands is None:
            raise FabricError("CommandFabric was not configured for healing")
        for command in self._heal_commands(source, target):
            self._run(command)

    def _run(self, command: Sequence[str]) -> None:
        rendered = [str(part) for part in command]
        self.commands.append(rendered)
        if self._dry_run:
            return
        try:
            result = subprocess.run(
                rendered,
                capture_output=True,
                text=True,
                timeout=self._timeout_s,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise FabricError(f"command failed to start: {rendered}: {exc}") from exc
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise FabricError(
                f"command exited {result.returncode}: {rendered}: {detail[:500]}"
            )


class TcNetemFabric:
    """Linux ``tc qdisc netem`` + ``iptables`` 实现。

    ``interfaces`` 把节点名映射到该节点实际使用的网络接口（容器/网络
    命名空间内通常是 ``eth0``）；``peer_ips`` 把节点名映射到可路由 IP，
    用于 ``iptables`` 分区。没有独立网络命名空间时，对共享接口执行
    ``tc`` 会影响所有节点——此时应改用容器后端。
    """

    fidelity = NetworkFidelity.KERNEL
    controls_links = True

    def __init__(
        self,
        interfaces: Mapping[str, str],
        *,
        peer_ips: Mapping[str, str] | None = None,
        tc: str = "tc",
        iptables: str = "iptables",
        dry_run: bool = False,
        timeout_s: float = 20.0,
    ) -> None:
        self._interfaces = dict(interfaces)
        self._peer_ips = dict(peer_ips or {})
        self._tc = tc
        self._iptables = iptables
        self._dry_run = dry_run
        self._timeout_s = timeout_s
        self.commands: list[list[str]] = []

    def supports(self, profile: Any) -> set[str]:
        missing: set[str] = set()
        if platform.system() != "Linux":
            missing.add("platform")
        if shutil.which(self._tc) is None:
            missing.add("tc")
        if self._peer_ips and shutil.which(self._iptables) is None:
            missing.add("iptables")
        if getattr(profile, "bandwidth_kbps", 0) and not self._interfaces:
            missing.add("interfaces")
        return missing

    def apply(self, profile: Any, *, duration_s: float | None = None) -> None:
        missing = self.supports(profile)
        if missing:
            raise FabricError(
                "TcNetemFabric unavailable: " + ", ".join(sorted(missing))
            )
        params = _netem_params(profile)
        for interface in self._interfaces.values():
            self._run([self._tc, "qdisc", "replace", "dev", interface, "root", "netem", *params])

    def clear(self) -> None:
        for interface in self._interfaces.values():
            self._run(
                [self._tc, "qdisc", "del", "dev", interface, "root"],
                ignore_missing=True,
            )

    def partition(self, source: str, target: str) -> None:
        ip = self._ip(target)
        self._run([self._iptables, "-I", "INPUT", "-s", ip, "-j", "DROP"])
        self._run([self._iptables, "-I", "OUTPUT", "-d", ip, "-j", "DROP"])

    def heal(self, source: str, target: str) -> None:
        ip = self._ip(target)
        self._run([self._iptables, "-D", "INPUT", "-s", ip, "-j", "DROP"])
        self._run([self._iptables, "-D", "OUTPUT", "-d", ip, "-j", "DROP"])

    def _ip(self, node: str) -> str:
        try:
            return self._peer_ips[node]
        except KeyError:
            raise FabricError(
                f"no peer IP registered for node {node!r}; "
                "TcNetemFabric.partition needs peer_ips"
            ) from None

    def _run(self, command: Sequence[str], *, ignore_missing: bool = False) -> None:
        rendered = [str(part) for part in command]
        self.commands.append(rendered)
        if self._dry_run:
            return
        try:
            result = subprocess.run(
                rendered,
                capture_output=True,
                text=True,
                timeout=self._timeout_s,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise FabricError(f"command failed to start: {rendered}: {exc}") from exc
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            if ignore_missing and ("No such file" in detail or "Cannot find" in detail):
                return
            raise FabricError(
                f"command exited {result.returncode}: {rendered}: {detail[:500]}"
            )


def _netem_params(profile: Any) -> list[str]:
    latency = int(getattr(profile, "latency_ms", 0) or 0)
    jitter = int(getattr(profile, "jitter_ms", 0) or 0)
    loss = float(getattr(profile, "loss", 0.0) or 0.0)
    duplicate = float(getattr(profile, "duplicate", 0.0) or 0.0)
    reorder = float(getattr(profile, "reorder", 0.0) or 0.0)
    bandwidth = int(getattr(profile, "bandwidth_kbps", 0) or 0)
    if reorder > 0 and latency == 0 and jitter == 0:
        latency = 1
    params: list[str] = []
    if latency or jitter:
        params += ["delay", f"{latency}ms"]
        if jitter:
            params += [f"{jitter}ms", "distribution", "normal"]
    if loss > 0:
        params += ["loss", f"{loss * 100:.2f}%"]
    if duplicate > 0:
        params += ["duplicate", f"{duplicate * 100:.2f}%"]
    if reorder > 0:
        params += ["reorder", f"{reorder * 100:.2f}%", "50%"]
    if bandwidth > 0:
        params += ["rate", f"{bandwidth}kbit"]
    if not params:
        params += ["delay", "0ms"]
    return params
