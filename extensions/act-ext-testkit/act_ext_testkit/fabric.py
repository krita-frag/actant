"""链路级网络 Fabric：把弱网参数下沉到操作系统/代理层。

``SemanticNetem`` 只能近似控制面；本模块提供三个**真实执行命令**的实现：

- :class:`CommandFabric`：由调用方提供 ``tc``/``iptables``/``toxiproxy`` 等
  命令模板，testkit 负责确定性执行与错误传播；
- :class:`TcNetemFabric`：Linux 下直接生成并执行 ``tc qdisc netem`` 与
  ``iptables`` 命令；
- :class:`DnctlFabric`：macOS 下用 ``dnctl`` + ``pfctl``（dummynet）按节点
  UDP 端口整形与分区，不需要容器或虚拟机。

三者都支持 ``dry_run=True``：只记录命令、不执行，便于在没有 root/容器的
CI 上验证参数生成逻辑。能力不足时 :meth:`supports` 会报告缺失参数，由上层
决定跳过还是失败，绝不静默降级。
"""

from __future__ import annotations

import os
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


class DnctlFabric:
    """macOS ``dummynet``（``dnctl`` + ``pfctl``）实现。

    macOS 没有 ``tc`` 与 network namespace；内核自带的 dummynet 仍可对
    UDP 提供真实的延迟、丢包与限速。本类按节点**固定 UDP 端口**下发规则，
    适用于 ``TestCluster`` 同机多进程拓扑（节点共享回环地址，端口不同）。

    规则写入 ``com.apple/actant-testkit`` anchor。macOS 默认 ``/etc/pf.conf``
    已包含 ``dummynet-anchor "com.apple/*"`` 与 ``anchor "com.apple/*"``，
    因此不需要改写主规则集。

    - ``apply``：配置一个 pipe，并对每个节点端口挂**出方向**规则
      （``out ... from any port N``）。环回流量会同时经过 out 与 in，
      只挂一侧可避免延迟被计算两次。
    - ``partition``：按 ``(source_port, target_port)`` 双向 ``block drop``；
      ``heal`` 后重建 anchor。
    - ``clear``：清空 anchor 并删除本类创建的 pipe；**不关闭 pf**，
      避免影响宿主机其他规则。

    需要 root 权限；本类不自动 ``sudo``，由调用方在 CI/本地显式提权。
    """

    fidelity = NetworkFidelity.KERNEL
    controls_links = True

    def __init__(
        self,
        ports: Mapping[str, int],
        *,
        anchor: str = "com.apple/actant-testkit",
        pipe_id: int = 39000,
        dnctl: str = "dnctl",
        pfctl: str = "pfctl",
        dry_run: bool = False,
        timeout_s: float = 20.0,
    ) -> None:
        if not ports:
            raise FabricError("DnctlFabric requires at least one node port")
        normalized: dict[str, int] = {}
        for name, port in ports.items():
            value = int(port)
            if not 1 <= value <= 65535:
                raise FabricError(f"invalid UDP port {value} for node {name!r}")
            normalized[name] = value
        if len(set(normalized.values())) != len(normalized):
            raise FabricError("DnctlFabric requires a unique UDP port per node")
        self._ports = normalized
        self._anchor = anchor
        self._pipe_id = int(pipe_id)
        self._dnctl = dnctl
        self._pfctl = pfctl
        self._dry_run = dry_run
        self._timeout_s = timeout_s
        self._profile: Any | None = None
        self._partitions: set[tuple[str, str]] = set()
        self.commands: list[list[str]] = []
        self.rules: list[str] = []

    def supports(self, profile: Any) -> set[str]:
        missing: set[str] = set()
        if not self._dry_run:
            if platform.system() != "Darwin":
                missing.add("platform")
            if shutil.which(self._dnctl) is None:
                missing.add("dnctl")
            if shutil.which(self._pfctl) is None:
                missing.add("pfctl")
            is_root = hasattr(os, "geteuid") and os.geteuid() == 0
            if not os.path.exists("/dev/pf") or not is_root:
                missing.add("root")
        if getattr(profile, "jitter_ms", 0):
            missing.add("jitter_ms")
        if getattr(profile, "duplicate", 0):
            missing.add("duplicate")
        if getattr(profile, "reorder", 0):
            missing.add("reorder")
        return missing

    def apply(self, profile: Any, *, duration_s: float | None = None) -> None:
        missing = self.supports(profile)
        if missing:
            raise FabricError("DnctlFabric unavailable: " + ", ".join(sorted(missing)))
        self._run([self._dnctl, "pipe", str(self._pipe_id), "config", *_dnctl_params(profile)])
        self._run([self._pfctl, "-E"])
        self._profile = profile
        self._reload()

    def clear(self) -> None:
        self._profile = None
        self._partitions.clear()
        self._reload()
        self._run(
            [self._dnctl, "pipe", "delete", str(self._pipe_id)],
            ignore_missing=True,
        )

    def partition(self, source: str, target: str) -> None:
        if source == target:
            raise FabricError("cannot partition a node from itself")
        self._port(source)
        self._port(target)
        self._partitions.add((source, target) if source < target else (target, source))
        self._reload()

    def heal(self, source: str, target: str) -> None:
        self._partitions.discard((source, target) if source < target else (target, source))
        self._reload()

    def _port(self, node: str) -> int:
        try:
            return self._ports[node]
        except KeyError:
            raise FabricError(
                f"no UDP port registered for node {node!r}; "
                "DnctlFabric needs the node's advertised listen port"
            ) from None

    def _reload(self) -> None:
        rules = [*self._partition_rules(), *self._shape_rules()]
        self.rules = rules
        self._run(
            [self._pfctl, "-a", self._anchor, "-f", "-"],
            stdin="\n".join(rules) + "\n",
        )

    def _partition_rules(self) -> list[str]:
        rules: list[str] = []
        for source, target in sorted(self._partitions):
            source_port = self._port(source)
            target_port = self._port(target)
            rules.append(
                f"block drop out quick proto udp "
                f"from any port {source_port} to any port {target_port}"
            )
            rules.append(
                f"block drop out quick proto udp "
                f"from any port {target_port} to any port {source_port}"
            )
        return rules

    def _shape_rules(self) -> list[str]:
        if self._profile is None:
            return []
        return [
            f"dummynet out quick proto udp from any port {port} to any pipe {self._pipe_id}"
            for port in sorted(self._ports.values())
        ]

    def _run(
        self,
        command: Sequence[str],
        *,
        stdin: str | None = None,
        ignore_missing: bool = False,
    ) -> None:
        rendered = [str(part) for part in command]
        self.commands.append(rendered)
        if self._dry_run:
            return
        try:
            result = subprocess.run(
                rendered,
                input=stdin,
                capture_output=True,
                text=True,
                timeout=self._timeout_s,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise FabricError(f"command failed to start: {rendered}: {exc}") from exc
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            if ignore_missing and (
                "No such file" in detail or "does not exist" in detail or "not found" in detail
            ):
                return
            raise FabricError(
                f"command exited {result.returncode}: {rendered}: {detail[:500]}"
            )


def _dnctl_params(profile: Any) -> list[str]:
    latency = int(getattr(profile, "latency_ms", 0) or 0)
    loss = float(getattr(profile, "loss", 0.0) or 0.0)
    bandwidth = int(getattr(profile, "bandwidth_kbps", 0) or 0)
    params: list[str] = []
    if bandwidth > 0:
        params += ["bw", f"{bandwidth}Kbit/s"]
    if latency > 0:
        params += ["delay", str(latency)]
    if loss > 0:
        params += ["plr", f"{loss:.4f}"]
    # 空参数表示「不限速、不延迟、不丢包」；bw 0 是 dummynet 的 unlimited。
    return params or ["bw", "0"]


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
