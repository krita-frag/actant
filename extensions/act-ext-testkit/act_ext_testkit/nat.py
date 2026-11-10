"""NAT / 中继场景的网络配置构造。

真实 NAT 穿透测试需要外部网络环境（relay、双 NAT 主机、端口映射）。本模块
只负责**构造正确的网络配置**与**显式的可用性门禁**，绝不在不可用环境里
静默跳过或静默通过：

- ``NatMode.DIRECT``：局域网直连；
- ``NatMode.RELAY_ONLY``：强制经 relay（需显式 relay 端点）；
- ``NatMode.HOLE_PUNCH``：使用 relay 做打洞协调。

``ensure_available()`` 在未设置 ``ACTANT_TEST_NAT=1`` 时抛
``NatUnavailable``；pytest 插件会把它转换为 skip。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from actant.actant import _NetworkConfig


class NatError(RuntimeError):
    """NAT 场景配置错误。"""


class NatUnavailable(NatError):
    """当前环境不具备执行真实 NAT 测试的条件。"""


class NatMode(str, Enum):
    """NAT 场景模式。"""

    DIRECT = "direct"
    RELAY_ONLY = "relay_only"
    HOLE_PUNCH = "hole_punch"


#: 启用外部 NAT 测试的环境变量（值为 ``1``/``true``/``yes``）。
NAT_ENV = "ACTANT_TEST_NAT"


def nat_enabled() -> bool:
    """外部 NAT 环境是否显式启用。"""
    return os.environ.get(NAT_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class NatTopology:
    """NAT 场景描述。

    Args:
        mode: 场景模式。
        relay_endpoints: 自定义 relay URL 列表；``RELAY_ONLY`` 必填。
        preset: Actant 发现 preset（默认 ``"local"``）。
        network_overrides: 额外传给 ``_NetworkConfig`` 的字段。
    """

    mode: NatMode = NatMode.DIRECT
    relay_endpoints: tuple[str, ...] = ()
    preset: str = "local"
    network_overrides: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode is NatMode.RELAY_ONLY and not self.relay_endpoints:
            raise NatError("RELAY_ONLY requires at least one relay endpoint")
        if self.mode is not NatMode.DIRECT and not self.relay_endpoints:
            raise NatError(f"{self.mode.value} requires relay_endpoints")

    @property
    def requires_external_network(self) -> bool:
        return self.mode is not NatMode.DIRECT

    def ensure_available(self) -> None:
        """外部场景门禁：未启用时抛 ``NatUnavailable``。"""
        if self.requires_external_network and not nat_enabled():
            raise NatUnavailable(
                f"{self.mode.value} requires a real NAT/relay environment; "
                f"set {NAT_ENV}=1 to enable"
            )

    def network_config(self, **overrides: Any) -> _NetworkConfig:
        """构造 ``_NetworkConfig``（外部场景先做可用性门禁）。"""
        from actant.actant import _NetworkConfig

        self.ensure_available()
        kwargs: dict[str, Any] = dict(self.network_overrides)
        kwargs.update(overrides)
        kwargs["preset"] = kwargs.get("preset", self.preset)
        if self.relay_endpoints:
            kwargs["relay_endpoints"] = list(self.relay_endpoints)
        return _NetworkConfig(**kwargs)

    def describe(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "preset": self.preset,
            "relay_endpoints": list(self.relay_endpoints),
            "requires_external_network": self.requires_external_network,
            "network_overrides": dict(self.network_overrides),
        }


def relay_only(relay_endpoints: list[str], **kwargs: Any) -> NatTopology:
    """``RELAY_ONLY`` 快捷构造。"""
    return NatTopology(NatMode.RELAY_ONLY, tuple(relay_endpoints), **kwargs)


def hole_punch(relay_endpoints: list[str], **kwargs: Any) -> NatTopology:
    """``HOLE_PUNCH`` 快捷构造。"""
    return NatTopology(NatMode.HOLE_PUNCH, tuple(relay_endpoints), **kwargs)


def direct(**kwargs: Any) -> NatTopology:
    """局域网直连快捷构造（无需外部环境）。"""
    return NatTopology(NatMode.DIRECT, **kwargs)
