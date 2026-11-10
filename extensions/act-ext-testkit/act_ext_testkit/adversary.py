"""对抗行为：信任域、成员限制与拒绝断言。

P2P 系统的对抗测试关注"不该发生的事情没有发生"：密钥不匹配的节点不应
被接受、非成员不应进入 gossip、未知 peer 的请求不应被处理。本模块提供
构造这类场景的配置助手与**显式拒绝断言**（拒绝必须可观测，而不是超时后
默默当作成功）。

注意：签名密钥不入报告。``TrustDomain.describe()`` 只输出指纹。
"""

from __future__ import annotations

import hashlib
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from actant.actant import _ActantConfig

    from .cluster import TestCluster


class AdversaryError(RuntimeError):
    """对抗场景构造错误。"""


def generate_signing_key() -> str:
    """生成随机签名密钥（32 字节 hex）。"""
    return secrets.token_hex(32)


def key_fingerprint(key: str) -> str:
    """密钥指纹（用于报告/断言，不泄露密钥本身）。"""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class TrustDomain:
    """一个节点所属的信任域配置。

    Args:
        signing_key: payload 签名密钥。
        allowed_peer_ids: gossip 成员白名单；非空时仅这些 peer 可加入。
        require_payload_signing: 是否强制校验 payload 签名。
        require_signed_records: 是否强制校验心跳记录签名。
        discovery: 网络发现 preset。
        extra_network: 额外网络配置字段。
    """

    signing_key: str
    allowed_peer_ids: tuple[str, ...] = ()
    require_payload_signing: bool = True
    require_signed_records: bool = True
    discovery: str = "local"
    extra_network: dict[str, Any] = field(default_factory=dict)

    def build_config(
        self,
        *,
        data_dir: str,
        labels: dict[str, str] | None = None,
        max_concurrent_tasks: int = 1,
        heartbeat_interval_ms: int = 500,
        failure_timeout_ms: int = 3_000,
    ) -> _ActantConfig:
        """构造该信任域的 ``_ActantConfig``。"""
        from actant.actant import (
            _ActantConfig,
            _FailoverConfig,
            _NetworkConfig,
            _StoreConfig,
        )

        if self.require_payload_signing and not self.signing_key:
            raise AdversaryError("require_payload_signing=True requires a non-empty signing_key")
        network_kwargs: dict[str, Any] = dict(self.extra_network)
        network_kwargs.setdefault("preset", self.discovery)
        if self.allowed_peer_ids:
            network_kwargs["allowed_peer_ids"] = list(self.allowed_peer_ids)
        if self.require_signed_records:
            network_kwargs["require_signed_records"] = True
        return _ActantConfig(
            payload_signing_key=self.signing_key,
            require_payload_signing=self.require_payload_signing,
            network=_NetworkConfig(**network_kwargs),
            failover=_FailoverConfig(
                heartbeat_interval_ms=heartbeat_interval_ms,
                failure_timeout_ms=failure_timeout_ms,
            ),
            node_labels=dict(labels or {}),
            max_concurrent_tasks=max_concurrent_tasks,
            store=_StoreConfig(data_dir=data_dir),
        )

    def describe(self) -> dict[str, Any]:
        return {
            "signing_key_fingerprint": key_fingerprint(self.signing_key),
            "allowed_peer_ids": list(self.allowed_peer_ids),
            "require_payload_signing": self.require_payload_signing,
            "require_signed_records": self.require_signed_records,
            "discovery": self.discovery,
        }


def mismatched_domains() -> tuple[TrustDomain, TrustDomain]:
    """构造两个密钥不匹配的信任域（用于拒绝测试）。"""
    return (
        TrustDomain(generate_signing_key()),
        TrustDomain(generate_signing_key()),
    )


def restricted_domain(
    allowed_peer_ids: list[str],
    *,
    signing_key: str | None = None,
) -> TrustDomain:
    """构造仅允许指定 peer 的信任域。"""
    if not allowed_peer_ids:
        raise AdversaryError("restricted domain requires at least one allowed peer id")
    return TrustDomain(
        signing_key=signing_key or generate_signing_key(),
        allowed_peer_ids=tuple(allowed_peer_ids),
    )


def assert_rejected(
    action: Callable[[], Any],
    *,
    description: str = "operation",
    timeout_s: float = 5.0,
) -> BaseException:
    """断言 ``action`` 在超时内抛出异常。

    返回捕获的异常供进一步检查（如异常类型/kind）。若正常返回，则断言失败。
    """
    started = time.monotonic()
    try:
        action()
    except Exception as exc:
        return exc
    elapsed = time.monotonic() - started
    raise AssertionError(
        f"{description} was expected to be rejected but returned normally "
        f"after {elapsed:.3f}s (timeout budget {timeout_s}s)"
    )


def assert_peer_absent(
    cluster: TestCluster,
    observer: str,
    peer: str,
    *,
    timeout_s: float = 10.0,
) -> None:
    """断言 ``observer`` 始终看不到 ``peer``（持续观察到超时）。"""
    peer_id = cluster.node_ids()[peer]
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        visible = any(p["node_id"] == peer_id for p in cluster.handle(observer).peers())
        if visible:
            raise AssertionError(f"{observer} unexpectedly sees rejected peer {peer}")
        time.sleep(0.1)


def assert_peer_visible(
    cluster: TestCluster,
    observer: str,
    peer: str,
    *,
    timeout_s: float = 10.0,
) -> None:
    """断言 ``observer`` 最终能看到 ``peer``。"""
    cluster.wait_peer_visible(observer, peer, timeout_s=timeout_s)
