"""公共 Runtime 配置构建器。

只覆盖常用字段，避免扩展作者直接依赖下划线配置类；高级字段仍可直接使用
``_ActantConfig``。本模块不新增运行时语义。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from actant.actant import _ActantConfig, _NetworkConfig

_ROLES = frozenset({"both", "orchestrator", "worker"})
_SCHEDULERS = frozenset({"fifo", "priority"})


class RuntimeConfigBuilder:
    """流式构建 ``_ActantConfig``。"""

    def __init__(self) -> None:
        self._payload_signing_key = ""
        self._require_payload_signing = False
        self._role: str | None = None
        self._labels: dict[str, str] | None = None
        self._quotas: dict[str, int] | None = None
        self._network: dict[str, Any] | None = None
        self._max_concurrent_tasks: int | None = None
        self._scheduler: str | None = None

    def signing(self, key: str, *, required: bool = False) -> RuntimeConfigBuilder:
        """设置 payload 签名密钥与是否强制要求签名。"""
        self._payload_signing_key = str(key)
        self._require_payload_signing = bool(required)
        return self

    def role(self, role: str) -> RuntimeConfigBuilder:
        """设置节点角色：``both`` / ``orchestrator`` / ``worker``。"""
        if role not in _ROLES:
            raise ValueError(f"unknown role {role!r}: expected one of {sorted(_ROLES)}")
        self._role = role
        return self

    def labels(self, labels: Mapping[str, str]) -> RuntimeConfigBuilder:
        """设置节点标签（整表覆盖）。"""
        self._labels = {str(key): str(value) for key, value in labels.items()}
        return self

    def quotas(self, quotas: Mapping[str, int]) -> RuntimeConfigBuilder:
        """设置节点命名配额容量（整表覆盖）。"""
        copied = {str(key): int(value) for key, value in quotas.items()}
        if any(value < 0 for value in copied.values()):
            raise ValueError("quota values must be non-negative")
        self._quotas = copied
        return self

    def network(self, **kwargs: Any) -> RuntimeConfigBuilder:
        """设置网络配置；参数与 ``_NetworkConfig`` 构造签名一致。"""
        self._network = dict(kwargs)
        return self

    def worker(self, *, max_concurrent_tasks: int) -> RuntimeConfigBuilder:
        """设置本地最大并发任务数。"""
        if max_concurrent_tasks < 0:
            raise ValueError("max_concurrent_tasks must be non-negative")
        self._max_concurrent_tasks = max_concurrent_tasks
        return self

    def scheduler(self, name: str) -> RuntimeConfigBuilder:
        """设置内置调度器：``priority`` / ``fifo``。"""
        if name not in _SCHEDULERS:
            raise ValueError(f"unknown scheduler {name!r}: expected one of {sorted(_SCHEDULERS)}")
        self._scheduler = name
        return self

    def build(self) -> _ActantConfig:
        """构造 PyO3 配置对象。"""
        if self._require_payload_signing and not self._payload_signing_key:
            raise ValueError("require_payload_signing=True requires a non-empty signing key")
        network = _NetworkConfig(**self._network) if self._network is not None else None
        return _ActantConfig(
            self._payload_signing_key,
            network=network,
            max_concurrent_tasks=self._max_concurrent_tasks,
            scheduler=self._scheduler,
            require_payload_signing=self._require_payload_signing,
            node_labels=self._labels,
            node_quotas=self._quotas,
            role=self._role,
        )


def runtime_config() -> RuntimeConfigBuilder:
    """创建配置构建器。"""
    return RuntimeConfigBuilder()
