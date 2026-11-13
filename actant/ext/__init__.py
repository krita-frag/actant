"""Actant 通用扩展契约（纯 Python）。

本命名空间只做类型适配与装配辅助，不新增 Rust 扩展缝，也不做插件自动发现。
具体业务策略应留在第 3 层；这里只提供稳定、通用、可测试的词汇。

用法::

    from actant.ext import PlacementDecision, NodeView, runtime_config

    cfg = runtime_config().role("worker").labels({"pool": "a"}).build()

"""

from __future__ import annotations

from actant.ext.cluster import ClusterChange, ClusterView
from actant.ext.config import RuntimeConfigBuilder, runtime_config
from actant.ext.events import EventAdapter, EventEnvelope, EventSubscription
from actant.ext.lifecycle import Component, Composition, compose
from actant.ext.placement import PlacementPolicy, bind_placement_policy
from actant.ext.types import NodeView, PlacementContext, PlacementDecision

__all__ = [
    "ClusterChange",
    "ClusterView",
    "Component",
    "Composition",
    "EventAdapter",
    "EventEnvelope",
    "EventSubscription",
    "NodeView",
    "PlacementContext",
    "PlacementDecision",
    "PlacementPolicy",
    "RuntimeConfigBuilder",
    "bind_placement_policy",
    "compose",
    "runtime_config",
]
