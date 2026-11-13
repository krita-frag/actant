"""事件归一化与订阅辅助。

Actant 的事件模型保持原样；本模块只提供稳定的外部信封，便于不同扩展
用同一种格式消费事件。事件是通知，不承担 exactly-once；``event_id``
由事件内容确定性生成，可用于消费端幂等。
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any

from actant.capabilities import TaskEvent, WorkflowEvent

#: 当前事件信封的 schema 版本。字段变化时递增；消费端应忽略未知字段。
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EventEnvelope:
    """跨扩展通用的事件信封。"""

    schema_version: int
    event_id: str
    kind: str
    occurred_at: str
    source: str
    payload: Mapping[str, Any]

    @classmethod
    def from_task(cls, event: TaskEvent, *, source: str = "") -> EventEnvelope:
        """把 ``TaskEvent`` 归一化。``kind`` 形如 ``task.completed``。"""
        return _make_envelope("task", asdict(event), source=source)

    @classmethod
    def from_workflow(cls, event: WorkflowEvent, *, source: str = "") -> EventEnvelope:
        """把 ``WorkflowEvent`` 归一化。``kind`` 形如 ``workflow.started``。"""
        return _make_envelope("workflow", asdict(event), source=source)

    def to_dict(self) -> dict[str, Any]:
        """返回 JSON 友好的字典（``payload`` 已做递归转换）。"""
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "kind": self.kind,
            "occurred_at": self.occurred_at,
            "source": self.source,
            "payload": dict(self.payload),
        }


class EventAdapter:
    """把 Actant 事件转换为 :class:`EventEnvelope`。

    ``subscribe(runtime, callback)`` 会同时订阅 ``TaskLifecycle`` 与
    ``WorkflowLifecycle``。与其它 Python handler 一样，订阅只影响 Python
    发起的 effect 与事件回调，不进入 Rust 内部 dispatch。
    """

    def __init__(self, *, source: str = "") -> None:
        self._source = source

    def task(self, event: TaskEvent) -> EventEnvelope:
        """转换任务事件。"""
        return EventEnvelope.from_task(event, source=self._source)

    def workflow(self, event: WorkflowEvent) -> EventEnvelope:
        """转换工作流事件。"""
        return EventEnvelope.from_workflow(event, source=self._source)

    def subscribe(
        self,
        runtime: Any,
        callback: Callable[[EventEnvelope], None],
    ) -> EventSubscription:
        """订阅任务与工作流事件，返回可关闭的订阅句柄。

        在 ``Runtime.start()`` 之后订阅会收到框架告警；如需完整生命周期，
        应在 start 前完成装配。回调异常会向上传播到事件发射方。
        """
        task_layer = runtime.layer("TaskLifecycle")
        workflow_layer = runtime.layer("WorkflowLifecycle")

        def _on_task(event: TaskEvent) -> None:
            callback(self.task(event))

        def _on_workflow(event: WorkflowEvent) -> None:
            callback(self.workflow(event))

        task_layer.chain(_on_task)
        workflow_layer.chain(_on_workflow)
        return EventSubscription((task_layer, workflow_layer), (_on_task, _on_workflow))


class EventSubscription:
    """事件订阅句柄；``close()`` 幂等。"""

    def __init__(
        self,
        layers: tuple[Any, ...],
        handlers: tuple[Callable[[Any], None], ...],
    ) -> None:
        self._layers = layers
        self._handlers = handlers
        self._closed = False

    def close(self) -> None:
        """移除订阅的 handler；重复调用无副作用。"""
        if self._closed:
            return
        for layer, handler in zip(self._layers, self._handlers, strict=True):
            layer.remove(handler)
        self._closed = True


def _make_envelope(category: str, payload: dict[str, Any], *, source: str) -> EventEnvelope:
    safe_payload = _json_safe(payload)
    kind = f"{category}.{safe_payload.get('kind', 'unknown')}"
    canonical = json.dumps(
        {"kind": kind, "payload": safe_payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    event_id = "evt_" + hashlib.blake2b(canonical.encode("utf-8"), digest_size=16).hexdigest()
    return EventEnvelope(
        schema_version=SCHEMA_VERSION,
        event_id=event_id,
        kind=kind,
        occurred_at=datetime.now(timezone.utc).isoformat(),
        source=source,
        payload=MappingProxyType(safe_payload),
    )


def _json_safe(value: Any) -> Any:
    """递归转换为可 JSON 序列化的值；bytes 编码为 base64 标记对象。"""
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)
