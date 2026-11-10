"""被测应用后端注册表。

后端把"如何拉起一个节点、如何解码事件、支持哪些故障"与编排逻辑分离。
内置 ``actant`` 后端；其他项目可通过 ``act_ext_testkit.backends`` 入口点
（entry point）注册自己的后端，无需修改 testkit 源码。
"""

from __future__ import annotations

import importlib
import importlib.metadata
from collections.abc import Callable
from typing import Any, cast

from ..contract import Backend, BackendError

_ENTRY_POINT_GROUP = "act_ext_testkit.backends"

_FACTORIES: dict[str, Callable[[], Backend]] = {}
_INSTANCES: dict[str, Backend] = {}


def register_backend(
    name: str,
    factory: Callable[[], Backend] | Backend,
) -> None:
    """注册一个后端工厂/实例（同名覆盖，后注册优先）。"""
    if not name:
        raise ValueError("backend name must be non-empty")
    if hasattr(factory, "launch") and hasattr(factory, "capabilities"):
        _INSTANCES[name] = factory  # type: ignore[assignment]
        _FACTORIES.pop(name, None)
        return
    _FACTORIES[name] = factory
    _INSTANCES.pop(name, None)


def get_backend(name: str = "actant") -> Backend:
    """按名称取后端实例（懒加载 + 缓存）。"""
    if name in _INSTANCES:
        return _INSTANCES[name]
    factory = _FACTORIES.get(name)
    if factory is None:
        factory = _load_factory(name)
    if factory is None:
        raise BackendError(
            f"unknown testkit backend: {name!r}; available: {sorted(list_backends())}"
        )
    backend = factory()
    _INSTANCES[name] = backend
    return backend


def list_backends() -> list[str]:
    """已注册 + 通过入口点声明的后端名称。"""
    names = set(_FACTORIES) | set(_INSTANCES) | {"actant"}
    names.update(_entry_point_factories())
    return sorted(names)


def reset_backends() -> None:
    """清空缓存（测试隔离用）。"""
    _FACTORIES.clear()
    _INSTANCES.clear()


def _load_factory(name: str) -> Callable[[], Backend] | None:
    if name == "actant":
        return _actant_factory
    return _entry_point_factories().get(name)


def _actant_factory() -> Backend:
    module = importlib.import_module(".actant", __package__)
    backend_cls: Any = module.ActantBackend
    return cast(Backend, backend_cls())


def _entry_point_factories() -> dict[str, Callable[[], Backend]]:
    result: dict[str, Callable[[], Backend]] = {}
    try:
        entry_points = importlib.metadata.entry_points()
    except Exception as exc:  # 元数据损坏时不影响内置后端
        raise BackendError(f"failed to read testkit entry points: {exc}") from exc
    selected = (
        entry_points.select(group=_ENTRY_POINT_GROUP)
        if hasattr(entry_points, "select")
        else cast(Any, entry_points).get(_ENTRY_POINT_GROUP, [])
    )
    for entry_point in selected:
        result[entry_point.name] = _entry_point_loader(entry_point)
    return result


def _entry_point_loader(entry_point: Any) -> Callable[[], Backend]:
    def load() -> Backend:
        loaded = entry_point.load()
        return cast(Backend, loaded() if callable(loaded) else loaded)

    return load
