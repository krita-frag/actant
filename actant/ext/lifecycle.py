"""扩展组件装配辅助。

这是显式装配，不是插件系统：没有自动发现、没有全局注册表。调用方决定
组件顺序，``compose`` 负责启动与逆序停止。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

_logger = logging.getLogger("actant.ext")


class Component(Protocol):
    """扩展组件生命周期协议。"""

    def start(self, runtime: Any) -> None:
        """在给定 Runtime 上装配/启动组件。"""
        ...

    def stop(self) -> None:
        """停止组件；应可重复调用。"""
        ...


@dataclass
class Composition:
    """``compose`` 的返回句柄。"""

    _started: list[Component] = field(default_factory=list)
    _stopped: bool = False

    def stop(self) -> None:
        """逆序停止已启动组件；幂等。"""
        if self._stopped:
            return
        first_error: Exception | None = None
        for component in reversed(self._started):
            try:
                component.stop()
            except Exception as exc:
                _logger.exception("component %s failed to stop", type(component).__name__)
                if first_error is None:
                    first_error = exc
        self._started.clear()
        self._stopped = True
        if first_error is not None:
            raise first_error


def compose(runtime: Any, *components: Component) -> Composition:
    """按顺序启动组件；任一步失败时逆序停止已启动组件并重抛原异常。"""
    composition = Composition()
    try:
        for component in components:
            component.start(runtime)
            composition._started.append(component)
    except Exception:
        for component in reversed(composition._started):
            try:
                component.stop()
            except Exception:
                _logger.exception(
                    "component %s failed to stop during rollback",
                    type(component).__name__,
                )
        composition._started.clear()
        composition._stopped = True
        raise
    return composition
