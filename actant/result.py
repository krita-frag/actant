"""AsyncResult：异步工作流结果查询与等待。"""

from __future__ import annotations

import asyncio
from typing import Any

from actant._serialization import loads
from actant.actant import _AsyncResultCore
from actant.exceptions import raise_for_state


class WorkflowResult:
    """工作流执行结果包装类。

    提供对工作流结果的类型安全访问，包含状态和元数据。
    """

    __slots__ = ("_state", "_value", "_workflow_id")

    def __init__(self, value: Any, state: str, workflow_id: str) -> None:
        self._value = value
        self._state = state
        self._workflow_id = workflow_id

    @property
    def value(self) -> Any:
        """工作流结果值。单结果直接返回，多结果返回列表。"""
        return self._value

    @property
    def state(self) -> str:
        """工作流最终状态（如 "Completed"）。"""
        return self._state

    @property
    def workflow_id(self) -> str:
        """工作流 ID。"""
        return self._workflow_id

    @property
    def is_success(self) -> bool:
        """工作流是否成功完成。"""
        return self._state == "Completed"

    def __repr__(self) -> str:
        return f"<WorkflowResult id={self._workflow_id} state={self._state}>"

    def __bool__(self) -> bool:
        return self.is_success


class AsyncResult:
    """包装 Rust 核心 AsyncResult，提供 Pythonic 异步 API。

    职责：
    - 查询 workflow 完成状态（同步，瞬间返回）
    - 异步等待结果返回
    - 同步等待结果返回（get_sync）
    - 反序列化结果为 Python 对象
    - 根据 state 抛出对应异常
    """

    def __init__(self, core: _AsyncResultCore) -> None:
        self._core = core

    @property
    def workflow_id(self) -> str:
        return self._core.workflow_id

    def ready(self) -> bool:
        return self._core.ready()

    def state(self) -> str:
        return self._core.state()

    async def get(self, timeout: float | None = None) -> WorkflowResult:
        """异步等待并返回工作流结果。

        Args:
            timeout: 最大等待时间（秒）。None 表示无限等待。

        Returns:
            WorkflowResult: 包含结果值、状态和工作流 ID 的包装对象。
        """
        timeout_ms: int | None = int(timeout * 1000) if timeout is not None else None
        raw: Any = await self._core.get(timeout_ms)

        if not isinstance(raw, dict):
            return WorkflowResult(value=raw, state="Completed", workflow_id=self.workflow_id)

        state: str = raw.get("state") or "Completed"

        if state != "Completed":
            raise_for_state(
                state,
                raw.get("error", f"workflow {state.lower()}"),
                failed_tasks=raw.get("failed_tasks"),
            )

        results: list[tuple[str, bytes]] = raw.get("results") or []

        if not results:
            value = None
        elif len(results) == 1:
            _, result_bytes = results[0]
            value = loads(result_bytes)
        else:
            value = [loads(rb) for _, rb in results]

        return WorkflowResult(value=value, state=state, workflow_id=self.workflow_id)

    def get_sync(self, timeout: float | None = None) -> WorkflowResult:
        """同步等待并返回工作流结果。

        非 async 上下文下的便捷方法。若在 async 上下文中，
        请使用 ``await result.get()`` 代替。

        自动检测当前事件循环：
        - 无事件循环时：使用 asyncio.run()
        - 有事件循环且正在运行时：在新线程中运行，避免冲突
        - 有事件循环但未运行时：使用 loop.run_until_complete()

        Args:
            timeout: 最大等待时间（秒）。None 表示无限等待。

        Returns:
            WorkflowResult: 包含结果值、状态和工作流 ID 的包装对象。
        """
        coro = self.get(timeout)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None:
            return asyncio.run(coro)

        if loop.is_running():
            # 当前线程已存在运行中的事件循环 —— 在独立线程的新事件循环中
            # 执行 coro，复用模块级线程池避免每次调用都创建/销毁线程池。
            from actant.task import _ASYNC_EXECUTOR

            future = _ASYNC_EXECUTOR.submit(asyncio.run, coro)
            return future.result()

        return loop.run_until_complete(coro)

    def __repr__(self) -> str:
        return f"<AsyncResult {self.workflow_id}>"
