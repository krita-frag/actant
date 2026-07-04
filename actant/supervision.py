"""Actor 监督：Actant Actor 系统的容错生命周期管理。

提供故障时自动重启和监督树功能。监督者监控子 Actor，
并使用来自 Rust ActorSystem 的基于推送的监督事件，
以指数退避方式重启失败的 Actor，实现零延迟故障检测。

监督事件流：Rust ActorSystem → EventBus → PyEventBridge
→ OrchestrationLoop._on_rust_event → ActorSupervisor._handle_event。
不涉及轮询 —— 事件通过 call_soon_threadsafe 推送。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from actant.actant import _ActorCore

logger = logging.getLogger("actant.supervision")


class RestartPolicy(Enum):
    """决定何时重启被监督的子 Actor。

    Attributes:
        PERMANENT: 无论退出原因如何，始终重启。
        TRANSIENT: 仅在异常退出（错误/恐慌）时重启。
        NEVER: 无论退出原因如何，永不重启。
    """

    PERMANENT = auto()
    TRANSIENT = auto()
    NEVER = auto()


@dataclass
class BackoffConfig:
    """配置指数退避策略。

    Attributes:
        initial_delay: 初始延迟时间（秒）。
        max_delay: 最大延迟时间（秒）。
        multiplier: 每次重试的退避乘数。
        max_retries: 最大重试次数（0 表示无限重试）。
        window: 重试计数窗口（秒）。
    """

    initial_delay: float = 0.2
    max_delay: float = 30.0
    multiplier: float = 2.0
    max_retries: int = 5
    window: float = 10.0


@dataclass
class _ChildEntry:
    """被监督的子 Actor 条目。"""

    actor_id: str
    actor_type: str
    restart_count: int = 0
    restart_timestamps: list[float] = field(default_factory=list)
    last_failure: str | None = None


class ActorSupervisor:
    """监督子 Actor。

    通过 OrchestrationLoop 的事件桥进行通信。不涉及轮询 ——
    事件从 Rust 通过 EventBus 和 PyEventBridge 直接推送到此监督者的回调。

    用法::

        supervisor = ActorSupervisor(actor_core, policy=RestartPolicy.TRANSIENT)
        supervisor.watch(actor_id, "MyActor")
        await supervisor.start()
    """

    def __init__(
        self,
        actor_core: _ActorCore,
        *,
        policy: RestartPolicy = RestartPolicy.TRANSIENT,
        backoff: BackoffConfig | None = None,
    ) -> None:
        self._actor_core = actor_core
        self._policy = policy
        self._backoff = backoff or BackoffConfig()

        self._children: dict[str, _ChildEntry] = {}
        self._running = False
        self._restart_tasks: set[asyncio.Task[None]] = set()

    def watch(self, actor_id: str, actor_type: str) -> None:
        """注册一个子 Actor 以便进行监督。

        Args:
            actor_id: 子 Actor 的唯一标识符。
            actor_type: Actor 的类型名称（例如类名）。
                Rust 层持有用于重启的调度器引用。
        """
        if actor_id in self._children:
            raise ValueError(f"actor {actor_id} is already supervised")
        self._children[actor_id] = _ChildEntry(
            actor_id=actor_id,
            actor_type=actor_type,
        )
        logger.debug("supervisor watching actor %s (type=%s)", actor_id, actor_type)

    def unwatch(self, actor_id: str) -> None:
        """从监督中移除一个子 Actor。"""
        self._children.pop(actor_id, None)
        logger.debug("supervisor stopped watching actor %s", actor_id)

    async def start(self) -> None:
        """启动监督者。"""
        if self._running:
            return
        self._running = True
        logger.info("actor supervisor started (policy=%s)", self._policy.name)

    async def stop(self) -> None:
        """停止监督者。"""
        self._running = False

        # 取消尚未完成的重启任务，避免停止后仍有 actor 被重启。
        for task in list(self._restart_tasks):
            task.cancel()
        if self._restart_tasks:
            with contextlib.suppress(Exception):
                await asyncio.wait(self._restart_tasks, timeout=2.0)
        self._restart_tasks.clear()

        # 终止所有被监督的 actor
        for child_entry in list(self._children.values()):
            with contextlib.suppress(Exception):
                self._actor_core.kill_actor(child_entry.actor_id)

        self._children.clear()
        logger.info("actor supervisor stopped")

    @property
    def child_ids(self) -> list[str]:
        """返回所有被监督的子 Actor 的唯一标识符。"""
        return list(self._children.keys())

    def handle_event(self, event_type: str, actor_id: str, error: str | None) -> None:
        """处理从 Rust ActorSystem 推送的监督事件。

        此方法从 OrchestrationLoop 的事件回调（在 asyncio 事件循环线程上运行）
        同步调用。重启调度通过 asyncio.ensure_future 完成，以避免阻塞事件循环。
        """
        if not self._running:
            return

        entry = self._children.get(actor_id)
        if entry is None:
            return

        if event_type == "ActorStarted":
            logger.debug("actor %s started", actor_id)

        elif event_type == "ActorFailed":
            logger.error(
                "actor %s (type=%s) failed: %s",
                actor_id,
                entry.actor_type,
                error or "unknown",
            )
            entry.last_failure = error

            if self._policy == RestartPolicy.NEVER:
                self.unwatch(actor_id)
                return

            self._spawn_restart(actor_id)

        elif event_type == "ActorStopped":
            if self._policy == RestartPolicy.PERMANENT:
                self._spawn_restart(actor_id)
            else:
                self.unwatch(actor_id)


    def _spawn_restart(self, actor_id: str) -> None:
        """生成重启 Actor 任务。"""
        task = asyncio.create_task(self._restart_actor(actor_id))
        self._restart_tasks.add(task)
        task.add_done_callback(self._restart_tasks.discard)

    async def _restart_actor(self, actor_id: str) -> None:
        """重启 Actor。"""
        entry = self._children.get(actor_id)
        if entry is None:
            return
        if not self._can_restart(entry):
            logger.error(
                "actor %s exceeded restart limit (count=%d, window=%ss), not restarting",
                actor_id,
                entry.restart_count,
                self._backoff.window,
            )
            self.unwatch(actor_id)
            return

        delay = self._compute_delay(entry.restart_count)
        logger.info(
            "restarting actor %s in %.1fs (attempt %d)",
            actor_id,
            delay,
            entry.restart_count + 1,
        )

        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise

        try:
            self._actor_core.restart_actor(actor_id, entry.actor_type)
        except Exception as e:
            logger.exception("failed to restart actor %s: %s", actor_id, e)
            entry.last_failure = str(e)
            return

        entry.restart_count += 1
        entry.restart_timestamps.append(time.time())
        logger.info(
            "actor %s restarted successfully (total restarts=%d)",
            actor_id,
            entry.restart_count,
        )

    def _can_restart(self, entry: _ChildEntry) -> bool:
        """检查是否可以根据重启策略重启 Actor。

        Returns:
            如果可以重启 Actor，则返回 True；如果已达到最大重试次数，则返回 False。
        """
        if self._backoff.max_retries == 0:
            return True
        now = time.time()
        window_start = now - self._backoff.window
        entry.restart_timestamps = [ts for ts in entry.restart_timestamps if ts > window_start]
        return len(entry.restart_timestamps) < self._backoff.max_retries

    def _compute_delay(self, count: int) -> float:
        """计算重启延迟（秒），使用指数退避策略。"""
        delay = self._backoff.initial_delay * (self._backoff.multiplier**count)
        return min(delay, self._backoff.max_delay)
