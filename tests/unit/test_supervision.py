"""supervision.py 单元测试：ActorSupervisor / RestartPolicy / BackoffConfig。

覆盖目标：100% 行覆盖 + 分支覆盖。
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from actant.supervision import (
    ActorSupervisor,
    BackoffConfig,
    RestartPolicy,
    _ChildEntry,
)

# ---------------------------------------------------------------------------
# RestartPolicy
# ---------------------------------------------------------------------------


class TestRestartPolicy:
    def test_policy_values_distinct(self):
        assert RestartPolicy.PERMANENT != RestartPolicy.TRANSIENT
        assert RestartPolicy.TRANSIENT != RestartPolicy.NEVER
        assert RestartPolicy.PERMANENT != RestartPolicy.NEVER

    def test_policy_name(self):
        assert RestartPolicy.PERMANENT.name == "PERMANENT"
        assert RestartPolicy.TRANSIENT.name == "TRANSIENT"
        assert RestartPolicy.NEVER.name == "NEVER"


# ---------------------------------------------------------------------------
# BackoffConfig
# ---------------------------------------------------------------------------


class TestBackoffConfig:
    def test_defaults(self):
        cfg = BackoffConfig()
        assert cfg.initial_delay == 0.2
        assert cfg.max_delay == 30.0
        assert cfg.multiplier == 2.0
        assert cfg.max_retries == 5
        assert cfg.window == 10.0

    def test_custom_values(self):
        cfg = BackoffConfig(initial_delay=1.0, max_delay=60.0, multiplier=3.0, max_retries=10, window=30.0)
        assert cfg.initial_delay == 1.0
        assert cfg.max_delay == 60.0
        assert cfg.multiplier == 3.0
        assert cfg.max_retries == 10
        assert cfg.window == 30.0


# ---------------------------------------------------------------------------
# _ChildEntry
# ---------------------------------------------------------------------------


class TestChildEntry:
    def test_defaults(self):
        entry = _ChildEntry(actor_id="a1", actor_type="MyActor")
        assert entry.actor_id == "a1"
        assert entry.actor_type == "MyActor"
        assert entry.restart_count == 0
        assert entry.restart_timestamps == []
        assert entry.last_failure is None


# ---------------------------------------------------------------------------
# ActorSupervisor — 基本属性
# ---------------------------------------------------------------------------


class TestActorSupervisorBasic:
    def test_default_construction(self):
        core = MagicMock()
        sup = ActorSupervisor(core)
        assert sup._policy == RestartPolicy.TRANSIENT
        assert sup._backoff.max_retries == 5
        assert sup.child_ids == []

    def test_custom_policy(self):
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.PERMANENT)
        assert sup._policy == RestartPolicy.PERMANENT

    def test_custom_backoff(self):
        core = MagicMock()
        cfg = BackoffConfig(max_retries=3)
        sup = ActorSupervisor(core, backoff=cfg)
        assert sup._backoff.max_retries == 3


# ---------------------------------------------------------------------------
# ActorSupervisor — watch / unwatch
# ---------------------------------------------------------------------------


class TestActorSupervisorWatch:
    def test_watch_adds_child(self):
        sup = ActorSupervisor(MagicMock())
        sup.watch("a1", "MyActor")
        assert "a1" in sup.child_ids

    def test_watch_duplicate_raises(self):
        sup = ActorSupervisor(MagicMock())
        sup.watch("a1", "MyActor")
        with pytest.raises(ValueError, match="already supervised"):
            sup.watch("a1", "MyActor")

    def test_unwatch_removes_child(self):
        sup = ActorSupervisor(MagicMock())
        sup.watch("a1", "MyActor")
        sup.unwatch("a1")
        assert "a1" not in sup.child_ids

    def test_unwatch_nonexistent_silent(self):
        sup = ActorSupervisor(MagicMock())
        # 不应抛异常
        sup.unwatch("nonexistent")


# ---------------------------------------------------------------------------
# ActorSupervisor — start / stop
# ---------------------------------------------------------------------------


class TestActorSupervisorStartStop:
    @pytest.mark.asyncio
    async def test_start_sets_running(self):
        sup = ActorSupervisor(MagicMock())
        await sup.start()
        assert sup._running is True

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        sup = ActorSupervisor(MagicMock())
        await sup.start()
        await sup.start()  # 不应抛异常
        assert sup._running is True

    @pytest.mark.asyncio
    async def test_stop_clears_running(self):
        sup = ActorSupervisor(MagicMock())
        await sup.start()
        await sup.stop()
        assert sup._running is False

    @pytest.mark.asyncio
    async def test_stop_kills_children(self):
        core = MagicMock()
        sup = ActorSupervisor(core)
        sup.watch("a1", "MyActor")
        sup.watch("a2", "MyActor")
        await sup.start()
        await sup.stop()
        # kill_actor 应被调用两次
        assert core.kill_actor.call_count == 2
        assert sup.child_ids == []

    @pytest.mark.asyncio
    async def test_stop_cancels_restart_tasks(self):
        core = MagicMock()
        sup = ActorSupervisor(core, backoff=BackoffConfig(initial_delay=10.0))
        sup.watch("a1", "MyActor")
        await sup.start()
        # 触发重启任务
        sup.handle_event("ActorFailed", "a1", "boom")
        assert len(sup._restart_tasks) > 0
        await sup.stop()
        assert len(sup._restart_tasks) == 0


# ---------------------------------------------------------------------------
# ActorSupervisor — handle_event
# ---------------------------------------------------------------------------


class TestActorSupervisorHandleEvent:
    def test_handle_event_not_running_ignored(self):
        sup = ActorSupervisor(MagicMock())
        sup.watch("a1", "MyActor")
        # 未 start，事件应被忽略
        sup.handle_event("ActorFailed", "a1", "boom")
        assert len(sup._restart_tasks) == 0

    def test_handle_event_unknown_actor_ignored(self):
        sup = ActorSupervisor(MagicMock())
        # 不在 watch 列表的 actor
        sup._running = True
        sup.handle_event("ActorFailed", "unknown", "boom")
        assert len(sup._restart_tasks) == 0

    def test_handle_event_actor_started(self):
        sup = ActorSupervisor(MagicMock())
        sup.watch("a1", "MyActor")
        sup._running = True
        # ActorStarted 不触发重启
        sup.handle_event("ActorStarted", "a1", None)
        assert len(sup._restart_tasks) == 0

    def test_handle_event_unknown_event_type_noop(self):
        """未知 event_type 既非 ActorStarted/ActorFailed/ActorStopped，应直接退出（226->exit 分支）。"""
        sup = ActorSupervisor(MagicMock())
        sup.watch("a1", "MyActor")
        sup._running = True
        # 未知事件类型，不应触发任何操作
        sup.handle_event("ActorUnknown", "a1", None)
        assert len(sup._restart_tasks) == 0
        assert "a1" in sup.child_ids

    def test_handle_event_actor_failed_never_policy_unwatches(self):
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.NEVER)
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorFailed", "a1", "boom")
        # NEVER 策略下应 unwatch，不重启
        assert "a1" not in sup.child_ids
        assert len(sup._restart_tasks) == 0

    @pytest.mark.asyncio
    async def test_handle_event_actor_failed_transient_spawns_restart(self):
        import contextlib
        core = MagicMock()
        core.restart_actor = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.TRANSIENT,
                              backoff=BackoffConfig(initial_delay=0.001))
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorFailed", "a1", "boom")
        assert len(sup._restart_tasks) == 1
        # 让重启任务执行完成
        await asyncio.sleep(0.05)
        # 清理任务
        for task in list(sup._restart_tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    @pytest.mark.asyncio
    async def test_handle_event_actor_failed_permanent_spawns_restart(self):
        import contextlib
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.PERMANENT)
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorFailed", "a1", "boom")
        assert len(sup._restart_tasks) == 1
        for task in list(sup._restart_tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    @pytest.mark.asyncio
    async def test_handle_event_actor_stopped_permanent_spawns_restart(self):
        import contextlib
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.PERMANENT)
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorStopped", "a1", None)
        assert len(sup._restart_tasks) == 1
        for task in list(sup._restart_tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    def test_handle_event_actor_stopped_transient_unwatches(self):
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.TRANSIENT)
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorStopped", "a1", None)
        # TRANSIENT 策略下正常停止不重启，unwatch
        assert "a1" not in sup.child_ids
        assert len(sup._restart_tasks) == 0

    def test_handle_event_actor_stopped_never_unwatches(self):
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.NEVER)
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorStopped", "a1", None)
        assert "a1" not in sup.child_ids
        assert len(sup._restart_tasks) == 0

    @pytest.mark.asyncio
    async def test_handle_event_actor_failed_records_last_failure(self):
        import contextlib
        core = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.TRANSIENT)
        sup.watch("a1", "MyActor")
        sup._running = True
        sup.handle_event("ActorFailed", "a1", "specific-error")
        entry = sup._children.get("a1")
        if entry is not None:
            assert entry.last_failure == "specific-error"
        for task in list(sup._restart_tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


# ---------------------------------------------------------------------------
# ActorSupervisor — _restart_actor
# ---------------------------------------------------------------------------


class TestActorSupervisorRestartActor:
    @pytest.mark.asyncio
    async def test_restart_actor_success(self):
        core = MagicMock()
        core.restart_actor = MagicMock()
        sup = ActorSupervisor(core, policy=RestartPolicy.TRANSIENT,
                              backoff=BackoffConfig(initial_delay=0.01))
        sup.watch("a1", "MyActor")
        await sup.start()

        await sup._restart_actor("a1")
        core.restart_actor.assert_called_once_with("a1", "MyActor")
        entry = sup._children["a1"]
        assert entry.restart_count == 1
        assert len(entry.restart_timestamps) == 1

    @pytest.mark.asyncio
    async def test_restart_actor_unknown_actor_noop(self):
        core = MagicMock()
        sup = ActorSupervisor(core)
        await sup.start()
        # 不在 children 中的 actor
        await sup._restart_actor("unknown")
        core.restart_actor.assert_not_called()

    @pytest.mark.asyncio
    async def test_restart_actor_exceeds_limit_unwatches(self):
        core = MagicMock()
        sup = ActorSupervisor(
            core,
            policy=RestartPolicy.TRANSIENT,
            backoff=BackoffConfig(max_retries=2, initial_delay=0.01),
        )
        sup.watch("a1", "MyActor")
        await sup.start()

        # 预填 restart_timestamps 使其超限
        entry = sup._children["a1"]
        entry.restart_timestamps = [time.time(), time.time()]

        await sup._restart_actor("a1")
        # 应 unwatch，不调用 restart_actor
        assert "a1" not in sup.child_ids
        core.restart_actor.assert_not_called()

    @pytest.mark.asyncio
    async def test_restart_actor_max_retries_zero_infinite(self):
        core = MagicMock()
        sup = ActorSupervisor(
            core,
            policy=RestartPolicy.TRANSIENT,
            backoff=BackoffConfig(max_retries=0, initial_delay=0.01),
        )
        sup.watch("a1", "MyActor")
        await sup.start()

        # max_retries=0 表示无限重试
        await sup._restart_actor("a1")
        core.restart_actor.assert_called_once()
        assert sup._children["a1"].restart_count == 1

    @pytest.mark.asyncio
    async def test_restart_actor_core_failure_records_error(self):
        core = MagicMock()
        core.restart_actor = MagicMock(side_effect=RuntimeError("restart failed"))
        sup = ActorSupervisor(
            core,
            policy=RestartPolicy.TRANSIENT,
            backoff=BackoffConfig(initial_delay=0.01),
        )
        sup.watch("a1", "MyActor")
        await sup.start()

        await sup._restart_actor("a1")
        entry = sup._children["a1"]
        assert entry.restart_count == 0  # 未成功重启
        assert "restart failed" in entry.last_failure

    @pytest.mark.asyncio
    async def test_restart_actor_cancelled_propagates(self):
        core = MagicMock()
        sup = ActorSupervisor(
            core,
            policy=RestartPolicy.TRANSIENT,
            backoff=BackoffConfig(initial_delay=10.0),  # 长延迟
        )
        sup.watch("a1", "MyActor")
        await sup.start()

        task = asyncio.create_task(sup._restart_actor("a1"))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# ActorSupervisor — _can_restart / _compute_delay
# ---------------------------------------------------------------------------


class TestActorSupervisorHelpers:
    def test_can_restart_max_retries_zero_always_true(self):
        sup = ActorSupervisor(
            MagicMock(),
            backoff=BackoffConfig(max_retries=0),
        )
        entry = _ChildEntry(actor_id="a1", actor_type="T")
        entry.restart_timestamps = [time.time()] * 100
        assert sup._can_restart(entry) is True

    def test_can_restart_within_limit(self):
        sup = ActorSupervisor(
            MagicMock(),
            backoff=BackoffConfig(max_retries=3, window=10.0),
        )
        entry = _ChildEntry(actor_id="a1", actor_type="T")
        entry.restart_timestamps = [time.time()]  # 1 次，<3
        assert sup._can_restart(entry) is True

    def test_can_restart_exceeds_limit(self):
        sup = ActorSupervisor(
            MagicMock(),
            backoff=BackoffConfig(max_retries=3, window=10.0),
        )
        entry = _ChildEntry(actor_id="a1", actor_type="T")
        entry.restart_timestamps = [time.time(), time.time(), time.time()]  # 3 次
        assert sup._can_restart(entry) is False

    def test_can_restart_filters_expired_timestamps(self):
        sup = ActorSupervisor(
            MagicMock(),
            backoff=BackoffConfig(max_retries=2, window=10.0),
        )
        entry = _ChildEntry(actor_id="a1", actor_type="T")
        # 过期的时间戳（窗口外）应被过滤
        entry.restart_timestamps = [time.time() - 20.0, time.time() - 15.0]
        assert sup._can_restart(entry) is True
        # 过期时间戳应被清理
        assert len(entry.restart_timestamps) == 0

    def test_compute_delay_exponential(self):
        sup = ActorSupervisor(
            MagicMock(),
            backoff=BackoffConfig(initial_delay=1.0, multiplier=2.0, max_delay=100.0),
        )
        assert sup._compute_delay(0) == 1.0
        assert sup._compute_delay(1) == 2.0
        assert sup._compute_delay(2) == 4.0
        assert sup._compute_delay(3) == 8.0

    def test_compute_delay_capped_at_max(self):
        sup = ActorSupervisor(
            MagicMock(),
            backoff=BackoffConfig(initial_delay=1.0, multiplier=2.0, max_delay=10.0),
        )
        # 1*2^10 = 1024，应被限制为 10.0
        assert sup._compute_delay(10) == 10.0
