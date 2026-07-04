"""_orchestration.py 单元测试：OrchestrationLoop / DefaultOrchestrationEventHandler。

覆盖目标：核心路径与组合接口。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from actant._components import EventContext
from actant._orchestration import (
    DefaultCapacityProvider,
    DefaultOrchestrationEventHandler,
    OrchestrationLoop,
    _ContextLogger,
    _log,
)

# ---------------------------------------------------------------------------
# _ContextLogger / _log
# ---------------------------------------------------------------------------


class TestContextLogger:
    def test_process_with_no_extra(self):
        adapter = _ContextLogger(logging.getLogger("test"), {"wf_id": "wf-1", "task_id": None})
        msg, kwargs = adapter.process("hello", {})
        assert "[wf_id=wf-1]" in msg
        assert "task_id" not in msg.split("]")[0]
        assert "hello" in msg
        assert "extra" in kwargs

    def test_process_with_all_none(self):
        adapter = _ContextLogger(logging.getLogger("test"), {"wf_id": None, "task_id": None})
        processed_msg, kwargs = adapter.process("hello", {})
        assert processed_msg == "hello"
        assert kwargs["extra"] == {"wf_id": None, "task_id": None}

    def test_process_with_existing_extra(self):
        adapter = _ContextLogger(logging.getLogger("test"), {"wf_id": "wf-1"})
        msg, kwargs = adapter.process("hello", {"extra": {"custom": "value"}})
        assert kwargs["extra"] == {"wf_id": "wf-1", "custom": "value"}
        assert "[wf_id=wf-1]" in msg

    def test_process_with_none_extra_in_kwargs(self):
        adapter = _ContextLogger(logging.getLogger("test"), {"wf_id": "wf-1"})
        _msg, kwargs = adapter.process("hello", {"extra": None})
        assert kwargs["extra"] == {"wf_id": "wf-1"}

    def test_process_with_empty_self_extra(self):
        adapter = _ContextLogger(logging.getLogger("test"), None)
        msg, kwargs = adapter.process("hello", {})
        assert msg == "hello"
        assert kwargs["extra"] == {}

    def test_log_helper_returns_context_logger(self):
        logger = _log("wf-1", "task-1")
        assert isinstance(logger, _ContextLogger)
        assert logger.extra == {"wf_id": "wf-1", "task_id": "task-1"}

    def test_log_helper_with_none_args(self):
        logger = _log()
        assert isinstance(logger, _ContextLogger)
        assert logger.extra == {"wf_id": None, "task_id": None}


# ---------------------------------------------------------------------------
# DefaultCapacityProvider
# ---------------------------------------------------------------------------


class TestDefaultCapacityProvider:
    def test_update_and_snapshot(self):
        provider = DefaultCapacityProvider()
        provider.update("node-1", 3, 10, endpoint_addr="ep-1")
        snap = provider.snapshot("local", {}, [])
        assert "node-1" in snap
        assert snap["node-1"].available == 3
        assert snap["node-1"].endpoint_addr == "ep-1"
        assert "local" in snap

    def test_update_preserves_endpoint(self):
        provider = DefaultCapacityProvider()
        provider.update("node-1", 5, 10, endpoint_addr="ep-1")
        provider.update("node-1", 3, 8)
        assert provider.endpoint_addr("node-1") == "ep-1"

    def test_snapshot_with_runtime(self):
        provider = DefaultCapacityProvider()
        rt = MagicMock()
        rt.available_capacity.return_value = 4
        rt.max_capacity.return_value = 8
        provider.set_local_runtime(rt)
        snap = provider.snapshot_with_runtime("local", {"gpu": "yes"}, ["t1"])
        local = snap["local"]
        assert local.available == 4
        assert local.max_capacity == 8
        assert local.capabilities["gpu"] == "yes"
        assert local.capabilities["tasks"] == ["t1"]

    def test_endpoint_addr_unknown(self):
        provider = DefaultCapacityProvider()
        assert provider.endpoint_addr("missing") is None


# ---------------------------------------------------------------------------
# EventContext
# ---------------------------------------------------------------------------


class TestEventContext:
    def test_construction_defaults(self):
        ctx = EventContext(
            node_id="local",
            runtime=MagicMock(),
            router=MagicMock(),
            serializer=MagicMock(),
            capacity_provider=DefaultCapacityProvider(),
        )
        assert ctx.node_id == "local"
        assert ctx.local_tasks == []
        assert ctx.condition_evaluators == {}


# ---------------------------------------------------------------------------
# DefaultOrchestrationEventHandler
# ---------------------------------------------------------------------------


def _mock_completion(success: bool = True) -> MagicMock:
    c = MagicMock()
    c.workflow_id = "wf-1"
    c.task_id = "t1"
    c.task_name = "task"
    c.state = "Completed" if success else "Failed"
    c.success = success
    c.error = None if success else "boom"
    c.result = b"ok" if success else None
    return c


class TestDefaultOrchestrationEventHandler:
    @pytest.mark.asyncio
    async def test_on_task_completed_success(self):
        handler = DefaultOrchestrationEventHandler()
        rt = MagicMock()
        rt._complete_task_and_broadcast.return_value = ([], [])
        ctx = EventContext(
            node_id="local",
            runtime=rt,
            router=MagicMock(),
            serializer=MagicMock(),
            capacity_provider=DefaultCapacityProvider(),
        )
        followups = await handler.on_task_completed(ctx, _mock_completion(success=True))
        rt._complete_task_and_broadcast.assert_called_once()
        assert followups is None or followups == []

    @pytest.mark.asyncio
    async def test_on_task_completed_failure_with_retry(self):
        handler = DefaultOrchestrationEventHandler()
        rt = MagicMock()
        rt._complete_task_and_broadcast.return_value = ([], [])
        rt._mark_failed_and_get_retry_info.return_value = MagicMock(
            current_retry_count=0,
            max_retries=3,
            next_delay_ms=10,
            workflow_id="wf-1",
            task_id="t1",
            task_name="task",
            payload=b"payload",
        )
        ctx = EventContext(
            node_id="local",
            runtime=rt,
            router=MagicMock(),
            serializer=MagicMock(),
            capacity_provider=DefaultCapacityProvider(),
        )
        followups = await handler.on_task_completed(ctx, _mock_completion(success=False))
        rt._mark_failed_and_get_retry_info.assert_called_once()
        assert len(followups) == 1

    @pytest.mark.asyncio
    async def test_route_tasks(self):
        from actant.router import TaskRouter

        class AlwaysRemoteRouter(TaskRouter):
            def route(self, local_node, node_key, task_meta, peer_capacities):
                return "remote-1"

        handler = DefaultOrchestrationEventHandler()
        rt = MagicMock()
        rt.node_id.return_value = "local"
        # 本地无容量，任务必须路由到远程
        rt.available_capacity.return_value = 0
        rt.max_capacity.return_value = 0
        provider = DefaultCapacityProvider()
        provider.update("remote-1", 5, 10)
        ctx = EventContext(
            node_id="local",
            runtime=rt,
            router=AlwaysRemoteRouter(),
            serializer=MagicMock(),
            capacity_provider=provider,
        )
        task = MagicMock()
        task.task_id = "t1"
        task.name = "mytask"
        task.workflow_id = "wf-1"
        task.payload = b"payload"
        task.target_node = None
        task.target_endpoint_addr = None
        task.timeout_ms = None
        task.retry_policy = None
        routed = await handler.route_tasks(ctx, [task])
        assert routed[0].target_node == "remote-1"

    @pytest.mark.asyncio
    async def test_on_orchestration_event_heartbeat(self):
        handler = DefaultOrchestrationEventHandler()
        provider = DefaultCapacityProvider()
        ctx = EventContext(
            node_id="local",
            runtime=MagicMock(),
            router=MagicMock(),
            serializer=MagicMock(),
            capacity_provider=provider,
        )
        event = MagicMock()
        event.event_type = "NodeHeartbeat"
        event.node_id = "node-1"
        event.available_capacity = 5
        event.max_capacity = 10
        result = await handler.on_orchestration_event(ctx, event)
        assert result is None
        assert provider._cache["node-1"].available == 5

    @pytest.mark.asyncio
    async def test_on_orchestration_event_worker_drained(self):
        from actant._events import clear as _clear_events
        from actant._events import subscribe as _subscribe

        _clear_events("worker.drained")
        received: list[Any] = []

        @_subscribe("worker.drained")
        async def on_drained(event: Any) -> None:
            received.append(event)

        handler = DefaultOrchestrationEventHandler()
        ctx = EventContext(
            node_id="local",
            runtime=MagicMock(),
            router=MagicMock(),
            serializer=MagicMock(),
            capacity_provider=DefaultCapacityProvider(),
        )
        event = MagicMock()
        event.event_type = "WorkerDrained"
        event.node_id = "node-1"
        result = await handler.on_orchestration_event(ctx, event)
        assert result is None
        assert len(received) == 1
        _clear_events("worker.drained")


# ---------------------------------------------------------------------------
# OrchestrationLoop
# ---------------------------------------------------------------------------


def _make_ctx(
    runtime: Any | None = None,
    router: Any | None = None,
) -> EventContext:
    return EventContext(
        node_id="local",
        runtime=runtime or MagicMock(),
        router=router or MagicMock(),
        serializer=MagicMock(),
        capacity_provider=DefaultCapacityProvider(),
    )


class TestOrchestrationLoopInit:
    def test_default_construction(self):
        runtime = MagicMock()
        handler = DefaultOrchestrationEventHandler()
        ctx = _make_ctx(runtime)
        loop = OrchestrationLoop(runtime, handler, ctx)
        assert loop._runtime is runtime
        assert loop._handler is handler
        assert loop._ctx is ctx
        assert loop._running is False


class TestOrchestrationLoopStartStop:
    @pytest.mark.asyncio
    async def test_start_sets_running_and_callbacks(self):
        runtime = MagicMock()
        handler = DefaultOrchestrationEventHandler()
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop.start()
        assert loop._running is True
        runtime.set_event_callback.assert_called_once_with(loop._on_rust_event)
        await loop.stop()

    @pytest.mark.asyncio
    async def test_stop_clears_running(self):
        runtime = MagicMock()
        handler = DefaultOrchestrationEventHandler()
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop.start()
        await loop.stop()
        assert loop._running is False
        assert loop._reroute_task is None

    @pytest.mark.asyncio
    async def test_stop_cancels_tracked_tasks(self):
        runtime = MagicMock()
        handler = DefaultOrchestrationEventHandler()
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop.start()

        async def long_task():
            await asyncio.sleep(100)

        loop._spawn_tracked(long_task())
        assert len(loop._tracked_tasks) > 0

        await loop.stop()
        assert len(loop._tracked_tasks) == 0


class TestOnRustEvent:
    def test_not_running_ignored(self):
        runtime = MagicMock()
        handler = MagicMock()
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        event = MagicMock()
        loop._on_rust_event(event)
        handler.on_task_completed.assert_not_called()

    @pytest.mark.asyncio
    async def test_completion_event(self):
        runtime = MagicMock()
        handler = AsyncMock(return_value=None)
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop.start()
        event = MagicMock()
        event.kind = "completion"
        event.completion = MagicMock()
        loop._on_rust_event(event)
        await asyncio.sleep(0.01)
        handler.on_task_completed.assert_called_once()
        await loop.stop()

    @pytest.mark.asyncio
    async def test_supervision_event(self):
        from actant._events import clear as _clear_events
        from actant._events import subscribe as _subscribe

        _clear_events("actor.failed")
        received: list[Any] = []

        @_subscribe("actor.failed")
        async def on_actor_failed(event: Any) -> None:
            received.append(event)

        runtime = MagicMock()
        loop = OrchestrationLoop(runtime, MagicMock(), _make_ctx(runtime))
        await loop.start()
        event = MagicMock()
        event.kind = "supervision"
        event.supervision = MagicMock()
        event.event_type = "ActorFailed"
        event.actor_id = "a1"
        event.error = "boom"
        loop._on_rust_event(event)
        await asyncio.sleep(0.01)
        assert len(received) == 1
        assert received[0].actor_id == "a1"
        assert received[0].error == "boom"
        await loop.stop()
        _clear_events("actor.failed")

    @pytest.mark.asyncio
    async def test_orchestration_event(self):
        runtime = MagicMock()
        handler = AsyncMock(return_value=None)
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop.start()
        event = MagicMock()
        event.kind = "orchestration"
        event.completion = None
        event.supervision = None
        event.orchestration = MagicMock()
        loop._on_rust_event(event)
        await asyncio.sleep(0.01)
        handler.on_orchestration_event.assert_called_once()
        await loop.stop()


class TestRerouteLoop:
    @pytest.mark.asyncio
    async def test_try_reroute_no_unrouted(self):
        runtime = MagicMock()
        runtime._drain_unrouted_tasks.return_value = []
        handler = DefaultOrchestrationEventHandler()
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop._try_reroute()
        runtime._drain_unrouted_tasks.assert_called_once()


class TestRecoverWorkflows:
    @pytest.mark.asyncio
    async def test_recover_with_exception(self):
        runtime = MagicMock()
        runtime._recoverable_workflows_with_pending.side_effect = RuntimeError("no data_dir")
        handler = DefaultOrchestrationEventHandler()
        loop = OrchestrationLoop(runtime, handler, _make_ctx(runtime))
        await loop._recover_workflows()

    @pytest.mark.asyncio
    async def test_recover_with_workflows(self):
        from actant.router import TaskRouter

        class AlwaysRemoteRouter(TaskRouter):
            def route(self, local_node, node_key, task_meta, peer_capacities):
                return "remote-1"

        runtime = MagicMock()
        runtime._recoverable_workflows_with_pending.return_value = [("wf-1", ["task-1"])]
        runtime.available_capacity.return_value = 0
        runtime.max_capacity.return_value = 0
        task = MagicMock()
        task.task_id = "t1"
        task.name = "mytask"
        task.workflow_id = "wf-1"
        task.payload = b"payload"
        task.target_node = None
        task.target_endpoint_addr = None
        task.timeout_ms = None
        task.retry_policy = None
        runtime._recover_workflow.return_value = [task]
        handler = DefaultOrchestrationEventHandler()
        ctx = _make_ctx(runtime, router=AlwaysRemoteRouter())
        loop = OrchestrationLoop(runtime, handler, ctx)
        await loop._recover_workflows()
        runtime.enqueue_tasks.assert_called_once()


class TestCapacityRefreshLoop:
    @pytest.mark.asyncio
    async def test_periodic_refresh_failure_swallowed(self):
        runtime = MagicMock()
        call_count = [0]

        def refresh():
            call_count[0] += 1
            if call_count[0] > 1:
                raise RuntimeError("periodic failed")

        handler = DefaultOrchestrationEventHandler()
        loop = OrchestrationLoop(
            runtime,
            handler,
            _make_ctx(runtime),
            refresh_capacity=refresh,
            capacity_refresh_interval=0.01,
        )
        loop._running = True

        task = asyncio.create_task(loop._capacity_refresh_loop())
        await asyncio.sleep(0.05)
        loop._running = False
        with __import__("contextlib").suppress(Exception):
            await asyncio.wait_for(task, timeout=1.0)
        assert call_count[0] >= 2
