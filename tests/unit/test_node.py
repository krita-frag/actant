"""_node.py 单元测试：_Node / _dispatch_generic_task / _RouteInfo。

覆盖目标：100% 行覆盖 + 分支覆盖。
通过 mock _RuntimeCore 避免真实网络/持久化。
"""

from __future__ import annotations

import asyncio
import dataclasses
import struct
import threading
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from actant._node import _dispatch_generic_task, _Node
from actant._orchestration import _RouteInfo
from actant._serialization import (
    TAG_GENERIC,
    TAG_POSITIONAL,
    TAG_SINGLE,
    TAG_UPSTREAM_PREFIX,
    dumps,
    loads,
)
from actant.config import NetworkConfig
from actant.exceptions import InvalidStateError, NotFoundError, SerializationError
from actant.router import LeastLoadedRouter


def _pack_upstream_prefix(upstream_results: list[Any], inner_payload: bytes) -> bytes:
    """构造 TAG_UPSTREAM_PREFIX 包装的 payload（测试辅助）。"""
    buf = bytearray()
    buf.append(TAG_UPSTREAM_PREFIX)
    buf += struct.pack("<I", len(upstream_results))
    for r in upstream_results:
        data = dumps(r)
        buf += struct.pack("<I", len(data))
        buf += data
    buf += inner_payload
    return bytes(buf)


@pytest.fixture(autouse=True)
def _fast_node_shutdown(monkeypatch):
    """让 _Node.shutdown() 在 mock runtime 下快速返回。

    shutdown() 内部轮询 ``runtime.running_task_count()`` 直到为 0 或超时（10s）。
    当测试用 MagicMock 替换 _runtime 但未设置 running_task_count.return_value 时，
    running_task_count() 返回 MagicMock 对象（!= 0），导致每次 shutdown 等满超时。

    本 fixture 包装 _Node.shutdown：若 _runtime 是 MagicMock 且
    running_task_count.return_value 仍是默认 MagicMock（未显式设置），
    则将其设为 0，让轮询立即退出。测试若需模拟运行中任务，显式设置
    running_task_count.return_value = N 即可走真实轮询逻辑。
    """
    from unittest.mock import MagicMock as _MM

    from actant import _node as _node_mod

    original_shutdown = _node_mod._Node.shutdown

    def _fast_shutdown(self, timeout: float = 10.0):
        rt = self._runtime
        if isinstance(rt, _MM):
            rtc = rt.running_task_count
            # return_value 是默认 MagicMock（未显式设置整数等）时，设为 0
            if isinstance(rtc.return_value, _MM):
                rtc.return_value = 0
        return original_shutdown(self, timeout=timeout)

    monkeypatch.setattr(_node_mod._Node, "shutdown", _fast_shutdown)


# ---------------------------------------------------------------------------
# _RouteInfo
# ---------------------------------------------------------------------------


class TestRouteInfo:
    def test_construction(self):
        info = _RouteInfo(node_key="0", task_name="mytask", tags=["gpu"], priority=1)
        assert info.node_key == "0"
        assert info.task_name == "mytask"
        assert info.tags == ["gpu"]
        assert info.priority == 1

    def test_frozen(self):
        info = _RouteInfo(node_key="0", task_name="mytask", tags=[], priority=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            info.node_key = "1"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _dispatch_generic_task
# ---------------------------------------------------------------------------


class TestDispatchGenericTask:
    def test_dispatch_generic_payload(self):
        def fn(x):
            return x * 2

        payload = bytes([TAG_GENERIC]) + dumps((fn, (5,), {}))
        result = _dispatch_generic_task(payload)
        assert loads(result) == 10

    def test_dispatch_positional_payload(self):
        def fn(a, b):
            return a + b

        # TAG_POSITIONAL 格式：
        # (inline_fn, positions, kwargs_keys, concrete_args, concrete_kwargs)
        # positions 标记 upstream 结果的位置（这里无 upstream，留空）
        payload = bytes([TAG_POSITIONAL]) + dumps((fn, (), (), (3, 4), {}))
        result = _dispatch_generic_task(payload)
        assert loads(result) == 7

    def test_dispatch_with_upstream_prefix(self):
        def fn(x):
            return x + 1

        inner_payload = bytes([TAG_GENERIC]) + dumps((fn, (10,), {}))
        payload = _pack_upstream_prefix([b"upstream-result"], inner_payload)
        result = _dispatch_generic_task(payload)
        assert loads(result) == 11

    def test_dispatch_invalid_inner_tag_raises(self):
        from actant.exceptions import SerializationError

        # TAG_SINGLE 是命名路径载荷，generic handler 应抛 SerializationError
        payload = bytes([TAG_SINGLE]) + dumps((b"some-data",))
        with pytest.raises(SerializationError, match="fell back to generic handler"):
            _dispatch_generic_task(payload)

    def test_dispatch_invalid_inner_tag_with_upstream_prefix(self):
        from actant.exceptions import SerializationError

        inner_payload = bytes([TAG_SINGLE]) + dumps((b"data",))
        payload = _pack_upstream_prefix([b"upstream"], inner_payload)
        with pytest.raises(SerializationError, match="fell back to generic handler"):
            _dispatch_generic_task(payload)


# ---------------------------------------------------------------------------
# _Node — 构造
# ---------------------------------------------------------------------------


class TestNodeConstruction:
    def test_default_construction(self):
        node = _Node("test-node", _executing=False, signing_key="test-key")
        assert node.name == "test-node"
        assert node._executing is False
        assert node._runtime is None
        assert isinstance(node.network, NetworkConfig)
        assert isinstance(node.router, LeastLoadedRouter)
        assert node._capabilities == {}
        assert node._tasks == {}
        assert node._actors == {}

    def test_with_dict_network(self):
        node = _Node("test", _executing=False, signing_key="test-key", network={"preset": "local"})
        assert node.network.preset == "local"

    def test_with_network_config(self):
        cfg = NetworkConfig(preset="local")
        node = _Node("test", _executing=False, signing_key="test-key", network=cfg)
        assert node.network is cfg

    def test_with_custom_router(self):
        router = LeastLoadedRouter()
        node = _Node("test", _executing=False, signing_key="test-key", router=router)
        assert node.router is router

    def test_with_capabilities(self):
        node = _Node("test", _executing=False, signing_key="test-key", capabilities={"gpu": True})
        assert node._capabilities == {"gpu": True}

    def test_with_log_level(self):
        with patch("actant._logging.configure_logging") as mock_cfg:
            node = _Node("test", _executing=False, signing_key="test-key", log_level="DEBUG")
            mock_cfg.assert_called_once_with("DEBUG", force=True)
        # 清理 node
        node.shutdown()

    def test_context_manager(self):
        with patch.object(_Node, "start") as mock_start, \
             patch.object(_Node, "shutdown") as mock_shutdown:
            with _Node("test", _executing=False, signing_key="test-key"):
                mock_start.assert_called_once()
            mock_shutdown.assert_called_once()


# ---------------------------------------------------------------------------
# _Node — 事件系统
# ---------------------------------------------------------------------------


class TestNodeEvents:
    def test_on_with_handler(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock()
        result = node.on("custom", handler)
        assert result is handler
        node.emit("custom", 1, 2, key="value")
        handler.assert_called_once_with(1, 2, key="value")

    def test_on_as_decorator(self):
        node = _Node("test", _executing=False, signing_key="test-key")

        @node.on("custom")
        def handler(*args, **kwargs):
            pass

        node.emit("custom")
        # handler 应被注册
        assert "custom" in node._custom_events

    def test_on_startup_event(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock()
        node.on("startup", handler)
        node._fire_startup()
        handler.assert_called_once()

    def test_on_shutdown_event(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock()
        node.on("shutdown", handler)
        node._fire_shutdown()
        handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_task_start_event(self):
        from actant._events import clear as _clear_events
        from actant._events import dispatch as _dispatch

        _clear_events("task.started")
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock()
        node.on("task_start", handler)
        from types import SimpleNamespace

        event = SimpleNamespace(workflow_id="wf-1", task_name="task-1")
        await _dispatch("task.started", event)
        handler.assert_called_once_with(event)
        _clear_events("task.started")

    @pytest.mark.asyncio
    async def test_on_task_complete_event(self):
        from actant._events import clear as _clear_events
        from actant._events import dispatch as _dispatch

        _clear_events("task.completed")
        _clear_events("task.failed")
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock()
        node.on("task_complete", handler)
        from types import SimpleNamespace

        event = SimpleNamespace(workflow_id="wf-1", task_name="task-1", state="Completed")
        await _dispatch("task.completed", event)
        await _dispatch("task.failed", event)
        # task_complete 同时订阅 completed 和 failed
        assert handler.call_count == 2
        _clear_events("task.completed")
        _clear_events("task.failed")

    def test_emit_no_handlers(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        # 无 handler，不应抛异常
        node.emit("unknown-event")

    def test_emit_handler_exception_swallowed(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock(side_effect=RuntimeError("handler failed"))
        node.on("custom", handler)
        # 不应抛异常
        node.emit("custom")

    def test_fire_startup_exception_swallowed(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock(side_effect=RuntimeError("startup failed"))
        node.on("startup", handler)
        # 不应抛异常
        node._fire_startup()

    def test_fire_shutdown_exception_swallowed(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock(side_effect=RuntimeError("shutdown failed"))
        node.on("shutdown", handler)
        node._fire_shutdown()

    @pytest.mark.asyncio
    async def test_fire_task_start_exception_swallowed(self):
        from actant._events import clear as _clear_events
        from actant._events import dispatch as _dispatch

        _clear_events("task.started")
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock(side_effect=RuntimeError("failed"))
        node.on("task_start", handler)
        from types import SimpleNamespace

        await _dispatch("task.started", SimpleNamespace(workflow_id="wf-1", task_name="task-1"))
        _clear_events("task.started")

    @pytest.mark.asyncio
    async def test_fire_task_complete_exception_swallowed(self):
        from actant._events import clear as _clear_events
        from actant._events import dispatch as _dispatch

        _clear_events("task.failed")
        node = _Node("test", _executing=False, signing_key="test-key")
        handler = MagicMock(side_effect=RuntimeError("failed"))
        node.on("task_complete", handler)
        from types import SimpleNamespace

        await _dispatch(
            "task.failed", SimpleNamespace(workflow_id="wf-1", task_name="task-1", state="Failed")
        )
        _clear_events("task.failed")


# ---------------------------------------------------------------------------
# _Node — 容量属性
# ---------------------------------------------------------------------------


class TestNodeCapacity:
    def test_available_capacity_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.available_capacity == 0

    def test_available_capacity_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 3
        assert node.available_capacity == 7

    def test_available_capacity_none_max(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = None
        node._runtime.running_task_count.return_value = 3
        assert node.available_capacity == 0

    def test_available_capacity_none_running(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = None
        assert node.available_capacity == 0

    def test_available_capacity_negative_clamped(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 5
        node._runtime.running_task_count.return_value = 10
        assert node.available_capacity == 0

    def test_max_capacity_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.max_capacity == 0

    def test_max_capacity_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 10
        assert node.max_capacity == 10

    def test_max_capacity_none(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = None
        assert node.max_capacity == 0

    def test_running_task_count_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.running_task_count == 0

    def test_running_task_count_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 5
        assert node.running_task_count == 5

    def test_running_task_count_none(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = None
        assert node.running_task_count == 0

    def test_local_node_capacity(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 3
        cap = node._local_node_capacity()
        assert cap.available == 7
        assert cap.max_capacity == 10


# ---------------------------------------------------------------------------
# _Node — peer capacity cache
# ---------------------------------------------------------------------------


class TestPeerCapacityCache:
    def test_update_peer_capacity_cache_new(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node.capacity_provider.update("node-1", 5, 10)
        cap = node.capacity_provider._cache["node-1"]
        assert cap.available == 5
        assert cap.max_capacity == 10
        assert cap.endpoint_addr is None

    def test_update_peer_capacity_cache_preserves_endpoint(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node.capacity_provider.update("node-1", 5, 10, endpoint_addr="ep-1")
        node.capacity_provider.update("node-1", 3, 8)
        cap = node.capacity_provider._cache["node-1"]
        assert cap.available == 3
        assert cap.max_capacity == 8
        assert cap.endpoint_addr == "ep-1"

    def test_refresh_peer_capacities_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._refresh_peer_capacities()

    def test_refresh_peer_capacities_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        cap1 = MagicMock()
        cap1.available = 5
        cap1.max = 10
        cap1.endpoint_addr = "ep-1"
        cap2 = MagicMock()
        cap2.available = 3
        cap2.max = 8
        cap2.endpoint_addr = None
        node._runtime.get_peer_capacities.return_value = {"node-1": cap1, "node-2": cap2}
        node._refresh_peer_capacities()
        assert "node-1" in node.capacity_provider._cache
        assert "node-2" in node.capacity_provider._cache
        assert node.capacity_provider._cache["node-1"].max_capacity == 10

    def test_refresh_peer_capacities_preserves_endpoint(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node.capacity_provider.update("node-1", 5, 10, endpoint_addr="ep-1")
        cap = MagicMock()
        cap.available = 3
        cap.max = 8
        cap.endpoint_addr = None
        node._runtime.get_peer_capacities.return_value = {"node-1": cap}
        node._refresh_peer_capacities()
        cached = node.capacity_provider._cache["node-1"]
        assert cached.endpoint_addr == "ep-1"

    def test_peer_capacities_snapshot_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node._peer_capacities_snapshot() == {}

    def test_peer_capacities_snapshot_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 3
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")
        snapshot = node._peer_capacities_snapshot()
        assert "local" in snapshot
        assert "remote-1" in snapshot

    def test_peer_capacities_snapshot_with_capabilities(self):
        node = _Node("test", _executing=False, signing_key="test-key", capabilities={"gpu": True})
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        from actant.task import Task

        task = Task("mytask", lambda: None)
        node._tasks["mytask"] = task
        snapshot = node._peer_capacities_snapshot()
        local_cap = snapshot["local"]
        assert "gpu" in local_cap.capabilities
        assert "tasks" in local_cap.capabilities


# ---------------------------------------------------------------------------
# _Node — 路由
# ---------------------------------------------------------------------------


class TestNodeRouting:
    def test_build_route_map_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        target = node._build_route_map([])
        assert target is None

    def test_build_route_map_local_capacity_zero_does_not_route_local(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0
        node._runtime.running_task_count.return_value = 0
        info = _RouteInfo(node_key="0", task_name="anytask", tags=[], priority=None)
        target = node._build_route_map([info])
        assert target is None or target.get("0") != "local"

    def test_build_route_map_with_local_task_no_remote(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        from actant.task import Task

        task = Task("mytask", lambda: None)
        node._tasks["mytask"] = task
        info = _RouteInfo(node_key="0", task_name="mytask", tags=[], priority=None)
        target = node._build_route_map([info])
        # 本地能执行，无需远程路由
        assert target is None or "0" not in target

    def test_build_route_map_no_local_capacity_considers_remote(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0
        node._runtime.running_task_count.return_value = 0
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")
        info = _RouteInfo(node_key="0", task_name="mytask", tags=[], priority=None)
        # local max=0，至少不应返回本地节点
        target = node._build_route_map([info])
        assert target is None or target.get("0") != "local"

    def test_route_endpoint_addrs_empty(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node._route_endpoint_addrs({}) is None

    def test_route_endpoint_addrs_with_endpoints(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")
        result = node._route_endpoint_addrs({"t1": "remote-1"})
        assert result == {"t1": "ep-1"}

    def test_route_endpoint_addrs_no_endpoint(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        node.capacity_provider.update("remote-1", 5, 10)
        result = node._route_endpoint_addrs({"t1": "remote-1"})
        assert result is None

    def test_route_orchestration_tasks(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        task = MagicMock()
        task.task_id = "t1"
        task.name = "mytask"
        from actant.task import Task

        task_obj = Task("mytask", lambda: None)
        node._tasks["mytask"] = task_obj
        result = node._route_orchestration_tasks([task])
        # 本地能执行，无需远程
        assert result is None or "t1" not in result


# ---------------------------------------------------------------------------
# _Node — 监督
# ---------------------------------------------------------------------------


class TestNodeRemoteAndSupervision:
    def test_on_actor_event_forwards_to_supervisors(self):
        from types import SimpleNamespace

        node = _Node("test", _executing=False, signing_key="test-key")
        sup1 = MagicMock()
        sup2 = MagicMock()
        node._supervisors = [sup1, sup2]
        event = SimpleNamespace(event_type="ActorFailed", actor_id="a1", error="boom")
        node._on_actor_event(event)
        sup1.handle_event.assert_called_once_with("ActorFailed", "a1", "boom")
        sup2.handle_event.assert_called_once_with("ActorFailed", "a1", "boom")


# ---------------------------------------------------------------------------
# _Node — actor 注册
# ---------------------------------------------------------------------------


class TestNodeActor:
    def test_actor_registration(self):
        node = _Node("test", _executing=False, signing_key="test-key")

        class MyActor:
            pass

        actor = node.actor(MyActor)
        assert actor.name == "MyActor"
        assert "MyActor" in node._actors

    def test_actor_registration_non_class_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(TypeError, match="cls must be a Python class"):
            node.actor("not-a-class")  # type: ignore[arg-type]

    def test_create_actor_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")

        class MyActor:
            pass

        with pytest.raises(InvalidStateError, match="not running"):
            node.create_actor(MyActor)

    def test_create_actor_with_class(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()

        class MyActor:
            def __init__(self):
                self.value = 0

        actor = node.create_actor(MyActor)
        assert actor.is_proxy
        node._runtime.create_actor.assert_called_once()

    def test_create_actor_with_existing_actor(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()

        class MyActor:
            def __init__(self):
                self.value = 0

        # 先注册
        registered = node.actor(MyActor)
        # 再创建
        actor = node.create_actor(registered)
        assert actor.is_proxy

    def test_create_actor_invalid_type_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        with pytest.raises(TypeError, match="expected Actor or class"):
            node.create_actor("invalid")  # type: ignore[arg-type]

    def test_create_actor_no_class_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        from actant.actor import Actor

        actor = Actor("NoClass", cls=None)
        with pytest.raises(InvalidStateError, match="no class registered"):
            node.create_actor(actor)

    def test_create_supervisor(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()
        sup = node.create_supervisor()
        assert sup in node._supervisors

    def test_create_supervisor_with_backoff(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()
        sup = node.create_supervisor(backoff={"max_retries": 3})
        assert sup._backoff.max_retries == 3

    def test_create_supervisor_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError, match="not running"):
            node.create_supervisor()


# ---------------------------------------------------------------------------
# _Node — submit / cancel / resubmit
# ---------------------------------------------------------------------------


class TestNodeSubmit:
    def test_submit_non_flow_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(TypeError, match="expected Flow"):
            node.submit("not-a-flow")  # type: ignore[arg-type]

    def test_submit_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        from actant.flow import Flow
        from actant.task import _make_task

        def t():
            return 1

        t_task = _make_task(t, name="t_for_test_submit")

        @Flow
        def my_flow():
            return t_task()

        # _build_dag 成功（产生一个任务），_submit_dag 因无 runtime 抛错
        with pytest.raises(InvalidStateError, match="Actant runtime is not running"):
            node.submit(my_flow)

    def test_cancel_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError, match="not running"):
            node.cancel("wf-1")

    def test_cancel_task_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError, match="not running"):
            node.cancel_task("wf-1", "task-1")

    def test_cancel_task_runtime_error_returns_false(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.cancel_task.side_effect = RuntimeError("cancel failed")
        assert node.cancel_task("wf-1", "task-1") is False

    def test_cancel_task_success(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.cancel_task.return_value = True
        assert node.cancel_task("wf-1", "task-1") is True

    def test_resubmit_workflow_unknown_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(KeyError, match="not found in submission history"):
            node.resubmit_workflow("unknown-wf")

    def test_get_stored_results_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError, match="not running"):
            node.get_stored_results("wf-1")

    def test_get_stored_results_none_returns_none(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.get_stored_results.return_value = None
        assert node.get_stored_results("wf-1") is None

    def test_get_stored_results_with_data(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        from actant._serialization import dumps

        node._runtime.get_stored_results.return_value = [dumps(42), dumps("hello")]
        results = node.get_stored_results("wf-1")
        assert results == [42, "hello"]


# ---------------------------------------------------------------------------
# _Node — runtime 状态查询（无 runtime 路径）
# ---------------------------------------------------------------------------


class TestNodeStateQueriesNoRuntime:
    def test_list_actors_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.list_actors()

    def test_list_workflows_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.list_workflows()

    def test_workflow_state_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.workflow_state("wf-1")

    def test_task_states_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.task_states("wf-1")

    def test_get_workflow_status_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.get_workflow_status("wf-1")

    def test_stop_actor_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.stop_actor("a1")

    def test_actor_status_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.actor_status("a1")

    def test_metrics_prometheus_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.metrics_prometheus()

    def test_capacity_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            _ = node.capacity

    def test_node_id_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            _ = node.node_id

    def test_connect_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.connect("addr")

    def test_add_gossip_peer_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.add_gossip_peer("peer-1")

    def test_drain_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.drain()

    def test_start_metrics_server_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.start_metrics_server()


# ---------------------------------------------------------------------------
# _Node — runtime 状态查询（有 runtime 路径）
# ---------------------------------------------------------------------------


class TestNodeStateQueriesWithRuntime:
    def test_task_names(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        from actant.task import Task

        node._tasks["t1"] = Task("t1", lambda: None)
        node._tasks["t2"] = Task("t2", lambda: None)
        names = node.task_names
        assert set(names) == {"t1", "t2"}

    def test_capabilities(self):
        node = _Node("test", _executing=False, signing_key="test-key", capabilities={"gpu": True})
        assert node.capabilities == {"gpu": True}

    def test_capacity_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 3
        cap = node.capacity
        assert cap.available == 7

    def test_node_id_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "node-123"
        assert node.node_id == "node-123"

    def test_list_actors_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value.list_actors.return_value = ["a1", "a2"]
        assert node.list_actors() == ["a1", "a2"]

    def test_list_workflows_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.list_workflows.return_value = [("wf-1", "Running")]
        assert node.list_workflows() == [("wf-1", "Running")]

    def test_workflow_state_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Running"
        assert node.workflow_state("wf-1") == "Running"

    def test_task_states_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.task_states.return_value = [("task-1", "Completed")]
        assert node.task_states("wf-1") == [("task-1", "Completed")]

    def test_metrics_prometheus_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.prometheus_text.return_value = "# metrics"
        assert node.metrics_prometheus() == "# metrics"

    def test_stop_actor_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node.stop_actor("a1")
        node._runtime.actor_core.return_value.stop_actor.assert_called_once_with("a1")

    def test_actor_status_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value.actor_status.return_value = "Running"
        assert node.actor_status("a1") == "Running"

    def test_get_workflow_status_not_found(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = None

        with pytest.raises(NotFoundError):
            node.get_workflow_status("wf-1")

    def test_get_workflow_status_running(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Running"
        node._runtime.task_states.return_value = [("task-1", "Completed"), ("task-2", "Running")]
        status = node.get_workflow_status("wf-1")
        assert status["state"] == "Running"
        assert status["result_count"] == 1
        assert len(status["tasks"]) == 2
        assert status["error"] is None

    def test_get_workflow_status_failed_with_error(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = []
        from actant._serialization import dumps

        # 模拟存储的错误结果
        error = RuntimeError("task failed")
        node._runtime.get_stored_results.return_value = [[dumps(error)]]
        status = node.get_workflow_status("wf-1")
        assert status["state"] == "Failed"
        assert "task failed" in status["error"]

    def test_get_workflow_status_failed_no_stored_results(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = []
        node._runtime.get_stored_results.return_value = None
        status = node.get_workflow_status("wf-1")
        assert status["error"] is None

    def test_get_workflow_status_failed_deserialize_error(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = []
        # 提供无法反序列化的数据
        node._runtime.get_stored_results.return_value = [[b"bad-data"]]
        status = node.get_workflow_status("wf-1")
        # 反序列化失败应被忽略
        assert status["error"] is None

    def test_get_workflow_status_failed_non_exception_result(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = []
        from actant._serialization import dumps

        # 非 Exception 结果不应作为 error
        node._runtime.get_stored_results.return_value = [[dumps("not-an-error")]]
        status = node.get_workflow_status("wf-1")
        assert status["error"] is None


# ---------------------------------------------------------------------------
# _Node — listen_addresses / connect / add_gossip_peer
# ---------------------------------------------------------------------------


class TestNodeNetworking:
    def test_listen_addresses_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.listen_addresses()

    def test_listen_addresses_no_event_loop(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._event_loop = None
        result = node.listen_addresses()
        assert result == {"endpoint_id": "", "relay_url": None, "direct_addrs": []}

    def test_connect_no_event_loop(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._event_loop = None
        with pytest.raises(InvalidStateError, match="event loop not running"):
            node.connect("addr")

    def test_add_gossip_peer_no_event_loop(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._event_loop = None
        with pytest.raises(InvalidStateError, match="event loop not running"):
            node.add_gossip_peer("peer-1")


# ---------------------------------------------------------------------------
# _Node — metrics server
# ---------------------------------------------------------------------------


class TestNodeMetricsServer:
    def test_has_metrics_server_default_false(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.has_metrics_server() is False

    def test_stop_metrics_server_noop_when_not_running(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        # 不应抛异常
        node.stop_metrics_server()

    def test_start_and_stop_metrics_server(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "node-1"
        node._runtime.get_health_info.return_value = ("Healthy", 3)
        node._runtime.prometheus_text.return_value = "# metrics"

        port = node.start_metrics_server(port=0)
        assert port > 0
        assert node.has_metrics_server() is True

        node.stop_metrics_server()
        assert node.has_metrics_server() is False


# ---------------------------------------------------------------------------
# _Node — __repr__
# ---------------------------------------------------------------------------


class TestNodeRepr:
    def test_repr(self):
        node = _Node("my-node", _executing=False, signing_key="test-key")
        assert repr(node) == "<_Node my-node>"


# ---------------------------------------------------------------------------
# _Node — drain
# ---------------------------------------------------------------------------


class TestNodeDrain:
    def test_drain_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.drain()

    def test_drain_no_running_tasks(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        assert node.drain() is True

    def test_drain_with_running_tasks_timeout(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 5
        # drained_event 不会被 set，应超时返回 False
        result = node.drain(timeout=0.1)
        assert result is False

    def test_drain_with_running_tasks_event_set(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 5

        # 在另一线程设置 drained_event
        def set_event():
            import time
            time.sleep(0.05)
            node._drained_event.set()

        threading.Thread(target=set_event, daemon=True).start()
        result = node.drain(timeout=1.0)
        assert result is True


# ---------------------------------------------------------------------------
# _Node — shutdown
# ---------------------------------------------------------------------------


class TestNodeShutdown:
    def test_shutdown_no_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        # 无 runtime，应不抛异常
        node.shutdown()

    def test_shutdown_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        node.shutdown()
        assert node._runtime is None
        assert node.capacity_provider._cache == {}
        assert node._ctx is None

    def test_shutdown_with_running_tasks(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        # 模拟任务一直运行，直到超时
        node._runtime.running_task_count.return_value = 5
        node.shutdown(timeout=0.2)
        assert node._runtime is None
        # shutdown 应被调用
        node._runtime = MagicMock()  # 重新 mock 因为已设 None
        # 检查 shutdown_event 已 set
        assert node._shutdown_event.is_set()

    def test_shutdown_fires_shutdown_hooks(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        hook = MagicMock()
        node.on("shutdown", hook)
        node.shutdown()
        hook.assert_called_once()

    def test_shutdown_stops_metrics_server(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        metrics_server = MagicMock()
        node._metrics_server = metrics_server
        node.shutdown()
        metrics_server.shutdown.assert_called_once()


# ---------------------------------------------------------------------------
# _Node — start (mock _RuntimeCore.start)
# ---------------------------------------------------------------------------


class TestNodeStart:
    def test_start_idempotent(self):
        """已启动的 node 再次 start 应直接返回。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()  # 模拟已启动
        with patch.object(node, "_start_orchestration") as mock_orch:
            node.start()
            mock_orch.assert_not_called()

    def test_start_executing_node_loads_global_tasks(self):
        """executing=True 的 node 应加载全局任务。"""
        from actant.task import Task, _global_tasks, register_global_task

        node = _Node("test", _executing=True, signing_key="test-key")

        # 添加一个全局任务
        def my_fn():
            return 42

        task = Task("globaltask", my_fn)
        register_global_task(task)

        try:
            with patch("actant._node._RuntimeCore.start") as mock_start, \
                 patch.object(node, "_start_orchestration"):
                mock_runtime = MagicMock()
                mock_start.return_value = mock_runtime
                node.start()
                # 应加载全局任务
                assert "globaltask" in node._tasks
                # 应注册 __actant_generic__ handler
                mock_start.assert_called_once()
                # 检查 tasks 参数包含 __actant_generic__
                call_kwargs = mock_start.call_args
                assert "__actant_generic__" in call_kwargs.kwargs["tasks"]
                node.shutdown()
        finally:
            _global_tasks.pop("globaltask", None)

    def test_start_transient_node_no_global_tasks(self):
        """executing=False 的 node 不应加载全局任务。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_runtime = MagicMock()
            mock_start.return_value = mock_runtime
            node.start()
            # tasks 应为空 dict
            call_kwargs = mock_start.call_args
            assert call_kwargs.kwargs["tasks"] == {}
            node.shutdown()

    def test_start_fires_startup_hooks(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        hook = MagicMock()
        node.on("startup", hook)
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            hook.assert_called_once()
            node.shutdown()

    def test_start_sets_ready_event(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            assert node._ready_event.is_set()
            node.shutdown()

    def test_start_with_heartbeat_and_failure_timeout(self):
        """测试 heartbeat_interval 和 failure_timeout 配置路径。"""
        node = _Node(
            "test",
            _executing=False, signing_key="test-key",
            heartbeat_interval=1.5,
            failure_timeout=30.0,
            default_task_timeout=60.0,
        )
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            call_kwargs = mock_start.call_args
            config = call_kwargs.kwargs["config"]
            assert config.failover.heartbeat_interval_ms == 1500
            assert config.failover.failure_timeout_ms == 30000
            assert config.default_task_timeout_ms == 60000
            node.shutdown()

    def test_start_no_failover_config(self):
        """heartbeat 和 failure 都为 None 时 failover 应为 None。

        Rust 侧 _ActantConfig 在 failover=None 时会用默认值填充 _FailoverConfig，
        因此验证 Python 侧传入的 failover 参数为 None 即可。
        """
        node = _Node("test", _executing=False, signing_key="test-key")
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            # 构造 _ActantConfig 时传入的 failover 应为 None
            # （Rust 侧会用默认值填充，但 Python 侧应传 None）
            assert node._heartbeat_interval is None
            assert node._failure_timeout is None
            node.shutdown()

    def test_start_executing_passes_max_concurrent(self):
        """executing=True 时 max_concurrent_tasks 应传给 runtime。"""
        node = _Node("test", _executing=True, signing_key="test-key", max_concurrent_tasks=8)
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            call_kwargs = mock_start.call_args
            config = call_kwargs.kwargs["config"]
            assert config.max_concurrent_tasks == 8
            node.shutdown()

    def test_start_transient_passes_zero_max_concurrent(self):
        """executing=False 时 max_concurrent_tasks 应为 0。"""
        node = _Node("test", _executing=False, signing_key="test-key", max_concurrent_tasks=8)
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            call_kwargs = mock_start.call_args
            config = call_kwargs.kwargs["config"]
            assert config.max_concurrent_tasks == 0
            node.shutdown()

    def test_start_with_node_id(self):
        """node_id 参数应传递给 _RuntimeCore.start。"""
        node = _Node("test", _executing=False, signing_key="test-key", node_id="custom-id")
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            call_kwargs = mock_start.call_args
            assert call_kwargs.kwargs["node_id"] == "custom-id"
            node.shutdown()

    def test_start_with_data_dir(self):
        """data_dir 参数应传递给 config。"""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            node = _Node("test", _executing=False, signing_key="test-key", data_dir=tmpdir)
            with patch("actant._node._RuntimeCore.start") as mock_start, \
                 patch.object(node, "_start_orchestration"):
                mock_start.return_value = MagicMock()
                node.start()
                call_kwargs = mock_start.call_args
                config = call_kwargs.kwargs["config"]
                assert config.data_dir == tmpdir
                node.shutdown()


# ---------------------------------------------------------------------------
# _Node — run (mock start/shutdown)
# ---------------------------------------------------------------------------


class TestNodeRun:
    def test_run_calls_start_and_waits(self):
        """run() 应调用 start() 并等待 shutdown_event。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()  # 模拟已启动

        def fake_start():
            node._ready_event.set()

        with patch.object(node, "start", side_effect=fake_start) as mock_start, \
             patch.object(node, "shutdown"):
            # 在另一线程设置 shutdown_event 让 run() 退出
            def trigger_shutdown():
                node._shutdown_event.set()

            threading.Thread(target=trigger_shutdown, daemon=True).start()
            node.run()
            mock_start.assert_called_once()

    def test_run_with_keyboard_interrupt_calls_shutdown(self):
        """run() 中 KeyboardInterrupt 应触发 shutdown。"""
        node = _Node("test", _executing=False, signing_key="test-key")

        with patch.object(node, "start") as mock_start, \
             patch.object(node, "shutdown") as mock_shutdown:
            # 让 ready_event.wait 抛 KeyboardInterrupt
            def raise_keyboard_interrupt():
                raise KeyboardInterrupt()

            mock_start.side_effect = lambda: setattr(node, "_runtime", MagicMock())
            with patch.object(node._ready_event, "wait", side_effect=raise_keyboard_interrupt):
                node.run()
                mock_shutdown.assert_called_once()


# ---------------------------------------------------------------------------
# _Node — _start_orchestration
# ---------------------------------------------------------------------------


class TestNodeStartOrchestration:
    def test_start_orchestration_creates_loop_and_thread(self):
        """_start_orchestration 应创建事件循环和后台线程。"""
        from actant._orchestration import OrchestrationLoop

        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()

        node._start_orchestration()

        assert node._event_loop is not None
        assert node._orchestration_loop is not None
        assert isinstance(node._orchestration_loop, OrchestrationLoop)

        # 清理
        node._shutdown_orchestration()

    def test_start_orchestration_start_failure_propagates(self):
        """如果 OrchestrationLoop.start() 失败，错误应传播给主线程。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()

        with patch("actant._orchestration.OrchestrationLoop.start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = RuntimeError("start failed")
            with pytest.raises(RuntimeError, match="start failed"):
                node._start_orchestration()


# ---------------------------------------------------------------------------
# _Node — _shutdown_orchestration
# ---------------------------------------------------------------------------


class TestNodeShutdownOrchestration:
    def test_shutdown_orchestration_noop_when_not_started(self):
        """orchestration 未启动时 _shutdown_orchestration 应无操作。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        # 不应抛异常
        node._shutdown_orchestration()

    def test_shutdown_orchestration_timeout(self):
        """orchestration stop 超时应取消 future 并继续。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._start_orchestration()

        # Mock future.result 抛 TimeoutError
        with patch("concurrent.futures.Future.result", side_effect=TimeoutError):
            # 不应抛异常
            node._shutdown_orchestration()

        assert node._orchestration_loop is None
        assert node._event_loop is None

    def test_shutdown_orchestration_future_exception_swallowed(self):
        """future 抛异常应被吞掉。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._start_orchestration()

        # Mock future.result 抛 Exception
        with patch("concurrent.futures.Future.result", side_effect=Exception("test")):
            # 不应抛异常
            node._shutdown_orchestration()

        assert node._orchestration_loop is None
        assert node._event_loop is None


# ---------------------------------------------------------------------------
# _Node — _submit_dag 完整路径
# ---------------------------------------------------------------------------


class TestNodeSubmitDagComplete:
    def test_submit_dag_with_remote_routing(self):
        """任务无法本地执行时应路由到远程节点。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0  # 本地无容量
        node._runtime.running_task_count.return_value = 0
        node._runtime.submit_dag.return_value = MagicMock()

        # 添加远程 peer
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")

        mock_node = MagicMock()
        mock_node.name = "mytask"
        node._submit_dag([mock_node], [(0, 1, None)])

        # 应调用 submit_dag
        node._runtime.submit_dag.assert_called_once()
        call_args = node._runtime.submit_dag.call_args
        # 应有 target_nodes_by_idx 和 target_endpoint_addrs_by_idx
        assert call_args.args[4] is not None  # target_nodes_by_idx
        assert call_args.args[5] is not None  # target_endpoint_addrs_by_idx

    def test_submit_dag_with_tags_and_priority(self):
        """任务带 tags 和 priority 应正确路由。"""
        from actant.task import Task

        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        node._runtime.submit_dag.return_value = MagicMock()

        # 添加带 tags 和 priority 的任务
        task = Task("tagged_task", lambda: None, _tags=["gpu"], _priority=1)
        node._tasks["tagged_task"] = task

        mock_node = MagicMock()
        mock_node.name = "tagged_task"
        node._submit_dag([mock_node], [])

        node._runtime.submit_dag.assert_called_once()

    def test_submit_dag_with_failure_strategy(self):
        """failure_strategy 应传递给 submit_dag。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()

        node._submit_dag([], [], failure_strategy="cancel_on_failure")
        call_args = node._runtime.submit_dag.call_args
        assert call_args.args[6] == "cancel_on_failure"

    def test_submit_dag_no_condition_evaluators(self):
        """无 condition_evaluators 不应更新 _condition_evaluators。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()

        before = dict(node._condition_evaluators)
        node._submit_dag([], [], condition_evaluators=None)
        assert node._condition_evaluators == before

    def test_submit_dag_stores_condition_evaluators(self):
        """有 condition_evaluators 应更新 _condition_evaluators。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()

        evaluator = MagicMock(return_value=True)
        node._submit_dag([], [], condition_evaluators={"tag1": evaluator})
        assert "tag1" in node._condition_evaluators


# ---------------------------------------------------------------------------
# _Node — cancel / resubmit_workflow
# ---------------------------------------------------------------------------


class TestNodeCancelAndResubmit:
    def test_cancel_with_runtime(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node.cancel("wf-1")
        node._runtime.cancel_workflow.assert_called_once_with("wf-1")

    def test_resubmit_workflow_with_history(self):
        """resubmit 应使用历史记录重新提交。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0

        from actant.flow import Flow
        from actant.task import _make_task

        # 设置 submit_dag 返回带真实 workflow_id 的 mock
        call_count = [0]
        def make_result(*args, **kwargs):
            call_count[0] += 1
            mock_result = MagicMock()
            mock_result.workflow_id = f"wf-{call_count[0]}"
            return mock_result
        node._runtime.submit_dag.side_effect = make_result

        def t():
            return 1

        t_task = _make_task(t, name="t_for_resubmit")

        @Flow
        def my_flow():
            return t_task()

        # 第一次提交
        result = node.submit(my_flow)
        wf_id = result.workflow_id

        # 重新提交
        new_result = node.resubmit_workflow(wf_id)
        assert new_result.workflow_id != wf_id
        assert new_result.workflow_id in node._submission_history


# ---------------------------------------------------------------------------
# _Node — connect / add_gossip_peer / listen_addresses (with event loop)
# ---------------------------------------------------------------------------


class TestNodeNetworkingWithLoop:
    def test_connect_with_event_loop(self):
        """connect 应通过 event loop 调用 runtime.dial。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.dial = AsyncMock()

        # 使用真实事件循环
        loop = asyncio.new_event_loop()
        node._event_loop = loop
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        try:
            node.connect("addr-1")
            node._runtime.dial.assert_called_once_with("addr-1")
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)

    def test_add_gossip_peer_with_event_loop(self):
        """add_gossip_peer 应通过 event loop 调用 runtime._add_gossip_peer。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime._add_gossip_peer = AsyncMock()

        loop = asyncio.new_event_loop()
        node._event_loop = loop
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        try:
            node.add_gossip_peer("peer-1")
            node._runtime._add_gossip_peer.assert_called_once_with("peer-1")
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)

    def test_listen_addresses_with_event_loop(self):
        """listen_addresses 应通过 event loop 调用 runtime.listen_addresses。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.listen_addresses = AsyncMock(return_value={
            "endpoint_id": "ep-1",
            "relay_url": "relay://example.com",
            "direct_addrs": ["/ip4/127.0.0.1/tcp/8080"],
        })

        loop = asyncio.new_event_loop()
        node._event_loop = loop
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        try:
            result = node.listen_addresses()
            assert result["endpoint_id"] == "ep-1"
            assert result["relay_url"] == "relay://example.com"
            assert len(result["direct_addrs"]) == 1
        finally:
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)


# ---------------------------------------------------------------------------
# _Node — drain 完整路径
# ---------------------------------------------------------------------------


class TestNodeDrainComplete:
    def test_drain_calls_runtime_drain(self):
        """drain 应调用 runtime.drain。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        node.drain()
        node._runtime.drain.assert_called_once()

    def test_drain_clears_drained_event(self):
        """drain 应先 clear drained_event。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        node._drained_event.set()  # 先 set
        node.drain()
        # 应返回 True（无运行任务）
        assert node.drain() is True


# ---------------------------------------------------------------------------
# _Node — shutdown 完整路径
# ---------------------------------------------------------------------------


class TestNodeShutdownComplete:
    def test_shutdown_with_orchestration_loop(self):
        """shutdown 应关闭 orchestration loop。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0

        # Mock orchestration loop
        node._start_orchestration()
        node.shutdown()
        assert node._orchestration_loop is None
        assert node._event_loop is None

    def test_shutdown_timeout_with_running_tasks(self):
        """shutdown 超时后应强制关闭。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        runtime = MagicMock()
        node._runtime = runtime
        # 模拟任务一直运行
        runtime.running_task_count.return_value = 5
        node.shutdown(timeout=0.1)
        # 应调用 runtime.shutdown
        runtime.shutdown.assert_called_once()
        assert node._runtime is None

    def test_shutdown_with_no_running_tasks(self):
        """shutdown 时无运行任务应直接关闭。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        runtime = MagicMock()
        node._runtime = runtime
        runtime.running_task_count.return_value = 0
        node.shutdown()
        runtime.shutdown.assert_called_once()
        assert node._runtime is None

    def test_shutdown_sets_shutdown_event(self):
        """shutdown 应设置 _shutdown_event。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        node.shutdown()
        assert node._shutdown_event.is_set()

    def test_shutdown_calls_gc_collect(self):
        """shutdown 应调用 gc.collect 释放 Rust Arc 引用。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        with patch("gc.collect") as mock_gc:
            node.shutdown()
            mock_gc.assert_called_once()


# ---------------------------------------------------------------------------
# _Node — metrics server 完整路径
# ---------------------------------------------------------------------------


class TestNodeMetricsServerComplete:
    def test_metrics_server_health_endpoint(self):
        """metrics server 应响应 /health 返回 JSON。"""
        import http.client
        import json as json_mod

        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "node-1"
        node._runtime.get_health_info.return_value = ("Healthy", 3)
        node._runtime.prometheus_text.return_value = "# metrics"

        port = node.start_metrics_server(port=0)
        try:
            # 测试 /health 端点
            conn = http.client.HTTPConnection("127.0.0.1", port)
            conn.request("GET", "/health")
            response = conn.getresponse()
            assert response.status == 200
            body = response.read().decode()
            data = json_mod.loads(body)
            assert data["node_id"] == "node-1"
            assert data["status"] == "Healthy"
            assert data["peers"] == 3
            conn.close()

            # 测试 /metrics 端点（默认路径）
            conn = http.client.HTTPConnection("127.0.0.1", port)
            conn.request("GET", "/metrics")
            response = conn.getresponse()
            assert response.status == 200
            body = response.read().decode()
            assert body == "# metrics"
            conn.close()
        finally:
            node.stop_metrics_server()

    def test_metrics_server_reuse_address(self):
        """_ReuseHTTPServer 应允许地址重用。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "node-1"
        node._runtime.get_health_info.return_value = ("Healthy", 0)
        node._runtime.prometheus_text.return_value = ""

        node.start_metrics_server(port=0)
        try:
            # 验证服务器类型
            server = node._metrics_server
            assert hasattr(server, "allow_reuse_address")
            assert server.allow_reuse_address is True
        finally:
            node.stop_metrics_server()


# ---------------------------------------------------------------------------
# _Node — _route_refs 完整路径
# ---------------------------------------------------------------------------


class TestNodeBuildRouteMapComplete:
    def test_build_route_map_local_can_execute_skips_routing(self):
        """本地能执行的任务应跳过路由。"""
        from actant.task import Task

        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0

        task = Task("local_task", lambda: None)
        node._tasks["local_task"] = task

        info = _RouteInfo(node_key="0", task_name="local_task", tags=[], priority=None)
        target = node._build_route_map([info])
        assert target is None

    def test_build_route_map_unknown_task_routes_to_remote(self):
        """本地容量为 0 且任务未注册时，路由器应选远程节点。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0
        node._runtime.running_task_count.return_value = 0
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")

        info = _RouteInfo(node_key="0", task_name="unknown_task", tags=[], priority=None)
        target = node._build_route_map([info])
        assert target is not None
        assert "0" in target
        assert target["0"] == "remote-1"

    def test_build_route_map_local_capacity_zero_does_not_route_local(self):
        """本地容量为 0 时不会路由到本地节点。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0
        node._runtime.running_task_count.return_value = 0
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")

        info = _RouteInfo(node_key="0", task_name="anytask", tags=[], priority=None)
        target = node._build_route_map([info])
        assert target is None or target.get("0") != "local"

    def test_build_route_map_target_equals_local_node_skips(self):
        """路由器返回本地节点时应跳过（不加入 target_nodes）。"""
        from actant.router import TaskRouter

        class AlwaysLocalRouter(TaskRouter):
            def route(self, local_node, node_key, task_meta, peer_capacities):
                return local_node

        node = _Node("test", _executing=False, signing_key="test-key", router=AlwaysLocalRouter())
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0
        node._runtime.running_task_count.return_value = 0
        node.capacity_provider.update("remote-1", 5, 10)

        info = _RouteInfo(node_key="0", task_name="anytask", tags=[], priority=None)
        target = node._build_route_map([info])
        assert target is None


# ---------------------------------------------------------------------------
# _Node — create_actor 完整路径
# ---------------------------------------------------------------------------


class TestNodeCreateActorComplete:
    def test_create_actor_registers_in_actors_dict(self):
        """create_actor 应将 actor 注册到 _actors 字典。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()

        class MyActor:
            def __init__(self):
                self.value = 0

        actor = node.create_actor(MyActor)
        assert "MyActor" in node._actors
        assert node._actors["MyActor"] is actor

    def test_create_actor_with_existing_actor_class(self):
        """create_actor 传入已注册的 Actor 应复用。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()

        class MyActor:
            def __init__(self):
                self.value = 0

        # 先注册
        registered = node.actor(MyActor)
        # 再创建
        actor = node.create_actor(registered)
        assert actor is registered
        # 应只创建一次实例
        node._runtime.create_actor.assert_called_once()

    def test_create_supervisor_appends_to_supervisors(self):
        """create_supervisor 应将 supervisor 加入列表。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.actor_core.return_value = MagicMock()
        sup1 = node.create_supervisor()
        sup2 = node.create_supervisor()
        assert sup1 in node._supervisors
        assert sup2 in node._supervisors
        assert len(node._supervisors) == 2


# ---------------------------------------------------------------------------
# _Node — get_workflow_status 完整路径
# ---------------------------------------------------------------------------


class TestNodeGetWorkflowStatusComplete:
    def test_get_workflow_status_completed(self):
        """Completed 状态应正确返回。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Completed"
        node._runtime.task_states.return_value = [("task-1", "Completed"), ("task-2", "Completed")]
        status = node.get_workflow_status("wf-1")
        assert status["state"] == "Completed"
        assert status["result_count"] == 2
        assert status["error"] is None

    def test_get_workflow_status_failed_with_first_error(self):
        """Failed 状态应返回第一个错误。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = []
        from actant._serialization import dumps

        error1 = ValueError("first error")
        error2 = RuntimeError("second error")
        node._runtime.get_stored_results.return_value = [[dumps(error1)], [dumps(error2)]]
        status = node.get_workflow_status("wf-1")
        assert "first error" in status["error"]

    def test_get_workflow_status_failed_breaks_after_error(self):
        """找到 error 后应 break 退出循环。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = []
        from actant._serialization import dumps

        error = RuntimeError("the error")
        # 第一个 result_list 中无 Exception，第二个有
        node._runtime.get_stored_results.return_value = [[dumps("not-error")], [dumps(error)]]
        status = node.get_workflow_status("wf-1")
        assert "the error" in status["error"]


# ---------------------------------------------------------------------------
# _Node — get_stored_results 完整路径
# ---------------------------------------------------------------------------


class TestNodeGetStoredResultsComplete:
    def test_get_stored_results_empty_list(self):
        """空结果列表应返回空列表。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.get_stored_results.return_value = []
        assert node.get_stored_results("wf-1") == []

    def test_get_stored_results_multiple(self):
        """多个结果应按顺序返回。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        from actant._serialization import dumps

        node._runtime.get_stored_results.return_value = [dumps(1), dumps(2), dumps(3)]
        assert node.get_stored_results("wf-1") == [1, 2, 3]


# ---------------------------------------------------------------------------
# _Node — _submit_dag
# ---------------------------------------------------------------------------


class TestNodeSubmitDag:
    def test_submit_dag_no_runtime_raises(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError, match="not running"):
            node._submit_dag([], [])

    def test_submit_dag_empty_nodes(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()
        node._submit_dag([], [])
        node._runtime.submit_dag.assert_called_once()

    def test_submit_dag_with_nodes(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = 0
        node._runtime.submit_dag.return_value = MagicMock()

        # 构造一个 mock node
        mock_node = MagicMock()
        mock_node.name = "mytask"
        node._submit_dag([mock_node], [(0, 1, None)])
        node._runtime.submit_dag.assert_called_once()

    def test_submit_dag_with_condition_evaluators(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()

        evaluator = MagicMock(return_value=True)
        node._submit_dag([], [], condition_evaluators={"tag1": evaluator})
        assert "tag1" in node._condition_evaluators

    def test_submit_dag_with_timeout(self):
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()

        node._submit_dag([], [], timeout=5.0)
        call_args = node._runtime.submit_dag.call_args
        # timeout_ms 应为 5000
        assert call_args.args[2] == 5000


# ---------------------------------------------------------------------------
# _dispatch_generic_task 完整路径
# ---------------------------------------------------------------------------


class TestDispatchGenericTaskComplete:
    def test_generic_tag_payload(self):
        """TAG_GENERIC 内层 payload 应正确执行。"""
        def fn():
            return 42
        inner = struct.pack("B", TAG_GENERIC) + dumps((fn, (), {}))
        result = _dispatch_generic_task(inner)
        assert loads(result) == 42

    def test_positional_tag_payload(self):
        """TAG_POSITIONAL 内层 payload 应正确执行。"""
        def fn(x, y):
            return x + y
        # TAG_POSITIONAL 格式: (inline_fn, positions, kwargs_keys, concrete_args, concrete_kwargs)
        # positions=[0,1] 表示参数 0 和 1 从 upstream 取
        inner = struct.pack("B", TAG_POSITIONAL) + dumps((fn, [0, 1], [], (), {}))
        payload = _pack_upstream_prefix([3, 4], inner)
        result = _dispatch_generic_task(payload)
        assert loads(result) == 7

    def test_upstream_prefix_with_generic(self):
        """TAG_UPSTREAM_PREFIX 包装的 TAG_GENERIC payload 应正确执行。"""
        def fn():
            return 99
        inner = struct.pack("B", TAG_GENERIC) + dumps((fn, (), {}))
        payload = _pack_upstream_prefix([1, 2], inner)
        result = _dispatch_generic_task(payload)
        assert loads(result) == 99

    def test_upstream_prefix_with_positional(self):
        """TAG_UPSTREAM_PREFIX 包装的 TAG_POSITIONAL payload 应正确执行。"""
        def fn(x):
            return x * 2
        inner = struct.pack("B", TAG_POSITIONAL) + dumps((fn, [0], [], (), {}))
        payload = _pack_upstream_prefix([5], inner)
        result = _dispatch_generic_task(payload)
        assert loads(result) == 10

    def test_named_tag_falls_back_raises_serialization_error(self):
        """内层 TAG_SINGLE 应引发 SerializationError。"""
        inner = struct.pack("B", TAG_SINGLE) + b"\x00\x00"
        with pytest.raises(SerializationError):
            _dispatch_generic_task(inner)

    def test_upstream_prefix_with_named_tag_raises(self):
        """TAG_UPSTREAM_PREFIX 包装的 TAG_SINGLE 应引发 SerializationError。"""
        inner = struct.pack("B", TAG_SINGLE) + b"\x00\x00"
        payload = _pack_upstream_prefix([1], inner)
        with pytest.raises(SerializationError):
            _dispatch_generic_task(payload)


# ---------------------------------------------------------------------------
# _Node — context manager + on() decorator
# ---------------------------------------------------------------------------


class TestNodeContextManager:
    def test_enter_calls_start(self):
        """__enter__ 应调用 start。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with patch.object(node, "start") as mock_start:
            result = node.__enter__()
            mock_start.assert_called_once()
            assert result is node

    def test_exit_calls_shutdown(self):
        """__exit__ 应调用 shutdown。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with patch.object(node, "shutdown") as mock_shutdown:
            node.__exit__(None, None, None)
            mock_shutdown.assert_called_once()


class TestNodeOnDecorator:
    def test_on_without_handler_returns_decorator(self):
        """on() 不传 handler 应返回装饰器。"""
        node = _Node("test", _executing=False, signing_key="test-key")

        @node.on("startup")
        def my_hook():
            pass

        assert my_hook in node._on_startup

    def test_on_custom_event(self):
        """on() 自定义事件应注册到 _custom_events。"""
        node = _Node("test", _executing=False, signing_key="test-key")

        def my_handler():
            pass

        node.on("custom_event", my_handler)
        assert my_handler in node._custom_events["custom_event"]


# ---------------------------------------------------------------------------
# _Node — event hook exception handling
# ---------------------------------------------------------------------------


class TestNodeEventHookExceptions:
    def test_startup_hook_exception_is_logged(self):
        """startup hook 异常应被捕获并日志记录。"""
        node = _Node("test", _executing=False, signing_key="test-key")

        def bad_hook():
            raise RuntimeError("boom")

        node._on_startup.append(bad_hook)
        # 不应抛出异常
        node._fire_startup()

    def test_shutdown_hook_exception_is_logged(self):
        """shutdown hook 异常应被捕获并日志记录。"""
        node = _Node("test", _executing=False, signing_key="test-key")

        def bad_hook():
            raise RuntimeError("boom")

        node._on_shutdown.append(bad_hook)
        node._fire_shutdown()

    @pytest.mark.asyncio
    async def test_task_start_hook_exception_is_logged(self):
        """task_start hook 异常应被捕获并日志记录。"""
        from types import SimpleNamespace

        from actant._events import clear as _clear_events
        from actant._events import dispatch as _dispatch

        _clear_events("task.started")
        node = _Node("test", _executing=False, signing_key="test-key")

        def bad_hook(event):
            raise RuntimeError("boom")

        node.on("task_start", bad_hook)
        # 异常应被 dispatch 内部捕获，不抛出
        await _dispatch("task.started", SimpleNamespace(workflow_id="wf-1", task_name="task-1"))
        _clear_events("task.started")

    @pytest.mark.asyncio
    async def test_task_complete_hook_exception_is_logged(self):
        """task_complete hook 异常应被捕获并日志记录。"""
        from types import SimpleNamespace

        from actant._events import clear as _clear_events
        from actant._events import dispatch as _dispatch

        _clear_events("task.failed")
        node = _Node("test", _executing=False, signing_key="test-key")

        def bad_hook(event):
            raise RuntimeError("boom")

        node.on("task_complete", bad_hook)
        # 异常应被 dispatch 内部捕获，不抛出
        await _dispatch(
            "task.failed", SimpleNamespace(workflow_id="wf-1", task_name="task-1", state="Failed")
        )
        _clear_events("task.failed")

    def test_emit_custom_event_exception_is_logged(self):
        """emit 自定义事件异常应被捕获并日志记录。"""
        node = _Node("test", _executing=False, signing_key="test-key")

        def bad_handler():
            raise RuntimeError("boom")

        node._custom_events["my_event"] = [bad_handler]
        node.emit("my_event")  # 不应抛出


# ---------------------------------------------------------------------------
# _Node — property None 返回路径
# ---------------------------------------------------------------------------


class TestNodePropertyNonePaths:
    def test_available_capacity_runtime_none(self):
        """_runtime 为 None 时 available_capacity 返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.available_capacity == 0

    def test_available_capacity_max_none(self):
        """max_concurrent_tasks 返回 None 时 available_capacity 返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = None
        node._runtime.running_task_count.return_value = 0
        assert node.available_capacity == 0

    def test_available_capacity_running_none(self):
        """running_task_count 返回 None 时 available_capacity 返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = 10
        node._runtime.running_task_count.return_value = None
        assert node.available_capacity == 0

    def test_max_capacity_runtime_none(self):
        """_runtime 为 None 时 max_capacity 返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.max_capacity == 0

    def test_max_capacity_returns_none(self):
        """max_concurrent_tasks 返回 None 时 max_capacity 返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.max_concurrent_tasks.return_value = None
        assert node.max_capacity == 0

    def test_running_task_count_runtime_none(self):
        """_runtime 为 None 时 running_task_count 返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        assert node.running_task_count == 0

    def test_running_task_count_returns_none(self):
        """running_task_count 属性返回 None 时返回 0。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = None
        assert node.running_task_count == 0


# ---------------------------------------------------------------------------
# _Node — _route_endpoint_addrs 完整路径
# ---------------------------------------------------------------------------


class TestNodeRouteEndpointAddrs:
    def test_route_endpoint_addrs_with_matching_node(self):
        """有匹配节点时返回 endpoint_addr。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")
        node.capacity_provider.update("remote-2", 3, 10, endpoint_addr="ep-2")
        target_nodes = {"0": "remote-1"}
        result = node._route_endpoint_addrs(target_nodes)
        assert result == {"0": "ep-1"}

    def test_route_endpoint_addrs_no_endpoint(self):
        """节点无 endpoint_addr 时不包含在结果中。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node.capacity_provider.update("remote-1", 5, 10)
        target_nodes = {"0": "remote-1"}
        result = node._route_endpoint_addrs(target_nodes)
        assert result is None

    def test_route_endpoint_addrs_empty_target(self):
        """target_nodes 为空时返回 None。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        result = node._route_endpoint_addrs({})
        assert result is None


# ---------------------------------------------------------------------------
# _Node — actor() TypeError + create_actor 边界
# ---------------------------------------------------------------------------


class TestNodeActorEdgeCases:
    def test_actor_rejects_non_class(self):
        """actor() 传入非类对象应抛出 TypeError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(TypeError, match="cls must be a Python class"):
            node.actor("not_a_class")

    def test_create_actor_rejects_invalid_type(self):
        """create_actor() 传入无效类型应抛出 TypeError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        with pytest.raises(TypeError, match="expected Actor or class"):
            node.create_actor(42)

    def test_create_actor_class_with_no_cls_raises(self):
        """create_actor() 的 Actor 没有 cls 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        from actant.actor import Actor
        actor = Actor("test_actor", None)  # cls=None
        with pytest.raises(InvalidStateError, match="no class"):
            node.create_actor(actor)


# ---------------------------------------------------------------------------
# _Node — cancel_task RuntimeError + resubmit KeyError + start already started
# ---------------------------------------------------------------------------


class TestNodeEdgeCaseErrors:
    def test_cancel_task_runtime_error_returns_false(self):
        """cancel_task 遇到 RuntimeError 应返回 False。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.cancel_task.side_effect = RuntimeError("not found")
        assert node.cancel_task("wf-1", "task-1") is False

    def test_cancel_task_not_found_error_propagates(self):
        """P1-D3：cancel_task 遇到 NotFoundError 应传播而非静默返回 False。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.cancel_task.side_effect = NotFoundError("task not found")
        with pytest.raises(NotFoundError):
            node.cancel_task("wf-1", "task-1")

    def test_resubmit_workflow_not_found_raises_keyerror(self):
        """resubmit_workflow 找不到历史记录应抛出 KeyError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(KeyError, match="not found"):
            node.resubmit_workflow("nonexistent-wf")

    def test_start_already_started_returns_immediately(self):
        """start() 在已启动时应立即返回。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()  # 模拟已启动
        # 不应调用 _RuntimeCore.start
        node.start()
        # _runtime 仍是原来的 MagicMock，未被替换
        assert isinstance(node._runtime, MagicMock)


# ---------------------------------------------------------------------------
# _Node — _submit_dag 条件评估器分支
# ---------------------------------------------------------------------------


class TestNodeSubmitDagConditionEvaluators:
    def test_submit_dag_stores_condition_evaluators(self):
        """_submit_dag 有 condition_evaluators 时应存储到 _condition_evaluators。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()

        evals = {"edge_0_1": lambda x: x > 0}
        node._submit_dag([], [], condition_evaluators=evals)
        assert "edge_0_1" in node._condition_evaluators

    def test_submit_dag_no_condition_evaluators_skips(self):
        """_submit_dag 无 condition_evaluators 时不应修改 _condition_evaluators。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.submit_dag.return_value = MagicMock()
        node._condition_evaluators.clear()

        node._submit_dag([], [], condition_evaluators=None)
        assert len(node._condition_evaluators) == 0


# ---------------------------------------------------------------------------
# _Node — start with _executing=False (no funcs registered)
# ---------------------------------------------------------------------------


class TestNodeStartNoExecuting:
    def test_start_non_executing_no_funcs(self):
        """_executing=False 时不注册任何任务函数（funcs 为空）。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            call_kwargs = mock_start.call_args
            tasks = call_kwargs.kwargs["tasks"]
            assert len(tasks) == 0  # _executing=False 时不注册任何函数
            node.shutdown()


# ---------------------------------------------------------------------------
# _Node — _shutdown_orchestration 异常路径
# ---------------------------------------------------------------------------


class TestNodeShutdownOrchestrationErrors:
    def test_shutdown_orchestration_timeout(self):
        """_shutdown_orchestration 超时应取消 future。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0

        from actant._orchestration import OrchestrationLoop
        mock_loop = MagicMock(spec=OrchestrationLoop)
        mock_ev = MagicMock()

        node._orchestration_loop = mock_loop
        node._event_loop = mock_ev

        # run_coroutine_threadsafe 返回超时的 future
        from concurrent.futures import Future
        future = Future()
        mock_ev.call_soon_threadsafe = MagicMock()

        with patch("actant._node.asyncio.run_coroutine_threadsafe", return_value=future):
            # future 永远不完成，模拟超时
            def raise_timeout():
                raise TimeoutError()
            future.result = raise_timeout
            node._shutdown_orchestration()

        assert node._orchestration_loop is None
        assert node._event_loop is None

    def test_shutdown_orchestration_generic_exception(self):
        """_shutdown_orchestration 一般异常应被捕获。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0

        mock_loop = MagicMock()
        mock_ev = MagicMock()
        node._orchestration_loop = mock_loop
        node._event_loop = mock_ev

        from concurrent.futures import Future
        future = Future()
        future.set_exception(RuntimeError("test"))
        mock_ev.call_soon_threadsafe = MagicMock()

        with patch("actant._node.asyncio.run_coroutine_threadsafe", return_value=future):
            node._shutdown_orchestration()

        assert node._orchestration_loop is None


# ---------------------------------------------------------------------------
# _Node — drain 完整路径
# ---------------------------------------------------------------------------


class TestNodeDrainWait:
    def test_drain_with_running_tasks(self):
        """drain 时有运行中任务应等待 drained_event。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 1

        # 在后台线程触发 drained_event
        def set_drained():
            import time
            time.sleep(0.05)
            node._drained_event.set()

        threading.Thread(target=set_drained, daemon=True).start()
        result = node.drain(timeout=5)
        assert result is True

    def test_drain_no_running_tasks(self):
        """drain 时无运行中任务应立即返回 True。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        result = node.drain()
        assert result is True


# ---------------------------------------------------------------------------
# _Node — get_workflow_status 完整路径
# ---------------------------------------------------------------------------


class TestNodeGetWorkflowStatusNotFound:
    def test_get_workflow_status_not_found(self):
        """workflow 不存在时应抛出 NotFoundError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = None
        with pytest.raises(NotFoundError):
            node.get_workflow_status("nonexistent")

    def test_get_workflow_status_failed_with_error(self):
        """Failed 状态的 workflow 应尝试提取错误信息。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = [("t1", "Failed")]
        # 模拟存储结果包含异常
        error = RuntimeError("task crashed")
        node._runtime.get_stored_results.return_value = [[dumps(error)]]

        status = node.get_workflow_status("wf-1")
        assert status["state"] == "Failed"
        assert status["error"] is not None
        assert "task crashed" in status["error"]

    def test_get_workflow_status_failed_no_error(self):
        """Failed 状态但无存储结果的 workflow。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = [("t1", "Failed")]
        node._runtime.get_stored_results.return_value = None

        status = node.get_workflow_status("wf-1")
        assert status["state"] == "Failed"
        assert status["error"] is None

    def test_get_workflow_status_completed(self):
        """Completed 状态的 workflow。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Completed"
        node._runtime.task_states.return_value = [("t1", "Completed"), ("t2", "Completed")]

        status = node.get_workflow_status("wf-1")
        assert status["state"] == "Completed"
        assert status["result_count"] == 2

    def test_get_workflow_status_result_deserialize_error(self):
        """结果反序列化失败不应影响 status 返回。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.workflow_state.return_value = "Failed"
        node._runtime.task_states.return_value = [("t1", "Failed")]
        node._runtime.get_stored_results.return_value = [[b"invalid_bytes"]]

        status = node.get_workflow_status("wf-1")
        assert status["error"] is None  # 反序列化失败被忽略


# ---------------------------------------------------------------------------
# _Node — connect / add_gossip_peer / listen_addresses 边界
# ---------------------------------------------------------------------------


class TestNodeNetworkingEdgeCases:
    def test_connect_no_runtime_raises(self):
        """connect 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.connect("addr")

    def test_connect_no_event_loop_raises(self):
        """connect 无 event loop 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        with pytest.raises(InvalidStateError, match="event loop"):
            node.connect("addr")

    def test_add_gossip_peer_no_runtime_raises(self):
        """add_gossip_peer 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.add_gossip_peer("peer-1")

    def test_add_gossip_peer_no_event_loop_raises(self):
        """add_gossip_peer 无 event loop 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        with pytest.raises(InvalidStateError, match="event loop"):
            node.add_gossip_peer("peer-1")

    def test_listen_addresses_no_runtime_raises(self):
        """listen_addresses 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.listen_addresses()

    def test_listen_addresses_no_event_loop_returns_empty(self):
        """listen_addresses 无 event loop 应返回空结构。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        result = node.listen_addresses()
        assert result["endpoint_id"] == ""
        assert result["direct_addrs"] == []


# ---------------------------------------------------------------------------
# _Node — __repr__ + misc properties
# ---------------------------------------------------------------------------


class TestNodeReprAndMisc:
    def test_repr(self):
        """__repr__ 应返回 <_Node name>。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        assert repr(node) == "<_Node test>"

    def test_node_id_no_runtime_raises(self):
        """node_id 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            _ = node.node_id

    def test_capacity_no_runtime_raises(self):
        """capacity 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            _ = node.capacity

    def test_list_actors_no_runtime_raises(self):
        """list_actors 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.list_actors()

    def test_list_workflows_no_runtime_raises(self):
        """list_workflows 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.list_workflows()

    def test_workflow_state_no_runtime_raises(self):
        """workflow_state 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.workflow_state("wf-1")

    def test_task_states_no_runtime_raises(self):
        """task_states 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.task_states("wf-1")

    def test_stop_actor_no_runtime_raises(self):
        """stop_actor 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.stop_actor("actor-1")

    def test_actor_status_no_runtime_raises(self):
        """actor_status 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.actor_status("actor-1")

    def test_metrics_prometheus_no_runtime_raises(self):
        """metrics_prometheus 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.metrics_prometheus()

    def test_start_metrics_server_no_runtime_raises(self):
        """start_metrics_server 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.start_metrics_server()

    def test_drain_no_runtime_raises(self):
        """drain 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.drain()

    def test_cancel_no_runtime_raises(self):
        """cancel 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.cancel("wf-1")

    def test_cancel_task_no_runtime_raises(self):
        """cancel_task 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.cancel_task("wf-1", "task-1")

    def test_get_stored_results_no_runtime_raises(self):
        """get_stored_results 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node.get_stored_results("wf-1")

    def test_submit_no_runtime_raises(self):
        """submit 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        from actant.flow import Flow
        from actant.task import _make_task

        def t():
            return 1

        t_task = _make_task(t, name="t_submit_nort")
        @Flow
        def f():
            return t_task()

        with pytest.raises(InvalidStateError):
            node.submit(f)

    def test_submit_dag_no_runtime_raises(self):
        """_submit_dag 无 runtime 应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        with pytest.raises(InvalidStateError):
            node._submit_dag([], [])

    def test_capabilities_property(self):
        """capabilities 应返回 _capabilities。"""
        node = _Node("test", _executing=False, signing_key="test-key", capabilities={"gpu": True})
        assert node.capabilities == {"gpu": True}

    def test_task_names_property(self):
        """task_names 应返回任务名列表。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        from actant.task import Task
        t = Task("my_task", lambda: None)
        node._tasks["my_task"] = t
        assert "my_task" in node.task_names


# ---------------------------------------------------------------------------
# _Node — 剩余分支覆盖（98% → 100%）
# ---------------------------------------------------------------------------


class TestNodeBranchCoverage:
    def test_create_actor_class_already_registered(self):
        """create_actor(class) 时类已注册应复用已有 Actor。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.create_actor.return_value = "actor-id-1"

        class MyCounter:
            pass

        # 先注册
        node.actor(MyCounter)
        # 再 create_actor —— 应复用已有注册
        result = node.create_actor(MyCounter)
        assert result.name == "MyCounter"

    def test_submit_dag_with_endpoint_addr_routing(self):
        """_submit_dag 远程路由时应填充 target_endpoint_addrs_by_idx。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0  # 本地无容量
        node._runtime.running_task_count.return_value = 0
        node._runtime.submit_dag.return_value = MagicMock()

        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")

        from actant.task import Task
        task = Task("my_task", lambda: None, _tags=["gpu"], _priority=1)
        node._tasks["my_task"] = task

        mock_node = MagicMock()
        mock_node.name = "my_task"
        node._submit_dag([mock_node], [(0, 0, None)])

        call_args = node._runtime.submit_dag.call_args
        # target_endpoint_addrs_by_idx 应非 None
        assert call_args.args[5] is not None
        assert 0 in call_args.args[5]

    def test_start_merges_global_tasks_skips_existing(self):
        """start() 合并全局任务时应跳过已注册的。"""
        node = _Node("test", _executing=True, signing_key="test-key")

        from actant.task import Task, register_global_task
        existing_task = Task("existing_task", lambda: 1)
        node._tasks["existing_task"] = existing_task

        global_task = Task("existing_task", lambda: 2)  # 同名
        register_global_task(global_task)

        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            # 已有的任务不应被覆盖
            assert node._tasks["existing_task"] is existing_task
            node.shutdown()

    def test_start_skips_task_with_no_func(self):
        """start() 中 func=None 的任务不应注册到 funcs。"""
        node = _Node("test", _executing=True, signing_key="test-key")

        from actant.task import Task
        task_with_func = Task("has_func", lambda: 1)
        task_no_func = Task("no_func", None)
        node._tasks["has_func"] = task_with_func
        node._tasks["no_func"] = task_no_func

        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            tasks = mock_start.call_args.kwargs["tasks"]
            assert "has_func" in tasks
            assert "no_func" not in tasks
            assert "__actant_generic__" in tasks
            node.shutdown()

    def test_start_orchestration_loop_none_returns(self):
        """_start_orchestration 中 loop 为 None 时应设置 ready_event 并返回。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0
        # 强制 event_loop 为 None 但 orchestration_loop 已创建
        from actant._orchestration import OrchestrationLoop
        node._orchestration_loop = MagicMock(spec=OrchestrationLoop)
        node._event_loop = None

        # 直接调用 _start_orchestration 的内部线程函数
        # 模拟 loop is None 的情况

        # 直接测试 _run_with_start 内 loop=None 的分支
        # 通过 mock new_event_loop 返回 None
        with patch("actant._node.asyncio.new_event_loop", return_value=None):
            # _start_orchestration 应不会卡住
            node._orchestration_loop = None
            node._event_loop = None
            node._start_orchestration()
            # loop is None → ready_event 被 set，orchestration_loop 仍为 None
            assert node._orchestration_loop is None or node._event_loop is None

    def test_start_orchestration_timeout_raises(self):
        """_start_orchestration 超时未启动应抛出 InvalidStateError。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0

        # Mock OrchestrationLoop 构造函数让 ready_event 永远不被设置
        with patch("actant._node.asyncio.new_event_loop") as mock_loop:
            mock_ev = MagicMock()
            mock_ev.run_until_complete = MagicMock()
            mock_ev.run_forever = MagicMock()
            mock_loop.return_value = mock_ev

            # 让 ready_event.wait 返回 False（超时）
            with (
                patch.object(threading.Event, "wait", return_value=False),
                pytest.raises(InvalidStateError, match="orchestration loop failed"),
            ):
                node._start_orchestration()

    def test_run_keyboard_interrupt(self):
        """run() 中 KeyboardInterrupt 应调用 shutdown。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()

        call_count = [0]
        def fake_start():
            call_count[0] += 1
            node._ready_event.set()

        with patch.object(node, "start", side_effect=fake_start), \
             patch.object(node, "shutdown") as mock_shutdown:
            # 让 _shutdown_event.wait 抛出 KeyboardInterrupt
            def interrupting_wait(timeout=None):
                raise KeyboardInterrupt()
            node._shutdown_event.wait = interrupting_wait
            node.run()
            mock_shutdown.assert_called_once()

    def test_shutdown_orchestration_runtime_error_handler(self):
        """_shutdown_orchestration 中 loop.close() 的 RuntimeError 应被捕获。"""
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0

        from actant._orchestration import OrchestrationLoop

        # 使用真实的 asyncio event loop 替代 MagicMock
        real_ev = asyncio.new_event_loop()

        mock_loop = MagicMock(spec=OrchestrationLoop)
        mock_loop.stop = AsyncMock()

        node._orchestration_loop = mock_loop
        node._event_loop = real_ev

        # 在另一个线程运行真实 event loop，以便 run_coroutine_threadsafe 工作
        import threading

        def run_loop():
            real_ev.run_forever()

        t = threading.Thread(target=run_loop, daemon=True)
        t.start()

        try:
            node._shutdown_orchestration()
            assert node._orchestration_loop is None
        finally:
            # 确保清理 event loop
            real_ev.call_soon_threadsafe(real_ev.stop)
            t.join(timeout=2.0)
            real_ev.close()


class TestNodeFullBranchCoverage:
    """针对覆盖率报告中 5 个缺失行/分支的补充测试。"""

    def test_submit_dag_multi_node_endpoint_addr_loop_continue(self):
        """_submit_dag 多节点路由时 for 循环回跳 (565->562)。

        需要 >=2 个路由节点且均有 endpoint_addr，触发 enumerate 循环
        第二次迭代，覆盖 565->562 回跳分支和 563->565 True 分支。
        """
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.node_id.return_value = "local"
        node._runtime.max_concurrent_tasks.return_value = 0
        node._runtime.running_task_count.return_value = 0
        node._runtime.submit_dag.return_value = MagicMock(workflow_id="wf-1")

        # 两个远程节点均有 endpoint
        node.capacity_provider.update("remote-1", 5, 10, endpoint_addr="ep-1")
        node.capacity_provider.update("remote-2", 3, 8, endpoint_addr="ep-2")

        from actant.task import Task

        task_a = Task("task_a", lambda: None, _tags=["gpu"], _priority=1)
        task_b = Task("task_b", lambda: None, _tags=["cpu"], _priority=2)
        node._tasks["task_a"] = task_a
        node._tasks["task_b"] = task_b

        mock_node_a = MagicMock()
        mock_node_a.name = "task_a"
        mock_node_b = MagicMock()
        mock_node_b.name = "task_b"

        node._submit_dag([mock_node_a, mock_node_b], [(0, 1, None)])

        call_args = node._runtime.submit_dag.call_args
        # target_endpoint_addrs_by_idx 应包含两个节点（循环执行了 >=2 次迭代）
        endpoint_addrs = call_args.args[5]
        assert endpoint_addrs is not None
        assert 0 in endpoint_addrs
        assert 1 in endpoint_addrs
        # 两个 entry 都有有效的 endpoint 地址
        assert endpoint_addrs[0] in ("ep-1", "ep-2")
        assert endpoint_addrs[1] in ("ep-1", "ep-2")

    def test_run_while_loop_continuation(self):
        """run() 中 while 循环继续迭代 (720->718)。

        让 _shutdown_event.wait 先返回 False（继续循环），再返回 True（退出），
        覆盖 while 循环的 continue 分支。
        """
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()

        wait_call_count = [0]

        def fake_start():
            node._ready_event.set()
            node._runtime = MagicMock()  # 保持非 None

        def counting_wait(timeout=None):
            wait_call_count[0] += 1
            if wait_call_count[0] <= 1:
                return False  # 第一次: 循环继续 (720->718)
            # 第二次: 模拟 shutdown() 设置 event
            node._runtime = None
            return True  # 退出循环

        with patch.object(node, "start", side_effect=fake_start), \
             patch.object(node, "shutdown"):
            node._shutdown_event.wait = counting_wait
            node.run()

        # 确认 wait 被调用 >= 2 次（循环至少迭代2次）
        assert wait_call_count[0] >= 2

    def test_run_keyboard_interrupt_exits(self):
        """run() 中 KeyboardInterrupt 捕获后退出 (718->exit)。

        KeyboardInterrupt 发生在 while 循环的 _shutdown_event.wait() 调用中，
        except 块调用 self.shutdown() 并退出函数。
        """
        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()

        def fake_start():
            node._ready_event.set()

        with patch.object(node, "start", side_effect=fake_start), \
             patch.object(node, "shutdown") as mock_shutdown:
            # _ready_event.wait 后在 while 循环中抛出 KeyboardInterrupt
            node._shutdown_event.wait = MagicMock(side_effect=KeyboardInterrupt())
            node.run()
            mock_shutdown.assert_called_once()

    def test_start_orchestration_pending_tasks_cancelled(self):
        """_start_orchestration 中 loop 关闭时取消 pending tasks (782-783)。

        通过 patch loop.run_forever 使其在创建 pending task 后立即停止，
        触发 _run_with_start 的 finally 块中的 pending tasks 取消逻辑。
        """
        import time

        node = _Node("test", _executing=False, signing_key="test-key")
        node._runtime = MagicMock()
        node._runtime.running_task_count.return_value = 0

        real_loop = asyncio.new_event_loop()
        loop_stopped = threading.Event()

        original_run_forever = real_loop.run_forever

        def patched_run_forever():
            """在 run_forever 中创建 pending task，然后立即停止 loop。"""
            # 创建一个 pending task
            pending_task = real_loop.create_task(asyncio.sleep(3600))
            # 安排在下一迭代停止
            real_loop.call_soon(real_loop.stop)
            # 保持引用，避免 pending task 被提前回收
            _ = pending_task
            original_run_forever()
            loop_stopped.set()

        with patch("actant._node.asyncio.new_event_loop", return_value=real_loop), \
             patch("actant._orchestration.OrchestrationLoop.start", new_callable=AsyncMock), \
             patch("actant._orchestration.OrchestrationLoop.stop", new_callable=AsyncMock), \
             patch.object(real_loop, "run_forever", side_effect=patched_run_forever):

            node._start_orchestration()

            # 等待 run_forever 完成（包括 finally 块）
            loop_stopped.wait(timeout=5)
            time.sleep(0.3)

        # 验证 loop 已关闭
        assert real_loop.is_closed()

    def test_start_dispatch_lambda_invoked(self):
        """start() 构建的 dispatch lambda 被调用 (661行)。

        start() 中为有 func 的 task 创建 lambda wrapper，
        调用该 lambda 覆盖 661 行的 lambda 体。
        """
        node = _Node("test", _executing=True, signing_key="test-key")

        from actant.task import Task

        def my_func():
            return 42

        task = Task("my_task", my_func)
        node._tasks["my_task"] = task

        with patch("actant._node._RuntimeCore.start") as mock_start, \
             patch.object(node, "_start_orchestration"):
            mock_start.return_value = MagicMock()
            node.start()
            tasks = mock_start.call_args.kwargs["tasks"]
            assert "my_task" in tasks

            # 构造一个合法的 TAG_GENERIC payload 调用 dispatch lambda
            import struct

            from cloudpickle import dumps, loads

            from actant._serialization import TAG_GENERIC
            inner = struct.pack("B", TAG_GENERIC) + dumps((my_func, (), {}))
            result_bytes = tasks["my_task"](inner)
            result = loads(result_bytes)
            assert result == 42

            node.shutdown()
