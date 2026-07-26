"""``actant._runtime`` 杂项单元测试（网络方法错误路径、repr、默认 handler）。"""
from __future__ import annotations

import pytest

from actant import Runtime
from actant._runtime import DefaultRetryPolicy, FifoScheduler, LocalRouter
from actant.capabilities import RetryCtx, RouteCtx, ScheduleCtx
from actant.exceptions import InvalidStateError
from actant.task import AsyncResult


def test_serve_before_start_raises() -> None:
    rt = Runtime()
    with pytest.raises(InvalidStateError):
        rt.serve()


def test_network_methods_before_start_raise() -> None:
    rt = Runtime()
    with pytest.raises(InvalidStateError):
        _ = rt.node_id
    with pytest.raises(InvalidStateError):
        _ = rt.peer_id
    with pytest.raises(InvalidStateError):
        rt.listen_addresses()
    with pytest.raises(InvalidStateError):
        rt.dial("addr")
    with pytest.raises(InvalidStateError):
        rt.add_gossip_peer("peer")
    with pytest.raises(InvalidStateError):
        rt.discover_peers()
    with pytest.raises(InvalidStateError):
        rt.set_max_concurrent_tasks(10)
    with pytest.raises(InvalidStateError):
        _ = rt.max_concurrent_tasks


def test_repr_started_and_stopped() -> None:
    rt = Runtime()
    assert "started=False" in repr(rt)
    with Runtime.with_defaults() as rt2:
        r = repr(rt2)
        assert "started=True" in r
        assert "node_id" in r


def test_cancel_task_after_completion() -> None:
    with Runtime.with_defaults() as rt:
        handle = AsyncResult("t1")
        handle._set_result(__import__("cloudpickle").dumps(1))
        rt.register_task("t1", handle)
        assert rt.cancel_task("t1") is False


def test_cancel_task_already_cancelled() -> None:
    with Runtime.with_defaults() as rt:
        handle = AsyncResult("t1")
        handle._set_cancelled()
        rt.register_task("t1", handle)
        assert rt.cancel_task("t1") is True


def test_local_router_empty_peers() -> None:
    router = LocalRouter()
    assert router(RouteCtx(task_name="x", peers=[], local_node="")) is None
    assert router(RouteCtx(task_name="x", peers=[], local_node="local")) == "local"


def test_local_router_round_robin() -> None:
    router = LocalRouter()
    peers = ["a", "b", "c"]
    results = {router(RouteCtx(task_name=f"t{i}", peers=peers, local_node="")) for i in range(10)}
    assert results.issubset(set(peers))


def test_fifo_scheduler() -> None:
    scheduler = FifoScheduler()
    assert scheduler(ScheduleCtx(workflow_id="wf", pending=[])) is None
    assert scheduler(ScheduleCtx(workflow_id="wf", pending=["a", "b"])) == "a"


def test_default_retry_policy() -> None:
    policy = DefaultRetryPolicy()
    assert policy(RetryCtx(task_id="t", attempt=0, last_error="", max_retries=2)) is True
    assert policy(RetryCtx(task_id="t", attempt=1, last_error="", max_retries=2)) is True
    assert policy(RetryCtx(task_id="t", attempt=2, last_error="", max_retries=2)) is None


def test_max_concurrent_tasks_adjustment() -> None:
    with Runtime.with_defaults() as rt:
        current = rt.max_concurrent_tasks
        rt.set_max_concurrent_tasks(current + 1)
        assert rt.max_concurrent_tasks == current + 1


def test_require_runtime_without_active_runtime() -> None:
    import queue
    import threading

    from actant._runtime import require_runtime

    def check(out: queue.Queue[bool]) -> None:
        try:
            require_runtime()
            out.put(False)
        except InvalidStateError:
            out.put(True)

    q: queue.Queue[bool] = queue.Queue()
    t = threading.Thread(target=check, args=(q,))
    t.start()
    t.join(timeout=5)
    assert not t.is_alive()
    assert q.get(timeout=1) is True


def test_layer_chain_rejects_non_callable() -> None:
    with Runtime.with_defaults() as rt:
        layer = rt.layer("Routing")
        with pytest.raises(TypeError):
            layer.chain("not-callable")  # type: ignore[arg-type]


def test_layer_remove_missing_handler() -> None:
    with Runtime.with_defaults() as rt:
        layer = rt.layer("Routing")
        assert layer.remove(lambda x: x) is False


def test_layer_name_and_kind() -> None:
    with Runtime.with_defaults() as rt:
        layer = rt.layer("Routing")
        assert layer.name == "Routing"
        assert str(layer.kind) == "ask"
