"""``actant._internal.runtime`` 杂项单元测试（网络方法错误路径、repr、layer 操作）。"""
from __future__ import annotations

import pytest

from actant import Runtime
from actant.exceptions import InvalidStateError
from actant.tasks import AsyncResult


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
    with Runtime() as rt2:
        r = repr(rt2)
        assert "started=True" in r
        assert "node_id" in r


def test_cancel_task_after_completion() -> None:
    with Runtime() as rt:
        handle = AsyncResult("t1")
        handle._set_result(1)
        rt.register_task("t1", handle)
        assert rt.cancel_task("t1") is False


def test_cancel_task_already_cancelled() -> None:
    with Runtime() as rt:
        handle = AsyncResult("t1")
        handle._set_cancelled()
        rt.register_task("t1", handle)
        assert rt.cancel_task("t1") is True


def test_max_concurrent_tasks_adjustment() -> None:
    with Runtime() as rt:
        current = rt.max_concurrent_tasks
        rt.set_max_concurrent_tasks(current + 1)
        assert rt.max_concurrent_tasks == current + 1


def test_layer_chain_rejects_non_callable() -> None:
    with Runtime() as rt:
        layer = rt.layer("MyCap", "ask")
        with pytest.raises(TypeError):
            layer.chain("not-callable")  # type: ignore[arg-type]


def test_layer_remove_missing_handler() -> None:
    with Runtime() as rt:
        layer = rt.layer("MyCap", "ask")
        assert layer.remove(lambda x: x) is False


def test_layer_name_and_kind() -> None:
    with Runtime() as rt:
        layer = rt.layer("MyCap", "ask")
        assert layer.name == "MyCap"
        assert str(layer.kind) == "ask"
