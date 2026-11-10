"""``actant._internal.runtime`` 注册表与 Layer 单元测试（不启动 Rust）。"""
from __future__ import annotations

import pytest

import actant
from actant import Runtime
from actant.tasks import AsyncResult


def test_runtime_registers_builtin_capabilities() -> None:
    rt = Runtime()
    names = rt.capabilities
    assert "Store" in names
    assert "Routing" not in names


def test_layer_chain_and_handlers() -> None:
    rt = Runtime()
    layer = rt.layer("MyRouter", kind="ask")
    def h(x: object) -> object:
        return x
    layer.chain(h)
    assert len(rt.handlers("MyRouter")) == 1
    assert rt.handlers("MyRouter") == [h]


def test_layer_remove() -> None:
    rt = Runtime()
    def h(x: object) -> object:
        return x
    rt.layer("MyRouter", kind="ask").chain(h)
    assert rt.layer("MyRouter").remove(h) is True
    assert rt.layer("MyRouter").remove(h) is False


def test_layer_clear() -> None:
    rt = Runtime()
    rt.layer("MyRouter", kind="ask").chain(lambda x: x)
    rt.chain("MyRouter", lambda x: x)
    assert rt.layer("MyRouter").clear() == 2
    assert len(rt.handlers("MyRouter")) == 0


def test_replace_handler() -> None:
    rt = Runtime()
    rt.layer("MyRouter", kind="ask").chain(lambda x: x)
    rt.replace_handler("MyRouter", lambda x: "new")
    assert len(rt.handlers("MyRouter")) == 1


def test_replace_handler_unknown_capability() -> None:
    rt = Runtime()
    with pytest.raises(KeyError):
        rt.replace_handler("Unknown", lambda x: x)


def test_custom_capability_requires_kind() -> None:
    rt = Runtime()
    with pytest.raises(ValueError, match="requires explicit kind"):
        rt.layer("Custom")


def test_builtin_capability_kind_mismatch() -> None:
    rt = Runtime()
    with pytest.raises(ValueError, match="kind"):
        rt.layer("Store", kind="ask")


def test_capability_meta() -> None:
    rt = Runtime()
    meta = rt.capability_meta("Store")
    assert meta.name == "Store"
    assert meta.kind == "perform"


def test_capability_meta_unknown() -> None:
    rt = Runtime()
    with pytest.raises(KeyError):
        rt.capability_meta("Unknown")


def test_register_task() -> None:
    rt = Runtime()
    handle = AsyncResult("t1")
    rt.register_task("t1", handle)
    assert rt.list_tasks() == ["t1"]
    assert rt.get_task("t1") is handle


def test_unregister_task() -> None:
    rt = Runtime()
    handle = AsyncResult("t1")
    rt.register_task("t1", handle)
    assert rt.unregister_task("t1") is True
    assert rt.list_tasks() == []
    assert rt.unregister_task("t1") is False


def test_is_cancelled() -> None:
    rt = Runtime()
    rt._mark_task_cancelled("t1")
    assert rt.is_cancelled("t1") is True
    rt._clear_task_cancelled("t1")
    assert rt.is_cancelled("t1") is False


def test_cancel_task_unknown() -> None:
    rt = Runtime()
    assert rt.cancel_task("missing") is False


def test_cancel_task_completed() -> None:
    rt = Runtime()
    handle = AsyncResult("t1")
    handle._set_result(1)
    rt.register_task("t1", handle)
    assert rt.cancel_task("t1") is False


def test_use_runtime_restores_previous() -> None:
    rt1 = Runtime()
    rt2 = Runtime()
    with actant.use_runtime(rt1):
        assert actant.get_current_runtime() is rt1
        with actant.use_runtime(rt2):
            assert actant.get_current_runtime() is rt2
        assert actant.get_current_runtime() is rt1
