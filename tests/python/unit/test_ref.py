"""值引用（0.3.2 R2–R5）单元测试：ValueStore capability、Ref、AsyncResult 双态、await 桥。

不依赖子进程 / 跨节点网络；Rust blob 桥经本地 ``Runtime()``（test 配置，临时
data_dir）真实往返。跨节点 blob 传输由 e2e 覆盖。
"""

from __future__ import annotations

import asyncio
import importlib
import threading
from typing import Any

import cloudpickle
import pytest

from actant import Runtime
from actant.capabilities import (
    BUILTIN_CAPABILITIES,
    PYTHON_ONLY_CAPABILITIES,
    RUST_BACKED_CAPABILITIES,
    VALUE_STORE,
    ValueStoreReq,
)
from actant.exceptions import ActantTimeoutError, InvalidStateError
from actant.task._async_result import AsyncResult, _collect_dep_ids
from actant.task._gather import gather_async
from actant.task._ref import (
    REF_INLINE_THRESHOLD,
    Ref,
    _degrade_large_values,
    _materialize_refs,
    _RefArg,
    _resolve_ref_arg,
)

# ``actant.task`` 包属性被顶层 @task 装饰器遮蔽，经 importlib 取模块对象供 monkeypatch。
ref_module = importlib.import_module("actant.task._ref")


# ───────────────────────── R2：ValueStore capability ─────────────────────────


def test_value_store_capability_declared() -> None:
    """第 11 个内置 capability：perform 语义、Python-only（无 Rust fallback）。"""
    assert VALUE_STORE in BUILTIN_CAPABILITIES
    assert BUILTIN_CAPABILITIES[VALUE_STORE].kind == "perform"
    assert VALUE_STORE in PYTHON_ONLY_CAPABILITIES
    assert VALUE_STORE not in RUST_BACKED_CAPABILITIES
    assert len(BUILTIN_CAPABILITIES) == 11


def test_value_store_roundtrip_via_rust_bridge() -> None:
    """默认 handler 走 Rust blob 桥：store 返回 BlobRef 编码，fetch 原样取回。"""
    data = cloudpickle.dumps((True, {"blob": "x" * 1024}))
    with Runtime() as rt:
        ref_bytes = rt._dispatch_perform(VALUE_STORE, ValueStoreReq(op="store", data=data))
        assert isinstance(ref_bytes, bytes)
        assert len(ref_bytes) < 512  # BlobRef wire 编码 ~几十字节
        fetched = rt._dispatch_perform(
            VALUE_STORE, ValueStoreReq(op="fetch", ref=ref_bytes)
        )
    assert fetched == data


def test_value_store_handler_override() -> None:
    """用户在 start() 前预注册 handler 即完全替换默认 Rust 桥（perform 取链末位）。"""
    calls: list[ValueStoreReq] = []

    def _fake_handler(req: ValueStoreReq) -> bytes:
        calls.append(req)
        if req.op == "store":
            return b"fake-ref"
        return b"fake-value"

    with Runtime() as rt:
        rt.chain(VALUE_STORE, _fake_handler)
        ref_bytes = rt._dispatch_perform(
            VALUE_STORE, ValueStoreReq(op="store", data=b"ignored")
        )
        assert ref_bytes == b"fake-ref"
        value = rt._dispatch_perform(
            VALUE_STORE, ValueStoreReq(op="fetch", ref=b"r")
        )
        assert value == b"fake-value"
    assert len(calls) == 2


# ───────────────────────── R3：Ref 类型 ─────────────────────────


def _make_ref(rt: Runtime, value: object) -> Ref:
    """按结果帧约定构造 Ref：blob 内容 = cloudpickle((True, value))。"""
    frame = cloudpickle.dumps((True, value))
    ref_bytes = rt._dispatch_perform(VALUE_STORE, ValueStoreReq(op="store", data=frame))
    return Ref(ref_bytes)


def test_ref_result_transparent_unwrap() -> None:
    with Runtime() as rt:
        r = _make_ref(rt, {"payload": list(range(100))})
        assert r.result() == {"payload": list(range(100))}
        # 幂等：二次调用命中缓存（本地 blob 命中路径也正确）。
        assert r.result() == {"payload": list(range(100))}


def test_ref_hash_and_node() -> None:
    with Runtime() as rt:
        r = _make_ref(rt, "v")
        assert len(r.hash) == 64
        assert int(r.hash, 16) >= 0  # 小写 hex
        assert r.node  # 来源节点 endpoint 地址非空
        assert r.hash in repr(r)


def test_ref_direct_value_convention() -> None:
    """unwrap_frame=False（参数直传约定）：loads 即值。"""
    with Runtime() as rt:
        payload = cloudpickle.dumps([1, 2, 3])
        ref_bytes = rt._dispatch_perform(
            VALUE_STORE, ValueStoreReq(op="store", data=payload)
        )
        r = Ref(ref_bytes, unwrap_frame=False)
        assert r.result() == [1, 2, 3]


def test_ref_requires_active_runtime() -> None:
    r = Ref(b"not-a-real-ref")
    with pytest.raises(InvalidStateError):
        _ = r.hash
    with pytest.raises(InvalidStateError):
        r.result()


def test_resolve_ref_arg_worker_side() -> None:
    """worker 侧解哨兵：两种 blob 约定 + 递归容器。"""
    frame_arg = _RefArg(cloudpickle.dumps((True, "from-frame")), unwrap_frame=True)
    direct_arg = _RefArg(cloudpickle.dumps({"k": 1}), unwrap_frame=False)
    resolved = _resolve_ref_arg((frame_arg, {"nested": [direct_arg]}))
    assert resolved == ("from-frame", {"nested": [{"k": 1}]})
    # 无哨兵参数原样返回（含元组）。
    assert _resolve_ref_arg((1, ("a",))) == (1, ("a",))


def test_degrade_large_values() -> None:
    """超阈值直传参数落 blob + 哨兵；小参数 / 标量 / 哨兵原样保留。

    降级按提交参数树的顶层值逐一测量（容器子树随整值 pickle），与
    ``_prepare_task_def`` 的逐参数调用方式一致。
    """
    stored: list[bytes] = []

    def _store(data: bytes) -> bytes:
        stored.append(data)
        return b"ref-bytes"

    big = "x" * (REF_INLINE_THRESHOLD + 1)
    small = {"k": "v"}
    sentinel = _RefArg(b"already", unwrap_frame=False)

    # 大值：哨兵内联原 pickle 字节（测量 pickle 复用，无二次序列化）。
    out = _degrade_large_values(big, _store)
    assert isinstance(out, _RefArg)
    assert out.payload == cloudpickle.dumps(big)
    assert not out.unwrap_frame
    assert stored == [cloudpickle.dumps(big)]
    # 小值 / 标量 / 哨兵：原样返回，不触碰 store。
    assert _degrade_large_values(small, _store) is small
    assert _degrade_large_values(7, _store) == 7
    assert _degrade_large_values(None, _store) is None
    assert _degrade_large_values(sentinel, _store) is sentinel
    assert stored == [cloudpickle.dumps(big)]


def test_degrade_large_values_unserializable_kept() -> None:
    """不可序列化值原样保留，交由 _safe_serialize 输出定位诊断。"""
    out = _degrade_large_values(lambda: 1, lambda data: b"ref")
    assert callable(out)


def test_materialize_refs_fetches_in_parent() -> None:
    """提交方父进程代取：Ref → _RefArg(帧字节, unwrap_frame=True)。"""
    frame = cloudpickle.dumps((True, "big-value"))
    fetched: list[bytes] = []

    def _fetch(ref_bytes: bytes) -> bytes:
        fetched.append(ref_bytes)
        return frame

    tree = {"a": [Ref(b"ref-1")], "b": ("keep",)}
    out = _materialize_refs(tree, _fetch)
    assert fetched == [b"ref-1"]
    assert isinstance(out["a"][0], _RefArg)
    assert out["a"][0].payload == frame
    assert out["a"][0].unwrap_frame
    assert out["b"] == ("keep",)


# ───────────────────────── R4：AsyncResult 双态统一 ─────────────────────────


def test_small_result_inline_state() -> None:
    """小结果：对象缓存态，ref() 返回 None，result() 直接返回对象（含 bytes）。"""
    h = AsyncResult("t1")
    h._set_result(b"echo-bytes")  # bytes 返回值不被误当作序列化结果
    assert h.result(timeout=0) == b"echo-bytes"
    assert h.ref() is None
    # 回灌：对象态重新 pickle。
    ok, payload = h._export_outcome()
    assert ok and cloudpickle.loads(payload) == b"echo-bytes"


def test_large_result_ref_state() -> None:
    """大结果：Ref 态，ref() 可用、result() 透明反序列化、回灌引用字节。"""
    h = AsyncResult("t1")
    value = {"big": list(range(1000))}
    ref_bytes = cloudpickle.dumps((True, value))
    h._set_result_ref(ref_bytes)
    r = h.ref()
    assert r is not None
    fetched: list[bytes] = []

    def _fake_fetch(rb: bytes) -> bytes:
        fetched.append(rb)
        return ref_bytes

    orig = ref_module._value_fetch
    ref_module._value_fetch = _fake_fetch  # type: ignore[method-assign]
    try:
        assert h.result(timeout=0) == value
        assert fetched == [ref_bytes]
    finally:
        ref_module._value_fetch = orig  # type: ignore[method-assign]
    # 回灌：Ref 态原样返回 BlobRef 编码，不重序列化值。
    ok, payload = h._export_outcome()
    assert ok and payload == ref_bytes


def test_collect_dep_ids_keeps_ref_for_large_result() -> None:
    """大结果 handle 作为依赖：保留 Ref 不取值，依赖 id 仍收集（等待完成）。"""
    h = AsyncResult("up-1")
    h._set_result_ref(b"ref-1")
    small = AsyncResult("up-2")
    small._set_result(42)
    ids: list[str] = []
    resolved = _collect_dep_ids({"big": h, "n": small}, set(), ids)
    assert ids == ["up-1", "up-2"]
    assert isinstance(resolved["big"], Ref)
    assert resolved["n"] == 42


def test_collect_dep_ids_pending_large_result_keeps_ref(monkeypatch) -> None:
    """R6：下游提交早于上游完成时（eager flow 常态），大结果同样保留 Ref。

    ``Ref`` 只在结果抵达回调中产生；修复前 pending handle 直接落 ``result()``
    阻塞等待后会把大值整体反序列化进提交方（随后再被
    ``_degrade_large_values`` 二次落 blob + 重 pickle）。此处令 ``Ref.result``
    抛错——解析路径一旦触碰对象级反序列化即失败。
    """
    h = AsyncResult("up-pending")

    def _boom(self: Ref, timeout: float | None = None) -> Any:
        raise AssertionError("Ref.result() must not be called during dep resolution")

    monkeypatch.setattr(Ref, "result", _boom)
    timer = threading.Timer(0.05, lambda: h._set_result_ref(b"ref-pending"))
    timer.start()
    try:
        ids: list[str] = []
        resolved = _collect_dep_ids([h], set(), ids)
    finally:
        timer.join()
    assert ids == ["up-pending"]
    assert isinstance(resolved[0], Ref)
    assert resolved[0]._ref_bytes == b"ref-pending"


def test_collect_dep_ids_pending_failure_propagates() -> None:
    """pending 上游失败：等待后经 result() 抛出原异常（与既有小结果语义一致）。"""
    h = AsyncResult("up-fail")

    def _fail_later() -> None:
        h._set_error(ValueError("upstream boom"))

    timer = threading.Timer(0.05, _fail_later)
    timer.start()
    try:
        with pytest.raises(ValueError, match="upstream boom"):
            _collect_dep_ids([h], set(), [])
    finally:
        timer.join()


# ───────────────────────── R5：__await__ 去线程化 ─────────────────────────


def _no_await_threads() -> None:
    """await 桥接不得派生 ``actant-await`` 守护线程（D6 回归）。"""
    assert not any(t.name == "actant-await" for t in threading.enumerate())


def test_await_completed_handle() -> None:
    async def _main() -> None:
        h = AsyncResult("t1")
        h._set_result(42)
        assert await h == 42

    asyncio.run(_main())
    _no_await_threads()


def test_await_pending_handle_via_done_callback() -> None:
    """未完成 handle：完成回调直通 event loop，结果与异常均正确投递。"""

    async def _main() -> None:
        h_ok = AsyncResult("ok")
        h_err = AsyncResult("err")

        def _complete_later() -> None:
            h_ok._set_result("value")
            h_err._set_error(ValueError("boom"))

        timer = threading.Timer(0.05, _complete_later)
        timer.start()
        try:
            assert await h_ok == "value"
            with pytest.raises(ValueError, match="boom"):
                await h_err
        finally:
            timer.join()

    asyncio.run(_main())
    _no_await_threads()


def test_await_cancelled_handle() -> None:
    async def _main() -> None:
        h = AsyncResult("t1")
        h._set_cancelled()  # 终态取消（等价 _on_task_result 收到 CANCELLED）
        with pytest.raises(Exception):  # noqa: B017 —— TaskCancelledError 镜像
            await h

    asyncio.run(_main())


def test_gather_async_timeout_and_exceptions() -> None:
    """gather_async：桥接 future 等待，超时抛 ActantTimeoutError；return_exceptions 语义不变。"""

    async def _main() -> None:
        done_h = AsyncResult("done")
        done_h._set_result(1)
        fail_h = AsyncResult("fail")
        fail_h._set_error(ValueError("bad"))
        pending_h = AsyncResult("pending")

        # 全部完成 + return_exceptions=True：失败结果为异常对象。
        results = await gather_async(done_h, fail_h, timeout=1.0, return_exceptions=True)
        assert results[0] == 1
        assert isinstance(results[1], ValueError)
        # return_exceptions=False：失败异常透传。
        with pytest.raises(ValueError, match="bad"):
            await gather_async(done_h, fail_h, timeout=1.0)
        # 超时：pending 未完成 → ActantTimeoutError。
        with pytest.raises(ActantTimeoutError):
            await gather_async(done_h, pending_h, timeout=0.05)

    asyncio.run(_main())
    _no_await_threads()


def test_gather_async_none_result_value() -> None:
    """await / gather 对 None 结果（对象缓存态初值）同样正确投递。"""

    async def _main() -> None:
        h = AsyncResult("t1")
        h._set_result(None)
        assert await h is None
        results = await gather_async(h, timeout=1.0)
        assert results == [None]

    asyncio.run(_main())
