"""``actant.effects`` 单元测试（构造 Runtime 但不 start Rust）。"""
from __future__ import annotations

import pytest

import actant
from actant import Runtime, aio
from actant.effects import ask, impossible
from actant.exceptions import InternalError, InvalidStateError, NotFoundError


def _routing_ask(name: str) -> str:
    return f"{name}?"


def test_ask_without_runtime_raises() -> None:
    with pytest.raises(InvalidStateError):
        ask("MyCap", "x")


def test_perform_without_runtime_raises() -> None:
    with pytest.raises(InvalidStateError):
        actant.perform("MyCap", "x")


def test_emit_without_runtime_raises() -> None:
    with pytest.raises(InvalidStateError):
        actant.emit("MyCap", "x")


def test_ask_capability_basic() -> None:
    rt = Runtime()
    rt.layer("MyCap", kind="ask").chain(_routing_ask)
    with actant.use_runtime(rt):
        result = ask("MyCap", "hello")
    assert result == "hello?"


def test_ask_returns_first_non_none() -> None:
    rt = Runtime()
    rt.layer("MyDecision", kind="ask").chain(lambda x: None)
    rt.layer("MyDecision", kind="ask").chain(lambda x: f"second:{x}")
    rt.layer("MyDecision", kind="ask").chain(lambda x: f"third:{x}")
    with actant.use_runtime(rt):
        result = ask("MyDecision", "x")
    # 后注册的 handler 优先级更高（逆序调用），因此第三个 handler 决定结果。
    assert result == "third:x"


def test_ask_returns_none_when_all_abstain() -> None:
    rt = Runtime()
    rt.layer("MyDecision", kind="ask").chain(lambda x: None)
    with actant.use_runtime(rt):
        assert ask("MyDecision", "x") is None


def test_perform_uses_last_handler() -> None:
    rt = Runtime()
    rt.layer("Codec", "perform").chain(lambda x: "first")
    rt.chain("Codec", lambda x: "last")
    with actant.use_runtime(rt):
        assert actant.perform("Codec", None) == "last"


def test_perform_without_handlers_raises() -> None:
    rt = Runtime()
    rt.layer("Codec", "perform")  # 注册 capability，但不附加 handler
    with actant.use_runtime(rt), pytest.raises(NotFoundError):
        actant.perform("Codec", None)


def test_emit_calls_all_handlers() -> None:
    rt = Runtime()
    called: list[str] = []
    rt.chain("TaskLifecycle", lambda e: called.append("a"))
    rt.chain("TaskLifecycle", lambda e: called.append("b"))
    with actant.use_runtime(rt):
        actant.emit("TaskLifecycle", "event")
    assert called == ["a", "b"]


def test_emit_on_error_raise() -> None:
    rt = Runtime()
    rt.chain("TaskLifecycle", lambda e: (_ for _ in ()).throw(ValueError("boom")))
    with actant.use_runtime(rt), pytest.raises(ValueError, match="boom"):
        actant.emit("TaskLifecycle", "event", on_error="raise")


def test_emit_on_error_collect() -> None:
    rt = Runtime()
    rt.chain("TaskLifecycle", lambda e: (_ for _ in ()).throw(ValueError("a")))
    rt.chain("TaskLifecycle", lambda e: (_ for _ in ()).throw(ValueError("b")))
    with actant.use_runtime(rt), pytest.raises(actant.ActantError):
        actant.emit("TaskLifecycle", "event", on_error="collect")


def test_emit_invalid_on_error() -> None:
    with pytest.raises(ValueError, match="on_error"):
        actant.emit("TaskLifecycle", "event", on_error="bad")  # type: ignore[arg-type]


def test_ask_kind_mismatch() -> None:
    rt = Runtime()
    rt.layer("MyPerform", kind="perform").chain(lambda x: x)
    with actant.use_runtime(rt), pytest.raises(InvalidStateError, match="not 'ask'"):
        ask("MyPerform", "x")


def test_perform_kind_mismatch() -> None:
    rt = Runtime()
    rt.layer("MyAsk", kind="ask").chain(lambda x: x)
    with actant.use_runtime(rt), pytest.raises(InvalidStateError, match="not 'perform'"):
        actant.perform("MyAsk", "x")


def test_emit_kind_mismatch() -> None:
    rt = Runtime()
    rt.layer("MyAsk", kind="ask").chain(lambda x: x)
    with actant.use_runtime(rt), pytest.raises(InvalidStateError, match="not 'emit'"):
        actant.emit("MyAsk", "x")


def test_impossible_raises() -> None:
    with pytest.raises(InternalError, match="impossible"):
        impossible("unreachable")


class TestAsyncEffects:
    """``aio.ask`` / ``aio.perform`` / ``aio.perform_batch``。

    Python handler 路径不依赖 Rust 运行时：handler 在默认 executor 中执行，
    因此只构造未 ``start()`` 的 Runtime 即可覆盖成功、弃权、异常与批量分支。
    """

    async def test_ask_async_without_runtime_raises(self) -> None:
        with pytest.raises(InvalidStateError):
            await aio.ask("MyCap", "x")

    def test_ask_async_requires_running_loop(self) -> None:
        rt = Runtime()
        rt.layer("MyCap", kind="ask").chain(_routing_ask)
        coro = aio.ask("MyCap", "x")
        try:
            with actant.use_runtime(rt), pytest.raises(
                InvalidStateError, match="event loop"
            ) as ei:
                coro.send(None)
            # 缺少 loop 与 Runtime 生命周期无关，不应给出 start() 提示。
            assert "Runtime.start" not in str(ei.value)
        finally:
            coro.close()

    async def test_ask_async_returns_handler_result(self) -> None:
        rt = Runtime()
        rt.layer("MyCap", kind="ask").chain(_routing_ask)
        with actant.use_runtime(rt):
            assert await aio.ask("MyCap", "hello") == "hello?"

    async def test_ask_async_first_non_none_wins(self) -> None:
        rt = Runtime()
        rt.layer("MyDecision", kind="ask").chain(lambda x: None)
        rt.layer("MyDecision", kind="ask").chain(lambda x: f"later:{x}")
        with actant.use_runtime(rt):
            assert await aio.ask("MyDecision", "x") == "later:x"

    async def test_ask_async_all_abstain_returns_none(self) -> None:
        rt = Runtime()
        rt.layer("MyDecision", kind="ask").chain(lambda x: None)
        with actant.use_runtime(rt):
            assert await aio.ask("MyDecision", "x") is None

    async def test_ask_async_kind_mismatch(self) -> None:
        rt = Runtime()
        rt.layer("MyPerform", kind="perform").chain(lambda x: x)
        with actant.use_runtime(rt), pytest.raises(
            InvalidStateError, match="not 'ask'"
        ):
            await aio.ask("MyPerform", "x")

    async def test_perform_async_without_runtime_raises(self) -> None:
        with pytest.raises(InvalidStateError):
            await aio.perform("MyCap", "x")

    async def test_perform_async_uses_last_handler(self) -> None:
        rt = Runtime()
        rt.layer("MyPerform", kind="perform").chain(lambda x: "first")
        rt.layer("MyPerform", kind="perform").chain(lambda x: f"last:{x}")
        with actant.use_runtime(rt):
            assert await aio.perform("MyPerform", "x") == "last:x"

    async def test_perform_async_propagates_handler_error(self) -> None:
        rt = Runtime()
        rt.layer("MyPerform", kind="perform").chain(
            lambda x: (_ for _ in ()).throw(ValueError("boom"))
        )
        with actant.use_runtime(rt), pytest.raises(ValueError, match="boom"):
            await aio.perform("MyPerform", "x")

    async def test_perform_async_without_handler_raises_not_found(self) -> None:
        rt = Runtime()
        rt.layer("Codec", "perform")  # 注册 capability，但不附加 handler
        with actant.use_runtime(rt), pytest.raises(NotFoundError):
            await aio.perform("Codec", "x")

    async def test_perform_batch_async_without_runtime_raises(self) -> None:
        with pytest.raises(InvalidStateError):
            await aio.perform_batch([("MyPerform", "x")])

    async def test_perform_batch_async_rejects_non_list(self) -> None:
        with pytest.raises(TypeError, match="must be a list"):
            await aio.perform_batch(("MyPerform", "x"))  # type: ignore[arg-type]

    async def test_perform_batch_async_rejects_bad_item(self) -> None:
        rt = Runtime()
        with actant.use_runtime(rt), pytest.raises(TypeError, match=r"items\[0\]"):
            await aio.perform_batch(["not-a-tuple"])  # type: ignore[list-item]

    async def test_perform_batch_async_empty_returns_empty_without_runtime(self) -> None:
        # 空批次是合法 no-op：不要求活跃 Runtime 或事件循环。
        assert await aio.perform_batch([]) == []

    async def test_gather_rejects_non_async_result(self) -> None:
        with pytest.raises(TypeError, match=r"handles\[0\]"):
            await aio.gather("not-a-handle")  # type: ignore[arg-type]

    async def test_perform_batch_records_all_entries_in_one_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """批量记账必须整批提交，避免逐条跨 PyO3 边界。"""
        rt = Runtime()
        rt.layer("MyPerform", kind="perform").chain(lambda x: x)
        recorded: list[list[tuple[str, str]]] = []
        monkeypatch.setattr(
            rt, "_record_effects", lambda entries: recorded.append(list(entries))
        )
        with actant.use_runtime(rt):
            await aio.perform_batch([("MyPerform", 1), ("MyPerform", 2)])
        assert recorded == [[("MyPerform", "perform"), ("MyPerform", "perform")]]

    async def test_perform_batch_async_preserves_order(self) -> None:
        rt = Runtime()
        rt.layer("MyPerform", kind="perform").chain(lambda x: f"done:{x}")
        with actant.use_runtime(rt):
            results = await aio.perform_batch(
                [("MyPerform", "a"), ("MyPerform", "b"), ("MyPerform", "c")]
            )
        assert results == ["done:a", "done:b", "done:c"]

    async def test_perform_batch_async_isolates_failures(self) -> None:
        rt = Runtime()

        def _maybe_fail(x: str) -> str:
            if x == "bad":
                raise ValueError("boom")
            return f"done:{x}"

        rt.layer("MyPerform", kind="perform").chain(_maybe_fail)
        with actant.use_runtime(rt):
            results = await aio.perform_batch(
                [("MyPerform", "ok"), ("MyPerform", "bad"), ("MyPerform", "ok2")]
            )
        assert results[0] == "done:ok"
        assert isinstance(results[1], ValueError)
        assert results[2] == "done:ok2"
