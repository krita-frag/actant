"""actor.py 单元测试：Actor / ActorMethodProxy / _ActorDispatcher。

覆盖目标：100% 行覆盖 + 分支覆盖。
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from actant.actor import Actor, ActorMethodProxy, _ActorDispatcher
from actant.exceptions import ActorError, SerializationError

# ---------------------------------------------------------------------------
# 测试用 actor 类
# ---------------------------------------------------------------------------


class Counter:
    def __init__(self, start: int = 0) -> None:
        self.value = start
        self.history: list[int] = []

    def increment(self, n: int = 1) -> int:
        self.value += n
        self.history.append(self.value)
        return self.value

    def add(self, a: int, b: int) -> int:
        return a + b

    def get_value(self) -> int:
        return self.value

    def raises_error(self) -> None:
        raise ValueError("boom")


# ---------------------------------------------------------------------------
# Actor — 基本属性
# ---------------------------------------------------------------------------


class TestActorProperties:
    def test_construction_default_cls_none(self):
        actor = Actor("counter")
        assert actor.name == "counter"
        assert actor.cls is None
        assert actor.actor_id is None

    def test_construction_with_cls(self):
        actor = Actor("counter", cls=Counter)
        assert actor.cls is Counter

    def test_is_class_true_when_cls_set(self):
        actor = Actor("counter", cls=Counter)
        assert actor.is_class is True

    def test_is_class_false_when_cls_none(self):
        actor = Actor("counter")
        assert actor.is_class is False

    def test_is_proxy_false_before_set_proxy(self):
        actor = Actor("counter", cls=Counter)
        assert actor.is_proxy is False
        assert actor.is_alive is False

    def test_is_proxy_true_after_set_proxy(self):
        actor = Actor("counter", cls=Counter)
        core = MagicMock()
        actor._set_proxy("actor-1", core)
        assert actor.is_proxy is True
        assert actor.is_alive is True
        assert actor.actor_id == "actor-1"

    def test_repr_not_proxy(self):
        actor = Actor("counter")
        assert repr(actor) == "<Actor counter proxy=False>"

    def test_repr_is_proxy(self):
        actor = Actor("counter", cls=Counter)
        actor._set_proxy("id-1", MagicMock())
        assert repr(actor) == "<Actor counter proxy=True>"


# ---------------------------------------------------------------------------
# Actor.__getattr__ — 延迟代理
# ---------------------------------------------------------------------------


class TestActorGetattr:
    def test_getattr_underscore_raises(self):
        actor = Actor("counter", cls=Counter)
        with pytest.raises(AttributeError, match="has no attribute"):
            actor._private_method  # noqa: B018

    def test_getattr_not_proxy_raises(self):
        actor = Actor("counter", cls=Counter)
        with pytest.raises(AttributeError, match="not created"):
            actor.increment  # noqa: B018

    def test_getattr_returns_method_proxy(self):
        actor = Actor("counter", cls=Counter)
        core = MagicMock()
        actor._set_proxy("actor-1", core)
        proxy = actor.increment
        assert isinstance(proxy, ActorMethodProxy)
        assert proxy._actor_id == "actor-1"
        assert proxy._method == "increment"


# ---------------------------------------------------------------------------
# ActorMethodProxy
# ---------------------------------------------------------------------------


class TestActorMethodProxy:
    def test_repr(self):
        core = MagicMock()
        proxy = ActorMethodProxy("id-1", core, "increment")
        assert repr(proxy) == "<ActorMethodProxy id-1.increment>"

    @pytest.mark.asyncio
    async def test_call_invokes_core(self):
        from actant._serialization import dumps, loads

        core = MagicMock()
        core.call_method = AsyncMock(return_value=dumps(42))
        proxy = ActorMethodProxy("id-1", core, "increment")

        result = await proxy(5)
        assert result == 42
        core.call_method.assert_awaited_once()
        call_args = core.call_method.call_args
        assert call_args.args[0] == "id-1"
        assert call_args.args[1] == "increment"
        # payload 解码后应包含 args/kwargs
        payload = call_args.args[2]
        data = loads(payload)
        assert data["args"] == (5,)
        assert data["kwargs"] == {}

    @pytest.mark.asyncio
    async def test_call_with_kwargs(self):
        from actant._serialization import dumps, loads

        core = MagicMock()
        core.call_method = AsyncMock(return_value=dumps(10))
        proxy = ActorMethodProxy("id-1", core, "add")

        result = await proxy(3, b=7)
        assert result == 10
        payload = core.call_method.call_args.args[2]
        data = loads(payload)
        assert data["args"] == (3,)
        assert data["kwargs"] == {"b": 7}


# ---------------------------------------------------------------------------
# _ActorDispatcher — _handle_message
# ---------------------------------------------------------------------------


class TestActorDispatcherHandleMessage:
    def test_handle_message_dict_payload(self):
        from actant._serialization import dumps

        dispatcher = _ActorDispatcher(Counter())
        payload = dumps({"args": (5,), "kwargs": {}})
        result_bytes = dispatcher._handle_message("increment", payload)
        from actant._serialization import loads

        assert loads(result_bytes) == 5

    def test_handle_message_tuple_payload(self):
        from actant._serialization import dumps, loads

        dispatcher = _ActorDispatcher(Counter())
        payload = dumps((3, 4))  # tuple 作为 args
        result_bytes = dispatcher._handle_message("add", payload)
        assert loads(result_bytes) == 7

    def test_handle_message_non_dict_non_tuple_payload_uses_defaults(self):
        """data 既非 dict 也非 tuple → args=() kwargs={}（34->37 False 分支）。"""
        from actant._serialization import dumps, loads

        dispatcher = _ActorDispatcher(Counter())
        # loads(dumps(42)) 返回 int 42，既非 dict 也非 tuple
        payload = dumps(42)
        # Counter.get_value() 不需要参数，应正常执行
        result_bytes = dispatcher._handle_message("get_value", payload)
        assert loads(result_bytes) == 0

    def test_handle_message_kwargs(self):
        from actant._serialization import dumps, loads

        dispatcher = _ActorDispatcher(Counter())
        payload = dumps({"args": (), "kwargs": {"n": 10}})
        result_bytes = dispatcher._handle_message("increment", payload)
        assert loads(result_bytes) == 10

    def test_handle_message_invalid_payload_raises_serialization_error(self):

        dispatcher = _ActorDispatcher(Counter())
        # 用无法被 loads 解码的字节
        with pytest.raises(SerializationError, match="failed to decode"):
            dispatcher._handle_message("increment", b"\xff\xffinvalid")

    def test_handle_message_unknown_method_raises_actor_error(self):
        from actant._serialization import dumps

        dispatcher = _ActorDispatcher(Counter())
        payload = dumps({"args": (), "kwargs": {}})
        with pytest.raises(ActorError, match="has no method"):
            dispatcher._handle_message("nonexistent", payload)

    def test_handle_message_method_raises_wraps_actor_error(self):
        from actant._serialization import dumps

        dispatcher = _ActorDispatcher(Counter())
        payload = dumps({"args": (), "kwargs": {}})
        with pytest.raises(ActorError, match="raised"):
            dispatcher._handle_message("raises_error", payload)

    def test_handle_message_result_encode_failure_raises_serialization_error(self):
        from actant._serialization import dumps

        # 返回不可序列化对象的 method
        class UnpicklableResult:
            def __getstate__(self):
                raise TypeError("cannot pickle")

        class BadActor:
            def bad_method(self):
                return UnpicklableResult()

        dispatcher = _ActorDispatcher(BadActor())
        payload = dumps({"args": (), "kwargs": {}})
        with pytest.raises(SerializationError, match="failed to encode"):
            dispatcher._handle_message("bad_method", payload)


# ---------------------------------------------------------------------------
# _ActorDispatcher — _save_state / _load_state
# ---------------------------------------------------------------------------


class TestActorDispatcherState:
    def test_save_state_returns_bytes(self):
        from actant._serialization import loads

        dispatcher = _ActorDispatcher(Counter(100))
        state = dispatcher._save_state()
        assert isinstance(state, bytes)
        # 反序列化验证
        restored = loads(state)
        assert restored.value == 100

    def test_load_state_restores_instance(self):
        from actant._serialization import dumps

        original = Counter(50)
        original.increment(10)  # value = 60
        state = dumps(original)

        dispatcher = _ActorDispatcher(Counter(0))
        dispatcher._load_state(state)
        # 验证 instance 被替换
        assert dispatcher._instance.value == 60

    def test_load_state_invalid_payload_raises_serialization_error(self):
        dispatcher = _ActorDispatcher(Counter(0))
        with pytest.raises(SerializationError, match="failed to restore"):
            dispatcher._load_state(b"\xff\xffinvalid")

    def test_save_state_unserializable_raises_serialization_error(self):
        class Unpicklable:
            def __getstate__(self):
                raise TypeError("cannot pickle")

        dispatcher = _ActorDispatcher(Unpicklable())
        with pytest.raises(SerializationError, match="failed to snapshot"):
            dispatcher._save_state()


# ---------------------------------------------------------------------------
# Actor — set_proxy 后调用方法
# ---------------------------------------------------------------------------


class TestActorProxyFlow:
    @pytest.mark.asyncio
    async def test_full_proxy_call_flow(self):
        """Actor.set_proxy → __getattr__ → ActorMethodProxy.__call__ 完整流程。"""
        from actant._serialization import dumps

        core = MagicMock()
        core.call_method = AsyncMock(return_value=dumps(15))
        actor = Actor("counter", cls=Counter)
        actor._set_proxy("actor-xyz", core)

        # 通过 __getattr__ 获取方法代理并调用
        proxy = actor.increment
        result = await proxy(10)
        assert result == 15

        # 验证 core.call_method 被正确调用
        core.call_method.assert_awaited_once()
        call_args = core.call_method.call_args
        assert call_args.args[0] == "actor-xyz"
        assert call_args.args[1] == "increment"
        assert isinstance(call_args.args[2], bytes)
