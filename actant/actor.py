"""Actor：Python Actor 注册、代理与方法调用。"""

from __future__ import annotations

from typing import Any

from actant._serialization import dumps, loads
from actant.actant import _ActorCore
from actant.exceptions import ActorError, SerializationError


class _ActorDispatcher:
    """Rust 侧调用的 Python actor 消息分发器。

    所有由 Rust actor_core 回调的入口都集中在此类，统一捕获序列化与
    方法调用异常并转换为 ``ActorError``，避免 Python 异常泄漏到 Rust
    层导致 PyO3 跨边界错误难以诊断。
    """

    def __init__(self, instance: Any) -> None:
        self._instance: Any = instance

    def _handle_message(self, method: str, payload: bytes) -> bytes:
        try:
            data = loads(payload)
        except Exception as exc:  # cloudpickle 反序列化失败
            raise SerializationError(f"failed to decode actor message payload: {exc}") from exc

        args: tuple[Any, ...] = ()
        kwargs: dict[str, Any] = {}
        if isinstance(data, dict):
            args = data.get("args", ())
            kwargs = data.get("kwargs", {})
        elif isinstance(data, tuple):
            args = data

        method_fn = getattr(self._instance, method, None)
        if method_fn is None:
            raise ActorError(
                f"actor {type(self._instance).__name__} has no method {method!r}"
            )

        try:
            result = method_fn(*args, **kwargs)
        except Exception as exc:
            raise ActorError(
                f"actor method {type(self._instance).__name__}.{method} raised: {exc}"
            ) from exc
        try:
            return dumps(result)
        except Exception as exc:
            raise SerializationError(
                f"failed to encode actor method {method!r} return value: {exc}"
            ) from exc

    def _save_state(self) -> bytes:
        try:
            return dumps(self._instance)
        except Exception as exc:
            raise SerializationError(f"failed to snapshot actor state: {exc}") from exc

    def _load_state(self, state: bytes) -> None:
        try:
            self._instance = loads(state)
        except Exception as exc:
            raise SerializationError(f"failed to restore actor state: {exc}") from exc


class ActorMethodProxy:
    """延迟绑定的方法代理，调用时通过 Actor 系统发送消息。"""

    def __init__(self, actor_id: str, core: _ActorCore, method: str) -> None:
        self._actor_id: str = actor_id
        self._core: _ActorCore = core
        self._method: str = method

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        payload: bytes = dumps({"args": args, "kwargs": kwargs})
        result_bytes: bytes = await self._core.call_method(self._actor_id, self._method, payload)
        return loads(result_bytes)

    def __repr__(self) -> str:
        return f"<ActorMethodProxy {self._actor_id}.{self._method}>"


class Actor:
    """Python Actor 包装器。

    职责：
    - 保存 actor 类型名称与 Python 类
    - 在 Rust 运行时中创建实例后转为 proxy 模式
    - 通过 ``__getattr__`` 提供方法级延迟代理
    """

    def __init__(self, name: str, cls: type | None = None) -> None:
        self.name: str = name
        self.cls: type | None = cls
        self._actor_id: str | None = None
        self._core: _ActorCore | None = None

    @property
    def actor_id(self) -> str | None:
        return self._actor_id

    @property
    def is_class(self) -> bool:
        return self.cls is not None

    @property
    def is_proxy(self) -> bool:
        return self._actor_id is not None and self._core is not None

    @property
    def is_alive(self) -> bool:
        return self.is_proxy

    def _set_proxy(self, actor_id: str, core: _ActorCore) -> None:
        self._actor_id = actor_id
        self._core = core

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(f"actor '{self.name}' has no attribute '{name}'")
        if not self.is_proxy:
            raise AttributeError(f"actor '{self.name}' not created, call app.create_actor() first")
        # is_proxy 已确保 _actor_id 与 _core 均非 None，下方安全解包。
        actor_id: str = self._actor_id  # type: ignore[assignment]
        core: _ActorCore = self._core  # type: ignore[assignment]
        return ActorMethodProxy(actor_id, core, name)

    def __repr__(self) -> str:
        return f"<Actor {self.name} proxy={self.is_proxy}>"
