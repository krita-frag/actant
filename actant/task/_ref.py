"""值引用（Ref）：内容寻址的大值句柄（0.3.2 R3）。

大数据（> ``REF_INLINE_THRESHOLD``）不再内联进任务 payload，而是经
``ValueStore`` capability 落本节点内容寻址 blob store，参数/边位置放
``Ref``（几十字节），消费侧按需拉取。设计权衡见 ``plans/REF_DESIGN.md``。

blob 内容有两种来源约定（由 ``unwrap_frame`` 标志区分，不外泄）：

- 任务结果降级：blob 内容 = worker 结果帧字节原样
  （``cloudpickle((success, payload_obj))``，0 次重序列化）→ 解析取 ``[1]``。
- 参数直传降级：blob 内容 = ``cloudpickle(value)`` → 解析即值。

## Intrusively-linked 数据流

- 结果侧（R4）：父进程 ``_on_task_result`` 发现结果帧超阈值 → store →
  ``AsyncResult`` 内部持 ``Ref``；``result()`` 透明解析。
- 参数侧（R3b）：``_collect_dep_ids`` 对大结果保留 ``Ref`` 不取值；
  提交方 ``_submit`` 把 ``Ref`` 解析为 ``_RefArg``（帧内联字节）传给 worker；
  worker 在 ``_execute_with_retries`` 前解哨兵。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

import cloudpickle

from actant._runtime import get_current_runtime

_logger = logging.getLogger("actant.task")

#: 内联阈值（字节）：任务结果 / 提交参数序列化后超过此值即降级为值引用。
#: 1MB 远小于 worker 帧上限（256MB），保证降级路径在常规大值场景下即触发；
#: 又足够大，使毫秒级微任务（KB 级 payload）完全不受落盘开销影响。
REF_INLINE_THRESHOLD = 1024 * 1024


class _RefArg:
    """dispatch 参数哨兵：携带已解析的值字节，由 worker 侧解包为真实参数。

    提交方父进程把 ``Ref``（或超阈值的直传参数）解析为哨兵后内联进 envelope；
    worker 在执行前经 :func:`_resolve_ref_arg` 还原。cloudpickle 对模块级类
    按引用序列化，envelope 中哨兵只占字节载荷本身的大小。
    """

    __slots__ = ("payload", "unwrap_frame")

    def __init__(self, payload: bytes, *, unwrap_frame: bool) -> None:
        self.payload = payload
        self.unwrap_frame = unwrap_frame


def _resolve_ref_arg(value: Any) -> Any:
    """worker 侧解哨兵：``_RefArg`` → 真实参数值（递归容器）。

    ``unwrap_frame=True`` 表示载荷是任务结果帧
    （``cloudpickle((success, payload_obj))``），取 ``[1]``。
    """
    if isinstance(value, _RefArg):
        loaded = cloudpickle.loads(value.payload)
        return loaded[1] if value.unwrap_frame else loaded
    if isinstance(value, list):
        return [_resolve_ref_arg(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_resolve_ref_arg(v) for v in value)
    if isinstance(value, dict):
        return {k: _resolve_ref_arg(v) for k, v in value.items()}
    return value


def _value_store(data: bytes, runtime: Any = None) -> bytes:
    """经 ``ValueStore`` capability 落 blob，返回 BlobRef wire 编码字节。

    走 capability 分发而非直连 Rust 桥：用户覆盖 ``ValueStore`` handler
    （如 S3 后端）时内部降级路径同样生效。``runtime`` 缺省时取当前线程
    活跃 Runtime；结果回调线程无 Runtime 上下文，由调用方显式传入。
    """
    from actant.capabilities import VALUE_STORE, ValueStoreReq

    rt = runtime if runtime is not None else get_current_runtime()
    if rt is None:
        from actant.exceptions import InvalidStateError

        raise InvalidStateError(
            "ValueStore store: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    return cast(bytes, rt._dispatch_perform(VALUE_STORE, ValueStoreReq(op="store", data=data)))


def _value_fetch(ref_bytes: bytes, runtime: Any = None) -> bytes:
    """经 ``ValueStore`` capability 按 BlobRef wire 编码取回值字节。"""
    from actant.capabilities import VALUE_STORE, ValueStoreReq

    rt = runtime if runtime is not None else get_current_runtime()
    if rt is None:
        from actant.exceptions import InvalidStateError

        raise InvalidStateError(
            "ValueStore fetch: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    return cast(bytes, rt._dispatch_perform(VALUE_STORE, ValueStoreReq(op="fetch", ref=ref_bytes)))


def _materialize_refs(
    value: Any,
    fetch: Callable[[bytes], bytes],
) -> Any:
    """把参数树中的 :class:`Ref` 解析为 :class:`_RefArg`（提交方父进程代取）。

    ``fetch`` 返回 blob 原始字节（结果帧约定，``unwrap_frame=True``）。
    递归规则与 ``_resolve_value`` 一致（list / tuple / dict）。
    """
    if isinstance(value, Ref):
        return _RefArg(fetch(value._ref_bytes), unwrap_frame=True)
    if isinstance(value, list):
        return [_materialize_refs(v, fetch) for v in value]
    if isinstance(value, tuple):
        return tuple(_materialize_refs(v, fetch) for v in value)
    if isinstance(value, dict):
        return {k: _materialize_refs(v, fetch) for k, v in value.items()}
    return value


def _degrade_large_values(
    value: Any,
    store: Callable[[bytes], bytes],
) -> Any:
    """把参数树中超阈值的直传大值降级为 :class:`_RefArg`（R3b）。

    每个候选值预序列化测量（定长标量跳过）；超 ``REF_INLINE_THRESHOLD`` 时
    字节落 blob（内容寻址去重）+ 参数位放哨兵——大值全程只序列化一次
    （测量得到的 pickle 字节既落 blob 又随帧内联，不再重 pickle）。
    落 blob 失败不阻断提交：哨兵字节仍随帧内联交付，仅去重/引用语义缺失，
    经 exc_info 日志承载原因（与结果侧降级同一策略）。
    序列化失败的值原样保留，交由 ``_safe_serialize`` 输出定位诊断。
    """
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (_RefArg, Ref)):
        return value
    try:
        payload = cloudpickle.dumps(value)
    except (TypeError, AttributeError, ValueError, RecursionError):
        return value
    if len(payload) <= REF_INLINE_THRESHOLD:
        return value
    try:
        store(payload)
    except Exception:
        _logger.warning(
            "large argument (%d bytes) failed to store as blob ref; "
            "delivering inline bytes only",
            len(payload),
            exc_info=True,
        )
    return _RefArg(payload, unwrap_frame=False)


class Ref:
    """内容寻址值引用句柄（0.3.2 R3 公开 API）。

    持有 ``BlobRef`` wire 编码（blake3 hash + 来源节点），可跨节点按需拉取。
    由 ``AsyncResult.ref()``（大结果时）产生，可直接作为下游 ``submit`` 参数
    （提交方自动解析为帧内联字节）；解析经 ``ValueStore`` capability 分发，
    用户覆盖存储后端时同样生效。

    用法::

        big = produce.submit(data)
        r = big.ref()          # 大结果时非 None
        if r is not None:
            print(r.hash)      # blake3 hex
        consume.submit(r)      # 或直接传 handle，等价
    """

    __slots__ = ("_cache", "_ref_bytes", "_unwrap_frame")

    def __init__(self, ref_bytes: bytes, *, unwrap_frame: bool = True) -> None:
        self._ref_bytes = ref_bytes
        self._unwrap_frame = unwrap_frame
        self._cache: Any = None

    @property
    def hash(self) -> str:
        """内容寻址 blake3 hash（64 字符小写 hex）。"""
        return self._parts()[0]

    @property
    def node(self) -> str:
        """持有该 blob 的来源节点 endpoint 地址。"""
        return self._parts()[1]

    def result(self, timeout: float | None = None) -> Any:
        """拉取并反序列化引用的值（幂等：首次后缓存解析结果）。

        Args:
            timeout: 兼容性参数。引用在结果产生时即已落 blob，解析路径无
                等待阶段；网络拉取的超时由传输层控制。

        Returns:
            引用的任务返回值。
        """
        del timeout  # 见 docstring：解析无等待阶段，参数仅为 API 形状一致。
        if self._cache is not None:
            return self._cache
        loaded = cloudpickle.loads(_value_fetch(self._ref_bytes))
        value = loaded[1] if self._unwrap_frame else loaded
        self._cache = value
        return value

    def _parts(self) -> tuple[str, str]:
        core = get_current_runtime()
        if core is None or core._rust_core is None:
            from actant.exceptions import InvalidStateError

            raise InvalidStateError("Ref.hash/node requires an active Runtime")
        return cast(tuple[str, str], core._rust_core.value_ref_parts(self._ref_bytes))

    def __repr__(self) -> str:
        try:
            return f"Ref(hash={self.hash!r})"
        except Exception:
            return f"Ref(ref_bytes={len(self._ref_bytes)}B, unresolved)"


__all__ = ["REF_INLINE_THRESHOLD", "Ref"]
