"""序列化工具：处理 Python 对象与 Rust 运行时之间的序列化。

Payload 格式（Python→Rust worker）：

## default_payload（由 Python 构建，Rust 视为不透明字节）

- 空 bytes: 无参调用
- [TAG_SINGLE(0x00), cloudpickle(args_tuple)...]: 位置参数调用
- [TAG_GROUP(0x01), count(u32), len1(u32), data1..., ...]: 组结果调用
- [TAG_SINGLE_KW(0x02), cloudpickle((args, kwargs))...]: 位置+关键字参数调用
- [TAG_GENERIC(0x05), cloudpickle((fn, args, kwargs))...]: 内联 callable
- [TAG_POSITIONAL(0x07), cloudpickle((fn, positions, concrete_args, concrete_kwargs))...]:
  位置感知合并（TaskRef 与具体参数混合）

## 最终 payload（Rust 构建后，worker 收到）

若任务有前驱，Rust 用 TAG_UPSTREAM_PREFIX 包装：
[TAG_UPSTREAM_PREFIX(0x08), upstream_count(u32), up_len1(u32), up_bytes1..., ..., default_payload]

Python dispatcher 先解包 upstream prefix，再把 default_payload 交给对应 tag 的 dispatcher。

Rust 类型编码：
- encode_retry: Python dict → _RetryPolicy
- encode_priority: Python int/str → i32 数值
- encode_failure_strategy: Python str → _FailureStrategy

注意：Rust 内部的 payload 序列化使用 postcard，与 Python 层的格式独立。

.. warning::

    **安全风险：cloudpickle 反序列化任意代码执行**

    本模块基于 `cloudpickle` 实现任务参数与结果的序列化。`cloudpickle`
    可以序列化任意 Python 可调用对象，其反序列化过程等价于执行任意
    代码。任何对 `loads` / `_dispatch_task` 的调用都会执行 payload 中的
    代码。

    **必须**确保仅在以下场景使用：

    1. 任务 payload 来自集群内可信节点（iroh P2P 通道提供 TLS 身份认证）；
    2. Worker 进程的权限受限（不要以特权用户运行 worker）；
    3. 集群节点不会被恶意节点加入（通过 `NetworkConfig.bootstrap_nodes`
       显式指定可信引导节点，或限制 `preset="local"` 的网络可达范围）。

    **禁止**：将 `_dispatch_task` 或 `loads` 直接暴露给未认证的远程输入
    （如 HTTP API、消息队列）。任何接收外部输入的入口必须由应用层
    自行实现签名校验或沙箱执行。
"""

from __future__ import annotations

import os
import struct
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

import cloudpickle

from actant.exceptions import InternalError, PayloadTooLargeError


class PayloadSerializer(ABC):
    """任务 payload 序列化策略。"""

    @abstractmethod
    def dumps(self, obj: object, *, max_size: int | None = None) -> bytes:
        """将 Python 对象序列化为 bytes。"""

    @abstractmethod
    def loads(self, data: bytes) -> Any:
        """将 bytes 反序列化为 Python 对象。"""


class CloudpickleSerializer(PayloadSerializer):
    """默认序列化器：cloudpickle。"""

    def dumps(self, obj: object, *, max_size: int | None = None) -> bytes:
        return dumps(obj, max_size=max_size)

    def loads(self, data: bytes) -> Any:
        return loads(data)

# 首次导入时发出安全警告，提醒用户 cloudpickle 反序列化的 RCE 风险。
# 使用进程级标志（存于 sys 模块属性）确保即使模块被 reload 也只触发一次。
# 设置环境变量 ACTANT_NO_CLOUDPICKLE_WARN=1 可完全禁用该警告。
_CLOUDPICKLE_WARN_KEY = "_actant_cloudpickle_warned"
if not os.environ.get("ACTANT_NO_CLOUDPICKLE_WARN") and not getattr(sys, _CLOUDPICKLE_WARN_KEY, False):
    warnings.warn(
        "actant._serialization uses cloudpickle which executes arbitrary code on "
        "deserialization. Only deserialize payloads from trusted cluster nodes. "
        "See module docstring for security guidelines.",
        category=UserWarning,
        stacklevel=2,
    )
    setattr(sys, _CLOUDPICKLE_WARN_KEY, True)

TAG_SINGLE = 0x00
TAG_GROUP = 0x01
TAG_SINGLE_KW = 0x02
TAG_GENERIC = 0x05  # 内联 callable:cloudpickle((fn, args, kwargs)) — 用于通用 worker
TAG_POSITIONAL = 0x07  # 位置感知合并：TaskRef 与具体参数混合
TAG_UPSTREAM_PREFIX = 0x08  # Rust orchestrator 前置上游结果的包装标签

# 预计算单字节前缀 — 避免每次调用 bytes([tag]) 分配。
_TAG_SINGLE_B = bytes((TAG_SINGLE,))
_TAG_GROUP_B = bytes((TAG_GROUP,))
_TAG_SINGLE_KW_B = bytes((TAG_SINGLE_KW,))
_TAG_GENERIC_B = bytes((TAG_GENERIC,))
_TAG_POSITIONAL_B = bytes((TAG_POSITIONAL,))


def pack_generic(fn: Callable[..., Any], args: tuple[object, ...], kwargs: dict[str, Any]) -> bytes:
    """打包内联 callable payload:cloudpickle((fn, args, kwargs))。

    用于通用 worker 节点:worker 无需预加载业务模块,运行时直接反序列化
    payload 中的 callable 并执行。
    """
    return _TAG_GENERIC_B + cloudpickle.dumps((fn, args, kwargs))  # type: ignore[no-any-return]


def pack_positional(
    fn: Callable[..., Any] | None,
    taskref_positions: list[int],
    taskref_kwargs_keys: list[str],
    concrete_args: tuple[object, ...],
    concrete_kwargs: dict[str, Any],
) -> bytes:
    """打包位置感知合并 payload（TAG_POSITIONAL）。

    用于 args/kwargs 中 TaskRef/BranchRef 与具体参数混合的场景。
    ``taskref_positions`` 记录依赖引用在原始 args 元组中的索引位置。
    ``taskref_kwargs_keys`` 记录依赖引用在原始 kwargs 中的 key 列表。
    运行时由 Rust 将上游结果按 DAG 边顺序前置（args 依赖优先，kwargs 依赖按
    key 列表顺序），Python 据此重建完整参数列表。

    ``fn`` 为 None 时 worker 通过 task_name 查找 handler；非 None 时内联执行。
    """
    return _TAG_POSITIONAL_B + cloudpickle.dumps(  # type: ignore[no-any-return]
        (fn, taskref_positions, taskref_kwargs_keys, concrete_args, concrete_kwargs)
    )


# ---------------------------------------------------------------------------
# 基础序列化
# ---------------------------------------------------------------------------


def get_default_retry_policy() -> dict[str, int | float]:
    """从 Rust 运行时获取默认重试策略。

    委托给 Rust _RetryPolicy.default() 作为单一可信源。
    """
    from actant.actant import _RetryPolicy

    p = _RetryPolicy.default()  # type: ignore[attr-defined]
    if p is None:  # pragma: no cover - defensive: Rust 不应返回 None
        raise InternalError("_RetryPolicy.default() returned None")
    return {
        "max_retries": p.max_retries,
        "delay_ms": p.delay_ms,
        "backoff_multiplier": p.backoff_multiplier,
        "max_delay_ms": p.max_delay_ms,
    }


def dumps(obj: object, *, max_size: int | None = None) -> bytes:
    data: bytes = cloudpickle.dumps(obj)
    if max_size is not None and len(data) > max_size:
        raise PayloadTooLargeError(actual=len(data), limit=max_size)
    return data


def loads(data: bytes) -> Any:
    return cloudpickle.loads(data)


def pack_single(args: tuple[object, ...]) -> bytes:
    return _TAG_SINGLE_B + cloudpickle.dumps(args)  # type: ignore[no-any-return]


def pack_single_kw(args: tuple[object, ...], kwargs: dict[str, object]) -> bytes:
    return _TAG_SINGLE_KW_B + cloudpickle.dumps((args, kwargs))  # type: ignore[no-any-return]


def pack_group(results: list[bytes]) -> bytes:
    buf = bytearray()
    buf += _TAG_GROUP_B
    buf += struct.pack("<I", len(results))
    for data in results:
        buf += struct.pack("<I", len(data))
        buf += data
    return bytes(buf)


def unpack_upstream_prefix(payload: bytes) -> tuple[list[Any], bytes]:
    """解包 TAG_UPSTREAM_PREFIX 包装的 payload。

    返回 (upstream_results, inner_payload)：
    - upstream_results: 前驱任务结果列表（已反序列化）
    - inner_payload: 原始 default_payload（含 callable + concrete args）

    若 payload 不是 TAG_UPSTREAM_PREFIX，返回 ([], payload)。
    """
    if not payload or payload[0] != TAG_UPSTREAM_PREFIX:
        return ([], payload)
    data = payload[1:]
    if len(data) < 4:
        raise ValueError(
            f"upstream prefix too short: need at least 4 bytes for count, got {len(data)}"
        )
    upstream_count = struct.unpack_from("<I", data, 0)[0]
    offset = 4
    upstream: list[Any] = []
    for i in range(upstream_count):
        if offset + 4 > len(data):
            raise ValueError(
                f"upstream prefix truncated at item {i}/{upstream_count}: "
                f"need 4 bytes for length at offset {offset}, got {len(data) - offset}"
            )
        length = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        if offset + length > len(data):
            raise ValueError(
                f"upstream prefix truncated at item {i}/{upstream_count}: "
                f"need {length} bytes at offset {offset}, got {len(data) - offset}"
            )
        upstream.append(loads(data[offset : offset + length]))
        offset += length
    inner_payload = data[offset:]
    return (upstream, inner_payload)


# ---------------------------------------------------------------------------
# 任务分发
# ---------------------------------------------------------------------------


def _dispatch_empty(fn: Callable[..., Any], upstream: list[Any]) -> Any:
    """分发无 default_payload 的调用（叶子任务无参数）。

    设计上仅用于叶子任务（无前驱），upstream 应为空。
    """
    from actant.task import _run_sync_or_async

    return _run_sync_or_async(fn)


def _dispatch_single(fn: Callable[..., Any], payload: bytes, upstream: list[Any]) -> Any:
    """分发位置参数调用（TAG_SINGLE）。

    设计上仅用于叶子任务（无前驱），upstream 应为空。
    有依赖的任务走 TAG_POSITIONAL 路径以保留位置信息。
    """
    from actant.task import _run_sync_or_async

    args = loads(payload)
    if isinstance(args, tuple):
        return _run_sync_or_async(fn, *args)
    return _run_sync_or_async(fn, args)


def _dispatch_single_kw(fn: Callable[..., Any], payload: bytes, upstream: list[Any]) -> Any:
    """分发位置参数 + 关键字参数调用（TAG_SINGLE_KW）。

    设计上仅用于叶子任务（无前驱），upstream 应为空。
    """
    from actant.task import _run_sync_or_async

    pos_args, kw_args = loads(payload)
    return _run_sync_or_async(fn, *pos_args, **kw_args)


def _dispatch_group(fn: Callable[..., Any], payload: bytes, upstream: list[Any]) -> Any:
    """分发组结果调用（TAG_GROUP）。

    用于 parallel() 收集多个前驱任务结果：upstream 是 Rust 前置的前驱结果列表。
    """
    from actant.task import _run_sync_or_async

    if upstream:
        return _run_sync_or_async(fn, upstream)

    count = struct.unpack_from("<I", payload, 0)[0]
    offset = 4
    results: list[Any] = []
    for _ in range(count):
        length = struct.unpack_from("<I", payload, offset)[0]
        offset += 4
        results.append(loads(payload[offset : offset + length]))
        offset += length
    return _run_sync_or_async(fn, results)


def _dispatch_generic(_fn: Any, payload: bytes, upstream: list[Any]) -> Any:
    """分发内联 callable 调用（TAG_GENERIC）。

    payload 中的 callable 直接覆盖 handler 入口的 *fn* 参数 — 这是设计意图:
    generic worker 注册了一个哑 handler,真正的执行函数在 payload 里。

    设计上仅用于叶子任务（无前驱），upstream 应为空。
    有依赖的任务走 TAG_POSITIONAL 路径以保留位置信息。
    """
    from actant.task import _run_sync_or_async

    fn, args, kwargs = loads(payload)
    return _run_sync_or_async(fn, *args, **(kwargs or {}))


def _dispatch_positional(fn: Callable[..., Any], payload: bytes, upstream: list[Any]) -> Any:
    """分发位置感知合并调用（TAG_POSITIONAL）。

    payload 格式：cloudpickle((inline_fn, taskref_positions, taskref_kwargs_keys,
                                 concrete_args, concrete_kwargs))
    upstream 由 Rust 前置（TAG_UPSTREAM_PREFIX），按 DAG 边顺序排列：
    先是 args 中的依赖结果（按 positions 顺序），再是 kwargs 中的依赖结果
    （按 kwargs_keys 顺序）。

    这是有依赖任务的统一路径：保留 TaskRef/BranchRef 在原始 args/kwargs 中的位置，
    避免 `combine(1, ref)` 被错位为 `combine(ref, 1)`，也支持
    `combine(ref_a, b=ref_b)` 等 kwargs 中的依赖。
    """
    from actant.task import _run_sync_or_async

    inline_fn, positions, kwargs_keys, concrete_args, concrete_kwargs = loads(payload)
    call_fn = inline_fn if inline_fn is not None else fn
    up_iter = iter(upstream)

    # 重建 args：按 positions 插入上游结果
    total_args = len(concrete_args) + len(positions)
    args: list[Any] = [None] * total_args
    conc_iter = iter(concrete_args)
    pos_set = set(positions)
    for i in range(total_args):
        if i in pos_set:
            args[i] = next(up_iter)
        else:
            args[i] = next(conc_iter)

    # 重建 kwargs：为 kwargs_keys 中的 key 注入上游结果
    final_kwargs = dict(concrete_kwargs)
    for key in kwargs_keys:
        final_kwargs[key] = next(up_iter)

    return _run_sync_or_async(call_fn, *args, **final_kwargs)


# Tag → 分发函数映射
_TAG_DISPATCHERS: dict[int, Callable[..., Any]] = {
    TAG_SINGLE: _dispatch_single,
    TAG_SINGLE_KW: _dispatch_single_kw,
    TAG_GROUP: _dispatch_group,
    TAG_GENERIC: _dispatch_generic,
    TAG_POSITIONAL: _dispatch_positional,
}


def _dispatch_task(
    fn: Callable[..., Any], payload: bytes, cancel_token: Any = None
) -> bytes:
    """反序列化 payload，调用 *fn* 并返回 pickle 结果。

    **内部函数**：仅由 :mod:`actant.app` 在 Worker 进程内调用，用于执行
    Rust 调度器派发过来的任务 payload。**禁止**作为外部 API 入口：
    见模块顶部关于 cloudpickle 任意代码执行的安全警告。

    传输安全由 iroh 提供，使用 TLS 加密的 P2P 连接。

    ``cancel_token`` 是一个 Rust 层的 `CancelToken` 实例，用于通过 `actant.is_cancelled()`
    检查是否需要取消任务。
    """
    from actant._task_context import _clear_context, _set_cancel_token

    _set_cancel_token(cancel_token)
    try:
        # 先解包 Rust 前置的上游结果（若有）
        upstream, inner_payload = unpack_upstream_prefix(payload)

        if not inner_payload:
            # 仅有上游结果，无 default_payload — 命名任务无参调用
            result = _dispatch_empty(fn, upstream)
        else:
            tag = inner_payload[0]
            dispatcher = _TAG_DISPATCHERS.get(tag)
            if dispatcher is None:
                raise ValueError(f"unknown payload tag: 0x{tag:02x}")
            result = dispatcher(fn, inner_payload[1:], upstream)

        return dumps(result)
    finally:
        _clear_context()


# ---------------------------------------------------------------------------
# Rust 类型编码
# ---------------------------------------------------------------------------


def encode_retry(retry_policy: dict[str, Any] | None) -> Any:
    """将 Python dict 重试策略编码为 Rust _RetryPolicy。

    Python API 使用秒（delay, max_delay），Rust 内部使用毫秒（delay_ms, max_delay_ms）。
    此函数负责单位转换。
    """
    if retry_policy is None:
        return None
    from actant.actant import _RetryPolicy

    _defaults = get_default_retry_policy()

    # 同时接受秒级 key（delay, max_delay）和遗留的毫秒级 key（delay_ms, max_delay_ms）
    delay_ms: int | None = retry_policy.get("delay_ms")
    if delay_ms is None and "delay" in retry_policy:
        delay_ms = int(retry_policy["delay"] * 1000)
    if delay_ms is None:
        delay_ms = int(_defaults["delay_ms"])

    max_delay_ms: int | None = retry_policy.get("max_delay_ms")
    if max_delay_ms is None and "max_delay" in retry_policy:
        max_delay_ms = int(retry_policy["max_delay"] * 1000)
    if max_delay_ms is None:
        max_delay_ms = int(_defaults["max_delay_ms"])

    return _RetryPolicy(
        max_retries=retry_policy.get("max_retries", _defaults["max_retries"]),
        delay_ms=delay_ms,
        backoff_multiplier=retry_policy.get("backoff_multiplier", _defaults["backoff_multiplier"]),
        max_delay_ms=max_delay_ms,
    )


def encode_priority(priority: int | str | None) -> int | None:
    """将 Python 优先级归一化为整数。

    Rust 核心层使用 i32 数值表示优先级，无需枚举映射。
    接受整数或字符串（"low"/"normal"/"high"/"critical"）。
    None 表示由 Rust 核心使用默认优先级。
    """
    from actant.config import normalize_priority

    if priority is None:
        return None
    return normalize_priority(priority)


def encode_failure_strategy(failure_strategy: str | None) -> str | None:
    """将 Python 失败策略归一化为字符串标签。

    Rust 核心层使用字符串标签，无需枚举映射。
    None 表示使用默认策略（FAIL_FAST）。
    """
    from actant.config import FailureStrategy

    if failure_strategy is None:
        return None
    return FailureStrategy.normalize(failure_strategy)


def build_payload(args: tuple[Any, ...], kwargs: dict[str, Any] | None) -> bytes:
    """将参数编码为 Rust 运行时 payload，自动过滤 TaskRef 类型的参数。

    TaskRef 参数代表 DAG 中的数据流依赖，运行时通过 DAG 边传递结果，
    因此不需要序列化到 payload 中。
    """
    from actant.task import TaskRef as _TaskRef

    has_args = bool(args)
    has_kwargs = kwargs is not None and len(kwargs) > 0
    if not has_args and not has_kwargs:
        return b""
    elif has_args and not has_kwargs:
        concrete_args = tuple(a for a in args if not isinstance(a, _TaskRef))
        if not concrete_args:
            return b""
        return pack_single(concrete_args)
    else:
        concrete_args = tuple(a for a in args if not isinstance(a, _TaskRef))
        concrete_kwargs = (
            {k: v for k, v in kwargs.items() if not isinstance(v, _TaskRef)} if kwargs else {}
        )
        if not concrete_args and not concrete_kwargs:
            return b""
        return pack_single_kw(concrete_args, concrete_kwargs)
