"""Task 注解上下文：批量设置 task 执行选项。

参考 dask.annotate()，提供上下文管理器为上下文内的 task 调用
批量注入执行选项，支持嵌套（内层覆盖外层）。

合并语义：
    - priority / timeout / retry_policy：内层非 None 覆盖外层
    - tags：列表累加（保持顺序，不去重）
    - metadata：dict 合并（内层覆盖同键）

优先级链：调用参数 > 当前活跃注解 > Task 注册默认值

用法::

    import actant

    @actant.task
    def process(x):
        return x * 2

    @actant.flow
    def my_flow():
        with actant.annotate(priority="high", tags=["gpu"]):
            a = process(1)
            b = process(2)
        # a 和 b 自动继承 priority="high", tags=["gpu"]
        return process(a + b)
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from actant.config import PriorityInput

# 标量字段：合并时取最内层非 None 值
_SCALAR_KEYS: tuple[str, ...] = ("priority", "timeout", "retry_policy")
# 累加字段：列表拼接
_ACCUMULATE_KEYS: tuple[str, ...] = ("tags",)
# 合并字段：dict 浅合并
_MERGE_KEYS: tuple[str, ...] = ("metadata",)

# 线程局部注解栈。使用 threading.local 与 FlowContext 保持一致的同步语义。
# 栈顶（最后一个元素）是最内层的注解。空列表表示无活跃注解。
_local: threading.local = threading.local()


def _stack() -> list[dict[str, Any]]:
    """获取当前线程的注解栈，不存在时初始化为空列表。

    栈顶（最后一个元素）是最内层的注解。空列表表示无活跃注解。
    """
    stack = getattr(_local, "stack", None)
    if stack is None:
        stack = []
        _local.stack = stack
    return stack


def current_annotations() -> dict[str, Any]:
    """获取当前所有活跃注解的合并视图（栈底 → 栈顶逐层合并）。

    返回新的 dict，调用方可安全修改。无活跃注解时返回空 dict。
    """
    return _merge_layers(_stack())


def _merge_layers(layers: list[dict[str, Any]]) -> dict[str, Any]:
    """合并多层注解：从底到顶逐层合并。

    合并规则：
        - 标量字段（priority/timeout/retry_policy）：内层非 None 覆盖外层
        - tags：列表累加（保持顺序，不去重）
        - metadata：dict 合并（内层覆盖同键）
    """
    merged: dict[str, Any] = {}
    for layer in layers:  # 从底到顶
        for key in _SCALAR_KEYS:
            value = layer.get(key)
            if value is not None:
                merged[key] = value
        for key in _ACCUMULATE_KEYS:
            value = layer.get(key)
            if value:
                merged.setdefault(key, [])
                merged[key] = [*merged[key], *value]
        for key in _MERGE_KEYS:
            value = layer.get(key)
            if value:
                merged.setdefault(key, {})
                merged[key] = {**merged[key], **value}
    return merged


def merge_options(
    defaults: dict[str, Any],
    overrides: dict[str, Any],
) -> dict[str, Any]:
    """合并执行选项：defaults < 当前活跃注解 < overrides。

    单一职责：纯函数，不读取全局状态以外的输入。

    Args:
        defaults: Task 注册时的默认选项（priority/timeout/retry_policy/tags/metadata）。
        overrides: 调用时显式传入的参数，None 值表示未传入。

    Returns:
        合并后的选项字典，包含全部 5 个字段。
    """
    annotations = current_annotations()
    result: dict[str, Any] = {}

    # 标量字段：overrides > annotations > defaults
    for key in _SCALAR_KEYS:
        ov = overrides.get(key)
        ann_v = annotations.get(key)
        df = defaults.get(key)
        result[key] = ov if ov is not None else (ann_v if ann_v is not None else df)

    # tags 累加：默认 + 注解 + 显式（与 metadata 三层合并语义一致）
    df_tags = defaults.get("tags") or []
    ann_tags = annotations.get("tags") or []
    ov_tags = overrides.get("tags") or []
    result["tags"] = [*df_tags, *ann_tags, *ov_tags]

    # metadata 合并：默认 < 注解 < 显式
    ann_meta = annotations.get("metadata") or {}
    ov_meta = overrides.get("metadata") or {}
    if ann_meta or ov_meta:
        result["metadata"] = {
            **(defaults.get("metadata") or {}),
            **ann_meta,
            **ov_meta,
        }
    else:
        result["metadata"] = dict(defaults.get("metadata") or {})

    return result


@contextmanager
def annotate(
    *,
    priority: PriorityInput = None,
    timeout: float | None = None,
    retry_policy: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, str] | None = None,
) -> Generator[dict[str, Any], None, None]:
    """为上下文内的 task 调用批量设置执行选项。

    进入时 push 注解到栈，退出时 pop（即使抛出异常也保证 pop）。
    嵌套调用时内层覆盖外层。Task.__call__ / map / reduce 自动从
    栈顶合并注解，调用时显式传入的参数仍优先。

    Args:
        priority: 任务优先级（int/str/None）。
        timeout: 任务超时（秒）。
        retry_policy: 重试策略字典。
        tags: 任务标签列表（嵌套时累加，不去重）。
        metadata: 任务元数据（嵌套时合并，内层覆盖同键）。

    Yields:
        当前活跃注解的合并视图（只读快照，修改不影响栈）。

    Raises:
        TypeError: tags 或 metadata 类型不匹配时。
    """
    if tags is not None and not isinstance(tags, list):
        raise TypeError(f"tags must be list or None, got {type(tags).__name__}")
    if metadata is not None and not isinstance(metadata, dict):
        raise TypeError(f"metadata must be dict or None, got {type(metadata).__name__}")

    layer: dict[str, Any] = {
        "priority": priority,
        "timeout": timeout,
        "retry_policy": retry_policy,
        "tags": list(tags) if tags else None,
        "metadata": dict(metadata) if metadata else None,
    }
    stack = _stack()
    stack.append(layer)
    try:
        # yield 当前合并快照（包括外层注解）
        yield _merge_layers(stack)
    finally:
        stack.pop()
