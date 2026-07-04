"""Flow：工作流定义与 DAG 构建。

Flow 是 @flow 装饰后的函数，在执行时自动追踪 Task 调用之间的依赖关系，
构建 DAG 提交到 Rust 运行时。

核心机制：
    - FlowContext 在 flow 函数执行期间追踪所有 TaskRef 及其依赖
    - TaskRef 作为参数传入另一个 Task 时自动建立 DAG 边（数据流驱动）
    - parallel() 显式声明并行执行（无依赖关系）
    - task.reduce() 实现 chord 语义（并行完成后聚合）
    - branch() / switch() 实现条件分支
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from typing import Any

from actant._serialization import (
    build_payload,
    encode_priority,
    encode_retry,
    pack_generic,
    pack_group,
    pack_positional,
)
from actant.config import FailureStrategyInput
from actant.task import TaskRef, _run_sync_or_async

_UNSET = object()


def _is_taskref_collection(x: Any) -> bool:
    """检测 x 是否是 TaskRef 集合（list/tuple of TaskRef，非空）。

    用于识别 chord 模式：fn([ref1, ref2]) 表示收集所有 ref 结果为 list 后调用 fn。
    """
    if not isinstance(x, (list, tuple)):
        return False
    return bool(x) and all(isinstance(i, TaskRef) for i in x)


def _build_ref_payload(ref: TaskRef) -> bytes:
    """为 TaskRef 构建 default_payload。

    统一设计（Ray 风格）：
    - **chord 模式**（args 含 list/tuple of TaskRef）：走 group 路径（TAG_GROUP）。
      callback 接收一个 list 参数，包含所有前驱结果。upstream 由 Rust 前置。
    - **有 TaskRef/BranchRef 依赖**：走 positional 路径（TAG_POSITIONAL）。
      记录依赖在原始 args 中的位置，运行时由 Rust 前置上游结果，Python 按位置重建。
      这保留了位置信息，避免 `combine(1, ref)` 被错位为 `combine(ref, 1)`。
    - **无依赖（叶子任务）**：
      - inline_func 不为 None（或能从注册表找到）：pack_generic 内联 callable
      - 否则：build_payload 走 named 路径（worker 须预加载业务模块）

   BranchRef 总是走 positional（运行时仅一个分支产生结果，不能被 list 分发或序列化）。
    """
    from actant.task import TaskRef as _TaskRef

    inline_func = ref.inline_func
    if inline_func is None:
        from actant.task import get_global_task

        task_obj = get_global_task(ref.task_name)
        if task_obj is not None and task_obj.func is not None:
            inline_func = task_obj.func

    # chord 模式：args 中有 list/tuple of TaskRef（如 callback.reduce([r1, r2, r3])）
    chord_positions = [i for i, a in enumerate(ref.args) if _is_taskref_collection(a)]
    if chord_positions:
        # callback 接收 list 参数，包含所有前驱结果。
        # upstream 由 Rust 前置，payload 只需标记 group 语义。
        return pack_group([])

    # TaskRef 与 BranchRef 都由 DAG 边传递，统称为"依赖引用"。
    dep_types = (_TaskRef, BranchRef)
    dep_positions = [i for i, a in enumerate(ref.args) if isinstance(a, dep_types)]
    dep_kwargs_keys = (
        [k for k, v in ref.kwargs.items() if isinstance(v, dep_types)]
        if ref.kwargs
        else []
    )

    if dep_positions or dep_kwargs_keys:
        # 有依赖：统一走 positional 路径，保留位置信息。
        concrete_args = tuple(a for a in ref.args if not isinstance(a, dep_types))
        concrete_kwargs = (
            {k: v for k, v in ref.kwargs.items() if not isinstance(v, dep_types)}
            if ref.kwargs
            else {}
        )
        return pack_positional(
            inline_func, dep_positions, dep_kwargs_keys, concrete_args, concrete_kwargs
        )

    # 无依赖：叶子任务。
    if inline_func is not None:
        return pack_generic(inline_func, ref.args, ref.kwargs or {})
    return build_payload(ref.args, ref.kwargs)


def _extract_output_refs(value: Any) -> list[TaskRef]:
    """从 flow 函数返回值中提取 TaskRef 列表。

    支持：单个 TaskRef、BranchRef、TaskRef 元组/列表、嵌套结构。
    """
    if isinstance(value, TaskRef):
        return [value]
    if isinstance(value, BranchRef):
        return [value.if_ref, value.else_ref]
    if isinstance(value, (list, tuple)):
        refs: list[TaskRef] = []
        for item in value:
            refs.extend(_extract_output_refs(item))
        return refs
    return []


_context_local = threading.local()


def _current_flow_context() -> FlowContext | None:
    """获取当前线程的 flow 上下文。"""
    return getattr(_context_local, "flow_context", None)


class FlowContext:
    """flow 执行期间的 DAG 构建上下文。

    追踪所有 TaskRef 及其依赖关系，最终构建 nodes 和 edges
    提交给 Rust 运行时的 submit_dag。
    """

    def __init__(self) -> None:
        self._refs: list[TaskRef] = []
        self._edges: list[tuple[int, int, str | None]] = []
        # 条件分支注册表：condition_tag -> evaluator function
        self._condition_evaluators: dict[str, Callable[[Any], bool]] = {}
        # O(1) 索引：TaskRef.id → 在 _refs 中的位置
        self._ref_index: dict[str, int] = {}
        # flow 函数的返回值（用于 subflow 嵌套时确定输出 TaskRef）
        self._return_value: Any = _UNSET
        # 被 conditional edge 取代的无条件边集合：(from_idx, to_idx)。
        # build_dag 时跳过这些边，避免两个分支同时激活。
        self._superseded_unconditional: set[tuple[int, int]] = set()

    def track(self, ref: TaskRef) -> None:
        """追踪一个新产生的 TaskRef，自动检测依赖关系。"""
        if ref.id in self._ref_index:
            return  # 已追踪

        new_idx: int = len(self._refs)
        self._refs.append(ref)
        self._ref_index[ref.id] = new_idx

        for dep in self._iter_ref_deps(ref):
            dep_idx: int | None = self._ref_index.get(dep.id)
            if dep_idx is not None:
                self._edges.append((dep_idx, new_idx, None))

    def track_branch(
        self,
        condition_ref: TaskRef,
        if_ref: TaskRef,
        else_ref: TaskRef,
        condition_fn: Callable[[Any], bool],
    ) -> None:
        """追踪条件分支：condition_ref 完成后根据 condition_fn 选择分支。

        创建两条条件边：condition_tag 为 "branch_{idx}_true" 和 "branch_{idx}_false"。
        编排循环在运行时评估 condition_fn 并激活对应分支。

        自动追踪产生的无条件边会被替换为条件边，避免两个分支同时激活。
        """
        cond_idx: int | None = self._ref_index.get(condition_ref.id)
        if cond_idx is None:
            raise ValueError("condition TaskRef must be tracked before branching")

        if_idx: int = self._ensure_tracked(if_ref)
        else_idx: int = self._ensure_tracked(else_ref)

        # 将 cond → if/else 的无条件边标记为被取代（由自动跟踪创建）。
        # build_dag 时跳过，避免两个分支同时激活。O(1) 而非重建整个边列表。
        self._superseded_unconditional.add((cond_idx, if_idx))
        self._superseded_unconditional.add((cond_idx, else_idx))

        true_tag = f"branch_{if_idx}_true"
        false_tag = f"branch_{else_idx}_false"

        self._edges.append((cond_idx, if_idx, true_tag))
        self._edges.append((cond_idx, else_idx, false_tag))
        self._condition_evaluators[true_tag] = condition_fn
        # false_tag evaluator is the negation
        self._condition_evaluators[false_tag] = lambda result, fn=condition_fn: not fn(result)  # type: ignore[misc]

    def track_switch(
        self,
        condition_ref: TaskRef,
        cases: list[tuple[str, TaskRef, Callable[[Any], bool]]],
    ) -> None:
        """追踪多路分支：condition_ref 完成后根据条件选择分支。

        每个 case 是 (label, task_ref, condition_fn) 三元组。

        自动追踪产生的无条件边会被替换为条件边，避免所有分支同时激活。
        """
        cond_idx: int | None = self._ref_index.get(condition_ref.id)
        if cond_idx is None:
            raise ValueError("condition TaskRef must be tracked before switching")

        case_indices: list[int] = []
        for label, ref, condition_fn in cases:
            case_idx: int = self._ensure_tracked(ref)
            case_indices.append(case_idx)
            tag = f"switch_{case_idx}_{label}"
            self._edges.append((cond_idx, case_idx, tag))
            self._condition_evaluators[tag] = condition_fn

        # 将 cond → case refs 的无条件边标记为被取代（由自动跟踪创建）。
        for ci in case_indices:
            self._superseded_unconditional.add((cond_idx, ci))

    def _ensure_tracked(self, ref: TaskRef) -> int:
        """确保 TaskRef 已被追踪，返回其索引。"""
        idx = self._ref_index.get(ref.id)
        if idx is not None:
            return idx
        idx = len(self._refs)
        self._refs.append(ref)
        self._ref_index[ref.id] = idx
        return idx

    def _iter_ref_deps(self, ref: TaskRef) -> Generator[TaskRef, None, None]:
        """从 TaskRef 的参数中生成 TaskRef 依赖（生成器，避免列表分配）。"""
        for arg in ref.args:
            if isinstance(arg, TaskRef):
                yield arg
            elif isinstance(arg, BranchRef):
                yield arg.if_ref
                yield arg.else_ref
            elif _is_taskref_collection(arg):
                # chord 模式：list/tuple of TaskRef
                yield from arg
        if ref.kwargs:
            for val in ref.kwargs.values():
                if isinstance(val, TaskRef):
                    yield val
                elif isinstance(val, BranchRef):
                    yield val.if_ref
                    yield val.else_ref
                elif _is_taskref_collection(val):
                    yield from val

    def build_dag(self) -> tuple[list[Any], list[tuple[int, int, str | None]]]:
        """构建 Rust 运行时需要的 DAG 节点和边。

        Python API 使用秒（timeout），Rust 内部使用毫秒（timeout_ms）。
        此方法负责单位转换。

        边格式为 (from_idx, to_idx, condition_tag)，
        condition_tag 为 None 表示无条件边。
        """
        from actant.actant import _DagNode

        nodes: list[Any] = []
        for ref in self._refs:
            payload = _build_ref_payload(ref)
            timeout_ms: int | None = int(ref.timeout * 1000) if ref.timeout is not None else None
            nodes.append(
                _DagNode(
                    name=ref.task_name,
                    payload=payload,
                    retry=encode_retry(ref.retry_policy),
                    timeout_ms=timeout_ms,
                    priority=encode_priority(ref.priority),
                    metadata=dict(ref.metadata) if ref.metadata else None,
                )
            )
        edges = [
            (f, t, c)
            for f, t, c in self._edges
            if not (c is None and (f, t) in self._superseded_unconditional)
        ]
        return nodes, edges

    @property
    def return_value(self) -> Any:
        """flow 函数的返回值（subflow 嵌套时用于确定输出 TaskRef）。"""
        return self._return_value

    @property
    def ref_count(self) -> int:
        """已追踪的 TaskRef 数量。"""
        return len(self._refs)

    @property
    def condition_evaluators(self) -> dict[str, Callable[[Any], bool]]:
        """条件分支评估器注册表（condition_tag -> evaluator）。"""
        return self._condition_evaluators

    def get_ref(self, index: int) -> TaskRef:
        """按索引获取已追踪的 TaskRef。"""
        return self._refs[index]

    def merge_into(self, parent: FlowContext) -> int:
        """将本上下文的 DAG 合并到父上下文中，返回子上下文的偏移量。

        合并 refs、edges（带偏移调整）、condition_evaluators 和 superseded 边集合。
        """
        offset = len(parent._refs)
        for ref in self._refs:
            parent._refs.append(ref)
            parent._ref_index[ref.id] = len(parent._refs) - 1
        for from_idx, to_idx, tag in self._edges:
            parent._edges.append((from_idx + offset, to_idx + offset, tag))
        parent._condition_evaluators.update(self._condition_evaluators)
        for f, t in self._superseded_unconditional:
            parent._superseded_unconditional.add((f + offset, t + offset))
        return offset

    def find_sinks(self) -> list[int]:
        """返回没有出边的节点索引（汇点），跳过被取代的无条件边。"""
        has_outgoing: set[int] = set()
        for f, t, c in self._edges:
            if c is None and (f, t) in self._superseded_unconditional:
                continue
            has_outgoing.add(f)
        return [i for i in range(len(self._refs)) if i not in has_outgoing]


class Flow:
    """工作流定义。@flow 装饰后的函数。

    职责：保存工作流名称和可调用对象，执行时自动追踪依赖构建 DAG。

    文件系统类比：Flow 是文件夹，Task 是文件。
    - 在 Flow 中调用 Task → 创建文件（叶子节点，执行实际工作）
    - 在 Flow 中调用 Flow → 创建子文件夹（DAG 扁平化，自动 subflow 语义）
    - 在 Flow 外调用 Flow → 本地同步执行（调试模式）

    无限嵌套：子 Flow 的返回值决定 subflow 的输出 TaskRef。
    返回单个 TaskRef 时直接返回；返回多个 TaskRef 时返回列表；
    无显式返回值时自动检测 sink 节点作为输出。

    调用行为：
    - Flow 上下文内：返回 flow 函数返回值对应的 TaskRef
    - Flow 上下文外：直接执行并返回结果
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: str | None = None,
        timeout: float | None = None,
        failure_strategy: FailureStrategyInput = None,
    ) -> None:
        self.name = name or getattr(func, "__name__", "unknown_flow")
        self.func: Callable[..., Any] = func
        self._timeout: float | None = timeout
        self._failure_strategy: FailureStrategyInput = failure_strategy

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """调用工作流。

        在 flow 上下文中：执行 subflow 语义（DAG 扁平化），
        返回 flow 函数返回值对应的 TaskRef。

        在 flow 上下文外：本地同步执行，返回结果值。

        用法：
            @flow
            def child(x):
                a = step1(x)
                b = step2(a)
                return b        # b 是 subflow 的输出

            @flow
            def parent(x):
                result = child(x)   # result = b 的 TaskRef
                c = step3(result)   # 自动建立 b → c 依赖
                return c
        """
        ctx: FlowContext | None = _current_flow_context()
        if ctx is None:
            # Flow 上下文外：本地同步执行
            return self.func(*args, **kwargs)

        # Flow 上下文内：执行 subflow 语义（DAG 扁平化）
        return self._embed_subflow(ctx, *args, **kwargs)

    def _embed_subflow(self, parent_ctx: FlowContext, *args: Any, **kwargs: Any) -> TaskRef | list[TaskRef]:
        """将子工作流的 DAG 扁平化嵌入父工作流。

        执行子工作流的函数体，收集其 TaskRef 和边，
        合并到父上下文中。返回值由 flow 函数的 return 语句决定：
        - 返回单个 TaskRef → 直接返回该 TaskRef
        - 返回多个 TaskRef（tuple/list）→ 返回列表
        - 无显式返回 → 自动检测 sink 节点
        """
        child_ctx = FlowContext()
        old_ctx = getattr(_context_local, "flow_context", None)
        _context_local.flow_context = child_ctx
        try:
            result = _run_sync_or_async(self.func, *args, **kwargs)
            child_ctx._return_value = result
        finally:
            _context_local.flow_context = old_ctx

        if child_ctx.ref_count == 0:
            raise ValueError(f"subflow '{self.name}' produced no tasks")

        # 合并子 DAG 到父上下文（带偏移调整）
        child_ctx.merge_into(parent_ctx)

        # 从 flow 函数返回值确定输出 TaskRef
        output_refs = _extract_output_refs(child_ctx.return_value)

        if output_refs:
            if len(output_refs) == 1:
                return output_refs[0]
            return output_refs

        # 无 TaskRef 返回值 → 回退到 sink 检测
        sink_indices = child_ctx.find_sinks()
        if len(sink_indices) == 1:
            return child_ctx.get_ref(sink_indices[0])
        return [child_ctx.get_ref(i) for i in sink_indices]

    def _build_dag(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[
        list[tuple[Any, ...]], list[tuple[int, int, str | None]], dict[str, Callable[[Any], bool]]
    ]:
        """执行 flow 函数体并构建 DAG。

        在提交到 Rust 运行时前进行循环检测，发现循环时抛出
        ``ValueError`` 并附带可读的循环路径（如 ``a -> b -> c -> a``）。

        Returns:
            (nodes, edges, condition_evaluators)
        """
        ctx = FlowContext()
        old_ctx = getattr(_context_local, "flow_context", None)
        _context_local.flow_context = ctx
        try:
            _run_sync_or_async(self.func, *args, **kwargs)
        finally:
            _context_local.flow_context = old_ctx

        nodes, edges = ctx.build_dag()
        if not nodes:
            raise ValueError("flow produced no tasks")

        # 循环检测：提交前快速失败，给出可读错误路径。
        # Python 侧检测比 Rust 侧报错更易调试（Rust 仅返回通用拓扑错误）。
        from actant._dag import detect_cycle, format_cycle_path

        cycle = detect_cycle(len(nodes), edges)
        if cycle is not None:
            node_names = [str(n.name) for n in nodes]
            path = format_cycle_path(node_names, cycle)
            raise ValueError(
                f"flow '{self.name}' contains a cycle: {path}"
            )

        return nodes, edges, ctx.condition_evaluators

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """本地执行模式：同步运行工作流，不提交到 Rust 运行时。

        所有 Task 调用直接执行并返回实际值（非 TaskRef），
        便于调试和单元测试。map/reduce/parallel 在本地模式下
        自动退化为同步操作。

        Returns:
            flow 函数的返回值。
        """
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<Flow {self.name}>"


def flow(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    timeout: float | None = None,
    failure_strategy: FailureStrategyInput = None,
) -> Flow | Callable[..., Flow]:
    """工作流装饰器。

    用法：
        @flow
        def my_workflow(x, y):
            a = add(x, y)
            b = multiply(a, 2)
            return b

        @flow(name="custom", timeout=30.0)
        def my_workflow(x, y):
            ...
    """
    if func is None:

        def decorator(f: Callable[..., Any]) -> Flow:
            return Flow(
                f,
                name=name,
                timeout=timeout,
                failure_strategy=failure_strategy,
            )

        return decorator

    return Flow(
        func, name=name, timeout=timeout, failure_strategy=failure_strategy
    )

def parallel(*refs: TaskRef) -> list[TaskRef] | list[Any]:
    """显式并行：声明所有 TaskRef 同时执行，互不依赖。

    在 flow 上下文中标记 TaskRef 为并行执行；
    在 flow 上下文外直接返回值列表（用于 Flow.run() 本地调试模式）。
    """
    if not refs:
        return []

    ctx: FlowContext | None = _current_flow_context()
    if ctx is None:
        # 本地模式：值已经是实际结果，直接返回
        return list(refs)

    result: list[TaskRef] = []
    for r in refs:
        if not isinstance(r, TaskRef):
            raise TypeError(f"parallel() arguments must be TaskRef, got {type(r).__name__}")
        ctx.track(r)
        result.append(r)
    return result


class BranchRef:
    """条件分支的延迟结果引用。

    由 branch() 返回，表示 if_ref 或 else_ref 之一将在运行时执行。
    BranchRef 可作为参数传入另一个 Task 调用，自动建立对两个分支的依赖边。
    """

    __slots__ = ("_else_ref", "_if_ref")

    def __init__(self, if_ref: TaskRef, else_ref: TaskRef) -> None:
        self._if_ref = if_ref
        self._else_ref = else_ref

    @property
    def if_ref(self) -> TaskRef:
        return self._if_ref

    @property
    def else_ref(self) -> TaskRef:
        return self._else_ref

    def __repr__(self) -> str:
        return f"<BranchRef {self._if_ref.task_name} | {self._else_ref.task_name}>"

def branch(
    condition_ref: TaskRef,
    condition_fn: Callable[[Any], bool],
    if_ref: TaskRef,
    else_ref: TaskRef,
) -> BranchRef | Any:
    """条件分支：根据 condition_fn 的返回值选择执行 if_ref 或 else_ref。

    condition_ref 的结果会传给 condition_fn 进行评估。
    condition_fn 返回 True 时执行 if_ref，返回 False 时执行 else_ref。

    用法：
        @flow
        def my_workflow(x):
            result = check(x)
            br = branch(result, lambda r: r > 0, process_positive(result), process_negative(result))
            # br 可传入下游 task，自动建立对两个分支的依赖

    在 flow 上下文外调用时返回 BranchRef 包装，condition_fn 不会被调用，
    分支选择推迟到运行时由编排循环评估。

    Args:
        condition_ref: 产生条件判断值的 TaskRef。
        condition_fn: 接收 condition_ref 结果，返回 bool 的函数。
        if_ref: 条件为 True 时执行的 TaskRef。
        else_ref: 条件为 False 时执行的 TaskRef。

    Returns:
        BranchRef：表示两个分支之一的延迟引用，可作为参数传入下游 Task。
    """
    ctx: FlowContext | None = _current_flow_context()
    if ctx is None:
        # 本地模式：返回 BranchRef 包装，与 flow 模式保持一致。
        # BranchRef 同时持有 if_ref / else_ref，作为下游 task 参数时
        # 自动建立对两个分支的依赖边，运行时由编排循环评估激活哪一支。
        # 注意：本地模式下 condition_fn 不会被调用，分支选择推迟到运行时。
        return BranchRef(if_ref, else_ref)

    ctx.track_branch(condition_ref, if_ref, else_ref, condition_fn)
    return BranchRef(if_ref, else_ref)

def switch(
    condition_ref: TaskRef,
    *cases: tuple[str, TaskRef, Callable[[Any], bool]],
) -> list[TaskRef]:
    """多路分支：根据多个条件函数选择执行路径。

    每个 case 是 (label, task_ref, condition_fn) 三元组。
    编排循环在运行时评估所有 condition_fn，激活条件为 True 的分支。

    用法：
        @flow
        def my_workflow(x):
            result = classify(x)
            switch(result,
                ("fast", process_fast(result), lambda r: r == "fast"),
                ("slow", process_slow(result), lambda r: r == "slow"),
            )

    在 flow 上下文外调用时直接评估条件并返回匹配的分支列表。

    Args:
        condition_ref: 产生条件判断值的 TaskRef。
        *cases: (label, task_ref, condition_fn) 三元组。

    Returns:
        所有 case 的 TaskRef 列表（实际分支由编排循环在运行时选择）。
    """
    ctx: FlowContext | None = _current_flow_context()
    if ctx is None:
        # 本地模式：直接评估条件
        return [ref for _, ref, fn in cases if fn(condition_ref)]

    ctx.track_switch(condition_ref, list(cases))
    return [ref for _, ref, _ in cases]


def _find_sinks(
    node_count: int,
    edges: list[tuple[int, int, str | None]],
) -> list[int]:
    """查找所有没有出边的节点索引（即“汇点”）。"""
    has_outgoing = set()
    for from_idx, _, _ in edges:
        has_outgoing.add(from_idx)

    return [i for i in range(node_count) if i not in has_outgoing]
