"""Flow DAG 编译化：将 ``@flow(compiled=True)`` 函数编译为静态 DAG 调度计划。

## 设计

每个 ``@flow(compiled=True)`` 函数首次执行时，通过 monkey-patching
``Task._submit`` 捕获提交序列（任务名、参数依赖、kwargs、target_node）。
执行结束后，将序列编译为静态 DAG（节点 + 边），缓存到
``func.__actant_compiled_dag__``。

后续执行时直接复用 DAG：按拓扑序并行调度无依赖的节点，依赖节点等待
上游完成后 submit。这避免了重复执行 flow 函数体的 Python 解释开销，
且使调度器能预先看到全图，做更优的资源分配。

## 限制

DAG 编译假设 flow 函数体是"纯提交"——提交序列仅依赖输入参数，不依赖
task 的运行时结果。若 flow 体包含基于 ``result()`` 的条件分支，编译路径
会自动回退到命令式执行（首次执行时检测到 result 调用即放弃编译）。

## 线程安全

DAG 编译图缓存于 ``func.__actant_compiled_dag__`` 属性，由 module-level
lock 保护编译期；运行期读取不加锁（DAG 在编译后不可变，多线程并发读取安全）。
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

from actant.task._async_result import AsyncResult

_logger = logging.getLogger("actant.flow")

# Module-level lock：保护同一 flow 函数的首次编译。
# 不同 flow 函数可并发编译，仅同函数串行化（避免重复 trace）。
_compile_lock = threading.Lock()

@dataclass(frozen=True)
class _DagNode:
    """DAG 节点：对应一次 ``Task.submit`` 调用。

    通过 ``param_deps`` 隐式表达对上游节点的依赖：若 ``args[i]`` /
    ``kwargs[k]`` 是另一节点的输出（AsyncResult 句柄），则本节点依赖
    该节点。编译期将句柄引用转换为 ``task_id`` 索引。
    """

    # 节点在提交序列中的序号（0-based），用作唯一标识。
    index: int
    # Task 对象的 name（去重/调试用）。
    task_name: str
    # 调用 submit 的 Task 对象引用（运行期复用）。
    task_obj_ref: Any
    # 参数模板：``args`` 中的 ``AsyncResult`` 替换为占位符 ``_ArgRef(index)``，
    # 运行期再替换为实际 handle。
    args_template: tuple[Any, ...]
    kwargs_template: dict[str, Any]
    # 提交目标（submit_to 用），``None`` 表示默认调度。
    target_node: str | None = None
    target_endpoint_addr: str | None = None
    # 依赖的上游节点 index 列表（基于参数中的 AsyncResult 引用）。
    upstream_indices: tuple[int, ...] = ()


@dataclass(frozen=True)
class _DagEdge:
    """DAG 边：上游节点 → 下游节点。"""

    src: int
    dst: int


@dataclass
class _CompiledDag:
    """编译后的 flow DAG：节点列表 + 拓扑分层。

    ``topo_layers`` 是按依赖关系分组的节点 index 列表：layer 0 无依赖，
    layer N 依赖 layer < N 的某些节点。每层内的节点可并行 submit。
    """

    nodes: list[_DagNode] = field(default_factory=list)
    edges: list[_DagEdge] = field(default_factory=list)
    # 拓扑分层：layer 0 = 无依赖节点；layer k = 依赖 layer < k 的节点。
    topo_layers: list[list[int]] = field(default_factory=list)
    # flow 函数的返回值引用：返回 None / 常量 / 某个节点的 AsyncResult。
    # ``("const", value)`` 表示返回常量；``("node", index)`` 表示返回某节点结果。
    # ``("gather", indices)`` 表示返回多个节点结果的列表（对应 actant.gather）。
    return_kind: str = "const"
    return_value: Any = None
    return_node_index: int = -1
    # return_kind="gather" 时使用：返回 [node_handles[i].result() for i in indices]。
    return_node_indices: list[int] = field(default_factory=list)

    def is_compiled(self) -> bool:
        """是否已成功编译（至少有一个节点）。"""
        return bool(self.nodes)


@dataclass(frozen=True)
class _ArgRef:
    """参数占位符：引用某个上游节点的输出。

    编译期将 ``AsyncResult`` 替换为 ``_ArgRef(index)``，运行期再将
    ``_ArgRef(index)`` 替换为实际 ``AsyncResult`` 句柄。
    """

    node_index: int


@dataclass(frozen=True)
class _FlowArgRef:
    """参数占位符：引用 flow 函数的某个输入参数。

    编译期将 ``_FlowInput(index)`` 替换为 ``_FlowArgRef(index)``，
    运行期再将 ``_FlowArgRef(index)`` 替换为实际传入的 ``args[index]``。
    """

    arg_index: int


class _FlowInput:
    """Sentinel：trace 期替换 flow 输入参数，用于识别参数依赖。

    flow 函数体若对输入参数做运算（如 ``a + 1``），会触发 ``__add__`` 抛
    ``TypeError``，导致 trace 失败回退命令式。这确保只有"纯提交"flow
    （参数仅传给 submit，不运算）才能编译。

    flow 体若直接返回输入参数（``return a``），finalize 识别为
    ``return_kind="flow_arg"``，运行期返回 ``args[index]``。
    """

    __slots__ = ("_index",)

    def __init__(self, index: int) -> None:
        self._index = index

    @property
    def index(self) -> int:
        return self._index

    def __repr__(self) -> str:
        return f"_FlowInput({self._index})"

    # 显式禁用所有运算符：flow 体对输入参数运算意味着依赖运行时值，
    # 无法静态编译。抛 TypeError 使 trace 失败回退命令式。
    def _fail(self, *args: Any, **kwargs: Any) -> Any:
        raise TypeError(
            "_FlowInput does not support operations; flow body must only "
            "pass flow inputs directly to task.submit() without computation"
        )

    __add__ = __radd__ = _fail
    __sub__ = __rsub__ = _fail
    __mul__ = __rmul__ = _fail
    __div__ = __rdiv__ = _fail
    __truediv__ = __rtruediv__ = _fail
    __floordiv__ = __rfloordiv__ = _fail
    __mod__ = __rmod__ = _fail
    __pow__ = __rpow__ = _fail
    __lt__ = __le__ = __gt__ = __ge__ = __eq__ = __ne__ = _fail
    __getitem__ = _fail
    __call__ = _fail


class _FlowTracer:
    """捕获 flow 函数体内的 ``Task.submit`` 调用序列。

    通过 monkey-patching 当前线程 ``Task._submit`` 实现：

    1. 调用 ``enable()`` 进入 trace 模式，记录所有 submit 调用。
    2. 每次 submit 时，将 (task, args, kwargs, target) 记录到 ``submissions``。
       参数中的 ``AsyncResult`` 替换为 ``_ArgRef(index)``，建立依赖。
    3. flow 函数返回时，``finalize()`` 将返回值转换为 ``("node", index)``
       或 ``("const", value)``，并构建 DAG 节点 + 拓扑分层。
    4. 若 trace 期间检测到 ``result()`` 调用（条件分支信号），设置
       ``aborted=True``，编译放弃，flow 回退命令式执行。

    线程局部：tracer 仅在 enable 的线程内生效，避免污染其他 flow。
    """

    def __init__(self) -> None:
        self.submissions: list[dict[str, Any]] = []
        self.aborted = False
        self._enabled = False
        # 节点 index → 已分配的 task_id 占位（仅用于依赖图构建）。
        # 实际 task_id 在运行期由 Task._submit 重新生成。
        self._index_to_placeholder: dict[int, AsyncResult] = {}

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def is_enabled(self) -> bool:
        return self._enabled

    def record_submission(
        self,
        task_obj: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        target_node: str | None,
        target_endpoint_addr: str | None,
    ) -> AsyncResult:
        """记录一次 submit 调用，返回占位 AsyncResult 供下游引用。

        参数中的 ``AsyncResult`` 被替换为 ``_ArgRef(index)``，建立依赖。
        """
        index = len(self.submissions)
        # 解析参数中的 AsyncResult 引用，记录上游依赖。
        upstream_indices: list[int] = []
        # 占位 task_id：仅用于 tracer 内部追踪，运行期会重新生成。
        placeholder_id = f"__tracer_node_{index}__"

        def _resolve_arg(v: Any) -> Any:
            # 检测 AsyncResult：若是已记录节点的占位，转换为 _ArgRef。
            if isinstance(v, AsyncResult):
                # 查找该 handle 对应的节点 index。
                for i, h in self._index_to_placeholder.items():
                    if h is v:
                        if i not in upstream_indices:
                            upstream_indices.append(i)
                        return _ArgRef(i)
                # 未知 handle：可能是 flow 外部传入的，无法追踪。
                # 标记 aborted，回退命令式。
                self.aborted = True
                return v
            # 检测 _FlowInput：flow 输入参数 sentinel。
            # 替换为 _FlowArgRef，运行期从新 args 中取值。
            if isinstance(v, _FlowInput):
                return _FlowArgRef(v.index)
            return v

        new_args = tuple(_resolve_arg(a) for a in args)
        new_kwargs = {k: _resolve_arg(v) for k, v in kwargs.items()}

        # 创建占位 handle：仅用于建立依赖关系，不会真正提交。
        # 使用一个 dummy AsyncResult，state 标记为 "completed" 避免下游阻塞。
        placeholder = AsyncResult(
            placeholder_id, workflow_id="__tracer__",
        )
        # 标记为 completed，避免下游解析时阻塞。
        # 传 None（而非 b""）：placeholder 不会被 result() 调用，仅用于 is_set()=True。
        # 用 _set_result_obj 标记 _result_is_obj=True，避免 None 被误判为 bytes 路径。
        placeholder._set_result_obj(None)
        self._index_to_placeholder[index] = placeholder

        self.submissions.append({
            "index": index,
            "task_obj": task_obj,
            "task_name": task_obj.name,
            "args_template": new_args,
            "kwargs_template": new_kwargs,
            "target_node": target_node,
            "target_endpoint_addr": target_endpoint_addr,
            "upstream_indices": tuple(upstream_indices),
            "placeholder_handle": placeholder,
        })
        return placeholder

    def record_result_call(self) -> None:
        """检测到 ``AsyncResult.result()`` 调用：放弃编译。

        flow 体若调用 result()，意味着依赖运行时结果做控制流，
        无法静态编译。设置 aborted=True，编译路径回退。
        """
        self.aborted = True

    def finalize(self, return_value: Any) -> _CompiledDag:
        """根据捕获的提交序列构建 ``_CompiledDag``。

        Args:
            return_value: flow 函数的返回值（可能是常量或某节点占位 handle）。

        Returns:
            编译后的 DAG。若 ``self.aborted``，返回空 DAG。
        """
        if self.aborted or not self.submissions:
            return _CompiledDag()

        dag = _CompiledDag()
        for sub in self.submissions:
            node = _DagNode(
                index=sub["index"],
                task_name=sub["task_name"],
                task_obj_ref=sub["task_obj"],
                args_template=sub["args_template"],
                kwargs_template=sub["kwargs_template"],
                target_node=sub["target_node"],
                target_endpoint_addr=sub["target_endpoint_addr"],
                upstream_indices=sub["upstream_indices"],
            )
            dag.nodes.append(node)
            for up_idx in node.upstream_indices:
                dag.edges.append(_DagEdge(src=up_idx, dst=node.index))

        # 解析返回值：常量 / 单节点引用 / gather 列表 / flow 输入参数。
        if isinstance(return_value, AsyncResult):
            for i, h in self._index_to_placeholder.items():
                if h is return_value:
                    dag.return_kind = "node"
                    dag.return_node_index = i
                    break
            else:
                # 返回的 handle 不属于本 flow：视为常量（罕见情况）。
                dag.return_kind = "const"
                dag.return_value = None
        elif isinstance(return_value, _FlowInput):
            # flow 体直接返回输入参数：return_kind="flow_arg"。
            dag.return_kind = "flow_arg"
            dag.return_node_index = return_value.index
        elif (
            isinstance(return_value, (list, tuple))
            and return_value
            and all(isinstance(v, AsyncResult) for v in return_value)
        ):
            # trace 期 actant.gather 返回 list of placeholder handles
            # （由 _patched_gather 截获，不调 result）。
            # 编译为 return_kind="gather"：运行期返回各节点结果列表。
            indices: list[int] = []
            for rv in return_value:
                matched = False
                for i, h in self._index_to_placeholder.items():
                    if h is rv:
                        indices.append(i)
                        matched = True
                        break
                if not matched:
                    # 列表中有未知 handle：无法静态编译。
                    _logger.debug(
                        "flow finalize: gather return list contains "
                        "unknown handle, falling back"
                    )
                    return _CompiledDag()
            dag.return_kind = "gather"
            dag.return_node_indices = indices
        else:
            dag.return_kind = "const"
            dag.return_value = return_value

        # 构建拓扑分层（Kahn 算法的分层变体）。
        dag.topo_layers = self._build_topo_layers(dag)
        return dag

    @staticmethod
    def _build_topo_layers(dag: _CompiledDag) -> list[list[int]]:
        """按依赖关系将节点分为拓扑层。

        layer 0 = 无上游依赖的节点；layer k = 所有上游在 layer < k 中的节点。
        """
        # 计算每个节点的入度（上游依赖数）。
        in_degree: dict[int, int] = {
            n.index: len(n.upstream_indices) for n in dag.nodes
        }
        # 反向邻接表：上游节点 → 依赖它的下游节点列表。
        adj: dict[int, list[int]] = {n.index: [] for n in dag.nodes}
        for edge in dag.edges:
            adj[edge.src].append(edge.dst)

        layers: list[list[int]] = []
        # 初始层：所有入度为 0 的节点。
        current = [idx for idx, d in in_degree.items() if d == 0]
        processed = 0
        while current:
            layers.append(sorted(current))  # 排序保证确定性
            processed += len(current)
            next_layer: list[int] = []
            for src in current:
                for dst in adj[src]:
                    in_degree[dst] -= 1
                    if in_degree[dst] == 0:
                        next_layer.append(dst)
            current = next_layer
        # 检测环：若处理节点数 < 总节点数，存在环（不应发生，因为依赖是 DAG）。
        if processed != len(dag.nodes):
            _logger.warning(
                "CompiledDag: cycle detected in flow DAG, falling back"
            )
            return []
        return layers


_tracer_local = threading.local()


def _get_tracer() -> _FlowTracer | None:
    """返回当前线程活跃的 tracer，未启用返回 None。"""
    return getattr(_tracer_local, "tracer", None)


def _set_tracer(tracer: _FlowTracer | None) -> None:
    """设置当前线程的 tracer。``None`` 表示清除。"""
    if tracer is None:
        if hasattr(_tracer_local, "tracer"):
            del _tracer_local.tracer
    else:
        _tracer_local.tracer = tracer



_original_task_submit: Any = None
_original_async_result: Any = None
_original_gather: Any = None
_patch_lock = threading.Lock()
_patched = False


def _install_patches() -> None:
    """全局安装 Task._submit / AsyncResult.result / actant.gather 的 tracer 钩子。

    仅安装一次（module 加载时），后续通过线程局部 tracer 控制是否启用。
    未设置 tracer 时，钩子直接调用原方法，开销仅为一次 threadlocal 读取。
    """
    global _original_task_submit, _original_async_result, _original_gather, _patched
    with _patch_lock:
        if _patched:
            return

        from actant.task._gather import gather as _gather_func
        from actant.task._task_obj import Task

        _original_task_submit = Task._submit

        def _patched_submit(
            self: Task,
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
            *,
            target_node: str | None,
            target_endpoint_addr: str | None,
        ) -> AsyncResult:
            tracer = _get_tracer()
            if tracer is None or not tracer.is_enabled():
                # 非 trace 模式：直接调用原方法。
                return cast(
                    AsyncResult,
                    _original_task_submit(
                        self, args, kwargs,
                        target_node=target_node,
                        target_endpoint_addr=target_endpoint_addr,
                    ),
                )
            # trace 模式：记录提交，返回占位 handle。
            return tracer.record_submission(
                self, args, kwargs, target_node, target_endpoint_addr,
            )

        Task._submit = _patched_submit  # type: ignore[method-assign]

        # AsyncResult.result patch：仅当 tracer 启用时检测 result() 调用，
        # 否则直接调用原方法（threadlocal 读取开销可忽略）。
        _original_async_result = AsyncResult.result

        def _patched_result(
            self: AsyncResult, timeout: float | None = None,
        ) -> Any:
            tracer = _get_tracer()
            if tracer is not None and tracer.is_enabled():
                # flow 内调用 result()：依赖运行时结果，无法静态编译。
                tracer.record_result_call()
            return _original_async_result(self, timeout=timeout)

        AsyncResult.result = _patched_result  # type: ignore[method-assign]

        # actant.gather patch：trace 期直接返回输入 handles 列表，
        # 不调用 result()（避免触发 aborted 标记和空 payload 反序列化）。
        # finalize 识别 list of AsyncResult placeholder，编译为 return_kind="gather"。
        _original_gather = _gather_func

        def _patched_gather(
            *handles: AsyncResult,
            timeout: float | None = None,
            return_exceptions: bool = False,
        ) -> list[Any]:
            tracer = _get_tracer()
            if tracer is None or not tracer.is_enabled():
                # 非 trace 模式：直接调用原 gather。
                return cast(
                    list[Any],
                    _original_gather(
                        *handles, timeout=timeout, return_exceptions=return_exceptions,
                    ),
                )
            # trace 模式：返回 handles 列表占位。
            # 若 handles 全是本 flow 的 placeholder，finalize 编译为 "gather" 返回。
            # 若包含未知 handle，flow 体后续用到返回值会触发异常，回退命令式。
            return list(handles)

        # patch actant.task._gather.gather 模块级函数。
        # actant.gather 和 actant.task.gather 都是同一函数对象的引用，
        # 替换模块属性即可影响所有调用路径。
        import actant
        import actant.task
        from actant.task import _gather as _gather_mod
        _gather_mod.gather = _patched_gather
        actant.task.gather = _patched_gather  # type: ignore[attr-defined]
        actant.gather = _patched_gather

        _patched = True


def _instantiate_arg(
    value: Any,
    node_handles: dict[int, AsyncResult],
    flow_args: tuple[Any, ...],
) -> Any:
    """将参数模板中的 ``_ArgRef`` / ``_FlowArgRef`` 替换为实际值。

    ``_ArgRef(index)`` → ``node_handles[index]``（上游节点 AsyncResult）
    ``_FlowArgRef(index)`` → ``flow_args[index]``（flow 输入参数）
    其他值原样返回。
    """
    if isinstance(value, _ArgRef):
        return node_handles[value.node_index]
    if isinstance(value, _FlowArgRef):
        return flow_args[value.arg_index]
    return value


def _execute_compiled_dag(
    dag: _CompiledDag,
    runtime: Any,
    flow_args: tuple[Any, ...],
    flow_kwargs: dict[str, Any],
) -> Any:
    """按编译后的 DAG 调度任务，返回最终结果。

    按拓扑层顺序 submit：同层节点并行 submit（不等待），跨层节点通过
    ``AsyncResult`` 参数依赖自动等待上游完成。

    Args:
        dag: 已编译的 DAG。
        runtime: 当前活跃 Runtime。
        flow_args: flow 函数的位置参数（用于替换 ``_ArgRef`` 之外的常量）。
        flow_kwargs: flow 函数的关键字参数。

    Returns:
        flow 函数的返回值（常量或某节点的 AsyncResult.result()）。
    """

    # node_index → 实际 AsyncResult 句柄（运行期 submit 后填充）。
    node_handles: dict[int, AsyncResult] = {}

    # 按 topo_layers 顺序 submit。同层节点可并行 submit（无相互依赖），
    # 但 submit 本身是同步的（调用 core.submit_task），并行仅体现在
    # 任务执行阶段（Worker 拉取后并发执行）。
    for layer in dag.topo_layers:
        for node_idx in layer:
            node = dag.nodes[node_idx]
            # 实例化参数：将 _ArgRef 替换为实际 handle。
            # 注意：上游 handle 此时已 submit 但可能未完成。这正是
            # AsyncResult 的依赖解析语义——下游 submit 时 _resolve_value
            # 会阻塞等待上游结果。但编译模式下我们希望"并行 submit，
            # 不阻塞"：因此 _ArgRef 直接传递 handle，由 Task._submit 内的
            # _resolve_value 自动处理依赖等待。
            real_args = tuple(
                _instantiate_arg(a, node_handles, flow_args)
                for a in node.args_template
            )
            real_kwargs = {
                k: _instantiate_arg(v, node_handles, flow_args)
                for k, v in node.kwargs_template.items()
            }
            # 提交任务。Runtime 上下文由调用方（flow wrapper）已设置，
            # 这里复用即可。
            handle = _original_task_submit(
                node.task_obj_ref, real_args, real_kwargs,
                target_node=node.target_node,
                target_endpoint_addr=node.target_endpoint_addr,
            )
            node_handles[node_idx] = handle

    # 根据 return_kind 返回结果。
    if dag.return_kind == "node" and dag.return_node_index >= 0:
        return node_handles[dag.return_node_index].result()
    if dag.return_kind == "flow_arg" and dag.return_node_index >= 0:
        # flow 体直接返回输入参数：从 flow_args 取对应位置。
        return flow_args[dag.return_node_index]
    if dag.return_kind == "gather" and dag.return_node_indices:
        # 对应 trace 期 actant.gather(...) 返回值。
        # 运行期复用原 gather 语义：并行等待所有 handle，返回结果列表。
        # _original_gather 在 _install_patches 中保存，非 trace 模式下
        # 直接调用原 gather（_patched_gather 内部会判断 tracer 状态）。
        handles = [node_handles[i] for i in dag.return_node_indices]
        return _original_gather(*handles)
    return dag.return_value


def _try_compile_flow(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    workflow_id: str,
) -> _CompiledDag | None:
    """首次执行时 trace flow 函数体，构建 DAG。

    若 flow 体调用 result() 或包含外部 handle 依赖，返回 ``None``
    表示编译失败，调用方应回退命令式执行。

    Args:
        func: 被编译的 flow 函数。
        args / kwargs: flow 的输入参数。
        workflow_id: 当前 workflow_id（仅用于日志）。

    Returns:
        编译后的 DAG，或 ``None``（编译失败/无 submit 调用）。
    """
    # 设置 FlowContext：trace 期 Task._submit 仍会读取 workflow_id，
    # 但被 tracer 截获不实际提交。这样 trace 期 handle 的 workflow_id
    # 与运行期一致，便于调试。
    from actant.flow import _FlowContext

    # 用 _FlowInput sentinel 替换 flow 输入参数。
    # trace 期 func 内部若把参数传给 submit，record_submission 识别
    # _FlowInput 并记录为 _FlowArgRef，运行期从新 args 取实际值。
    # 若 func 对参数做运算（如 a+1），_FlowInput.__add__ 抛 TypeError，
    # trace 失败回退命令式——这是预期行为：运算依赖运行时值无法静态编译。
    sentinel_args = tuple(_FlowInput(i) for i in range(len(args)))
    # TODO: kwargs 输入参数的编译化未启用——多数 flow 使用位置参数。
    #     当前实现：kwargs 非空时直接 abort 编译回退命令式，确保正确性。
    #     未来若需支持 kwargs 引用，可引入 _FlowKwargRef(key) sentinel
    #     与独立的 kwargs 命名空间。
    if kwargs:
        _logger.debug(
            "flow %s: compilation aborted (kwargs not supported yet)",
            workflow_id,
        )
        return None

    tracer = _FlowTracer()
    tracer.enable()
    _set_tracer(tracer)
    try:
        with _FlowContext(workflow_id):
            return_value = func(*sentinel_args)
    except Exception:
        # trace 期间抛异常：放弃编译，让命令式路径重新抛出。
        _set_tracer(None)
        return None
    finally:
        tracer.disable()
        _set_tracer(None)

    if tracer.aborted:
        _logger.debug(
            "flow %s: compilation aborted (result() called or external handle)",
            workflow_id,
        )
        return None
    dag = tracer.finalize(return_value)
    if not dag.is_compiled():
        _logger.debug(
            "flow %s: no submissions captured, skipping compilation",
            workflow_id,
        )
        return None
    return dag


def run_compiled_flow(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    workflow_id: str,
    *,
    dag_attr: str = "__actant_compiled_dag__",
    cache_target: Any = None,
) -> tuple[bool, Any]:
    """执行 ``@flow(compiled=True)`` 函数，使用编译 DAG 加速。

    Args:
        func: 被装饰的 flow 函数（已包装）。
        args / kwargs: flow 的输入参数。
        workflow_id: 当前 workflow_id。
        dag_attr: 缓存 DAG 的属性名。
        cache_target: 缓存 DAG 的目标对象（默认 ``func``）。
            ``@flow`` 装饰器传入 wrapper 函数，使外部可通过
            ``wrapper.__actant_compiled_dag__`` 访问缓存的 DAG。
            ``@wraps`` 仅在装饰时复制一次 ``__dict__``，后续 func 上
            新增属性不会自动同步到 wrapper，因此需显式设置。

    Returns:
        ``(used_compiled, result)``：
        - ``used_compiled=True``：使用了编译 DAG，``result`` 为最终结果。
        - ``used_compiled=False``：编译未启用或失败，``result`` 为命令式
          执行结果，调用方应执行命令式路径。
    """
    from actant.flow import _FlowContext

    if cache_target is None:
        cache_target = func

    # 1. 检查缓存的 DAG。
    cached_dag: _CompiledDag | None = getattr(cache_target, dag_attr, None)
    if cached_dag is None:
        # cache_target（wrapper）上未命中时回退到 func 检查：
        # @wraps 装饰时仅一次性复制 __dict__，后续在 func 上设置的
        # DAG 缓存属性不会自动同步到 wrapper，故需双查。
        cached_dag = getattr(func, dag_attr, None)
    if cached_dag is not None and cached_dag.is_compiled():
        # 复用缓存的 DAG：在 FlowContext 内执行 _execute_compiled_dag，
        # 使其内部 _original_task_submit 能正确读取 workflow_id。
        with _FlowContext(workflow_id):
            return True, _execute_compiled_dag(cached_dag, None, args, kwargs)

    # 2. 首次执行：编译 DAG。
    with _compile_lock:
        # 双检：可能在等锁期间已被其他线程编译。
        cached_dag = getattr(cache_target, dag_attr, None)
        if cached_dag is None:
            cached_dag = getattr(func, dag_attr, None)
        if cached_dag is not None and cached_dag.is_compiled():
            with _FlowContext(workflow_id):
                return True, _execute_compiled_dag(cached_dag, None, args, kwargs)

        # trace 编译。trace 期 _try_compile_flow 已设置 FlowContext。
        dag = _try_compile_flow(func, args, kwargs, workflow_id)
        if dag is None or not dag.is_compiled():
            return False, None
        # 双写缓存到 cache_target 与 func：外部代码可能通过任一对象访问
        # 缓存的 DAG（如直接调用 func 或通过 wrapper），双写保证两者一致。
        for target in {cache_target, func}:
            with contextlib.suppress(AttributeError, TypeError):
                # 某些 callable（如 functools.partial）无法设置属性，
                # 跳过缓存但本次仍可用编译路径。
                setattr(target, dag_attr, dag)
        # 运行期执行：同样需要 FlowContext。
        with _FlowContext(workflow_id):
            return True, _execute_compiled_dag(dag, None, args, kwargs)


# Module 加载时安装 patches（幂等）。
_install_patches()


__all__ = [
    "_ArgRef",
    "_CompiledDag",
    "_DagEdge",
    "_DagNode",
    "_FlowTracer",
    "run_compiled_flow",
]
