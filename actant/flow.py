"""工作流编排：``@flow`` 装饰器。

`@flow` 提供与 Prefect ``@flow`` 等价的编排入口：在函数体内调用 ``task.submit()``
即可组合任务，``AsyncResult`` 作为下游 ``submit`` 参数时自动解析依赖。

设计说明
========

**动态 DAG 语义**：flow 函数体以命令式方式在调用线程执行（与 Prefect/Ray 的
显式 DAG 构建不同），任务依赖通过 ``Task.submit`` 的 ``AsyncResult`` 自动解析
（阻塞等待上游结果）隐式表达。执行期由 ``FlowDAG`` 记录器捕获每次 ``task.submit()``
产生的节点与依赖边；flow 函数体成功返回后，将完整 DAG 提交到 Rust Orchestrator
（``Runtime.submit_dag``）持久化，并把任务实际结果回灌（``Runtime.complete_workflow``）
驱动状态机推进到终态，使工作流获得可查询的状态与恢复能力。

flow 生命周期通过 ``WorkflowLifecycle`` capability 广播：``submitted``/``started``
在函数体执行前实时广播（调用信号）；``completed``/``failed`` 由 Orchestrator 实际
持久化状态驱动（与 ``Runtime.get_workflow_state`` 一致）——只有任务结果回灌后状态机
真正到达 Completed / Failed，才广播对应的终态事件。

**Flow 级重试与超时**：
- ``retries``：flow 函数体抛异常时整体重试，最多 ``retries`` 次。
- ``retry_delay_ms``：重试间隔。
- ``timeout_ms``：flow 函数体的总执行时长上限，超时抛 ``ActantTimeoutError``。
  超时为**软超时**（子线程协作）：flow 函数在子线程中执行，主线程等待结果或超时；
  超时后子线程仍可能继续运行（Python 无法强制中断线程），但 ``@flow`` 调用立即返回超时错误。

**Flow 上下文（workflow_id 传播）**：
flow 执行期间设置 ``_flow_local.workflow_id``，``Task.submit`` 读取它作为任务的
``workflow_id``，使 ``TaskEvent`` 携带正确的归属信息。

用法
====

::

    import actant

    @actant.task
    def extract(src):
        return open(src).read()

    @actant.task
    def transform(raw):
        return raw.upper()

    @actant.task
    def load(data):
        print("loaded:", data)

    @actant.flow
    def pipeline(src):
        raw = extract.submit(src)        # AsyncResult
        upper = transform.submit(raw)    # 自动等待 extract 完成后取结果传入
        load.submit(upper)               # 自动等待 transform
        return upper.result()

    with actant.Runtime.with_defaults() as rt:
        rt.layer("WorkflowLifecycle", "emit").chain(
            lambda e: print(f"flow {e.kind}: {e.workflow_id}")
        )
        print(pipeline("data.txt"))
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

from actant._effects import emit
from actant._runtime import (
    WORKFLOW_STATE_COMPLETED,
    WORKFLOW_STATE_FAILED,
    get_current_runtime,
    use_runtime,
)
from actant.capabilities import WORKFLOW_LIFECYCLE, WorkflowEvent
from actant.exceptions import ActantTimeoutError, InvalidStateError
from actant.task._helpers import CallbackErrorPolicy

_logger = logging.getLogger("actant.flow")

P = ParamSpec("P")
R = TypeVar("R")

# DAG 合法失败策略（镜像 Rust FailureStrategy::parse 接受的规范字符串）。
_VALID_FAILURE_STRATEGIES = ("fail_fast", "continue")

# 超时触发重试前等待孤儿线程结束的上限（秒）。孤儿线程仍在执行流程体时
# 超过该上限则放弃等待并直接失败——宁可失败也不并发重复执行副作用。
_FLOW_ORPHAN_JOIN_TIMEOUT_S = 5.0

# 线程局部：当前 flow 的上下文状态，供 Task.submit 读取。
# None 表示不在任何 flow 上下文中。
_flow_local = threading.local()


class _FlowFailure(Exception):
    """内部异常：封装 flow 函数体失败且重试耗尽。

    携带最后一次尝试记录的 ``FlowDAG``（可能为 ``None``），使 ``@flow``
    在失败路径也能把已记录的部分 DAG 提交到 Orchestrator 并回灌结果，
    让状态机到达 Failed，从而让 ``failed`` 事件由 Orchestrator 状态驱动。
    """

    def __init__(self, cause: BaseException, dag: FlowDAG | None) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.dag = dag


class _OrphanFlowTimeout(ActantTimeoutError):
    """软超时异常：携带仍在执行流程体的孤儿线程引用。

    ``_run_with_timeout_in_context`` 的超时路径抛出。``_run_flow_with_retry``
    在重试前必须 join 该线程（带上限）；孤儿线程未在上限内结束时不得重试，
    避免流程体被并发重复执行产生重复副作用。
    """

    def __init__(self, message: str, thread: threading.Thread) -> None:
        super().__init__(message)
        self.thread = thread


class _NodeRecord:
    """``FlowDAG`` 记录的一个任务节点（提交时再构造 ``_DagNode``）。"""

    __slots__ = (
        "name",
        "payload",
        "priority",
        "retries",
        "retry_delay_ms",
        "task_id",
        "timeout_ms",
    )

    def __init__(
        self,
        *,
        task_id: str,
        name: str,
        payload: bytes,
        timeout_ms: int | None,
        priority: int | None,
        retries: int,
        retry_delay_ms: int,
    ) -> None:
        self.task_id = task_id
        self.name = name
        self.payload = payload
        self.timeout_ms = timeout_ms
        self.priority = priority
        self.retries = retries
        self.retry_delay_ms = retry_delay_ms


class FlowDAG:
    """flow 执行期的 DAG 记录器（动态 DAG 语义）。

    flow 函数体执行期间，``Task.submit`` / ``submit_batch`` 会把每个已提交
    任务作为节点、把参数中出现的 ``AsyncResult`` 依赖作为边增量记录到当前
    flow 的 ``FlowDAG``。flow 函数体成功返回后，``@flow`` 将完整 DAG 提交到
    Rust Orchestrator 持久化（``Runtime.submit_dag``）。

    记录器不改变命令式执行语义——任务仍被 eager 提交并阻塞解析依赖；它只把
    已发生的任务调用映射为 DAG 结构，供 Orchestrator 状态追踪与持久化。
    """

    def __init__(self) -> None:
        self._nodes: dict[str, _NodeRecord] = {}
        self._edges: list[tuple[str, str]] = []
        self._edge_set: set[tuple[str, str]] = set()
        self._outcomes: dict[str, tuple[bool, bytes]] = {}
        self._outcome_cond = threading.Condition()

    def add_task(
        self,
        *,
        task_id: str,
        name: str,
        payload: bytes,
        deps: list[str],
        timeout_ms: int | None = None,
        priority: int | None = None,
        retries: int = 0,
        retry_delay_ms: int = 0,
    ) -> None:
        """记录一个已提交任务节点及其上游依赖边。

        Args:
            task_id: 任务在 DAG 中的唯一标识。
            name: 人类可读名称。
            payload: 已序列化的任务载荷（cloudpickle bytes）。
            deps: 上游 ``AsyncResult`` 的 task_id 列表。
        """
        # 写入与 record_outcome/wait_all 共用 _outcome_cond 锁：任务完成回调
        # 与函数体提交可并发到达，锁保证 len(_outcomes) == len(_nodes) 的
        # notify 判定与节点插入原子，避免并发读写 dict。
        with self._outcome_cond:
            # task_id 由 uuid 生成，同一 flow 内不会重复；重复时防御性跳过。
            if task_id in self._nodes:
                return
            self._nodes[task_id] = _NodeRecord(
                task_id=task_id,
                name=name,
                payload=payload,
                timeout_ms=timeout_ms,
                priority=priority,
                retries=retries,
                retry_delay_ms=retry_delay_ms,
            )
            for dep in deps:
                if dep == task_id:
                    continue
                edge = (dep, task_id)
                if edge not in self._edge_set:
                    self._edge_set.add(edge)
                    self._edges.append(edge)

    def is_empty(self) -> bool:
        """是否未记录任何节点（flow 函数体未提交任务）。"""
        return not self._nodes

    def record_outcome(self, task_id: str, outcome: tuple[bool, bytes]) -> None:
        """记录一个已提交任务节点的终态结果。

        由 ``Task.submit`` / ``submit_batch`` 注册的 ``AsyncResult`` 完成回调
        触发，在任务终态（成功/失败/取消）时写入。``complete_workflow`` 依据
        这些结果把 Orchestrator 状态机推进到终态。

        Args:
            task_id: 任务在 DAG 中的唯一标识。
            outcome: ``(success, result_bytes)``，格式见
                ``AsyncResult._export_outcome``。
        """
        with self._outcome_cond:
            if task_id in self._nodes and task_id not in self._outcomes:
                self._outcomes[task_id] = outcome
                if len(self._outcomes) == len(self._nodes):
                    self._outcome_cond.notify_all()

    def wait_all(self, timeout: float | None = None) -> bool:
        """阻塞等待所有已记录节点产生终态结果。

        Args:
            timeout: 最大等待秒数，``None`` 表示无限等待。

        Returns:
            ``True`` 若所有节点均已产生终态结果；``False`` 若超时。
        """
        with self._outcome_cond:
            return self._outcome_cond.wait_for(
                lambda: len(self._outcomes) >= len(self._nodes),
                timeout=timeout,
            )

    def to_outcomes(self) -> list[tuple[str, bool, bytes]]:
        """按节点提交顺序返回 ``[(task_id, success, result_bytes)]``。

        仅包含已产生终态结果的节点；未产生结果的节点（调用方应先
        ``wait_all``）会被跳过。
        """
        out: list[tuple[str, bool, bytes]] = []
        for rec in self._nodes.values():
            outcome = self._outcomes.get(rec.task_id)
            if outcome is not None:
                out.append((rec.task_id, outcome[0], outcome[1]))
        return out

    def __len__(self) -> int:
        return len(self._nodes)

    def to_nodes(self) -> list[Any]:
        """构造 ``actant.actant._DagNode`` 列表（惰性导入已构建的 Rust 模块）。

        仅当 ``retries > 0`` 时为节点附带 ``_RetryPolicy``；否则传 ``None``
        让 Orchestrator 应用 DAG 级默认重试策略。

        重试语义（单层执行）：task 级 ``retries`` 的唯一重试执行者是 worker
        Python 层（``actant.task._worker`` 按 payload 头部 retries 调
        ``_execute_with_retries``，payload 自足、跨节点语义一致）。节点上的
        ``RetryPolicy`` 目前只是 Orchestrator 的**记录/查询元数据**——Rust 派发
        路径没有任何 ``TaskDefinition.retry_policy`` 消费者，
        ``Orchestrator::prepare_task_retry`` 亦无调用方，不会对该节点再重试，
        故与 worker 层不叠加。0.3.3 若接线 orchestrator 驱动重试（S3/S6），
        必须避免两层各按同一 ``retries`` 重试（最坏 retries² 次执行）。
        """
        from actant.actant import _DagNode, _RetryPolicy

        nodes: list[Any] = []
        for rec in self._nodes.values():
            retry = None
            if rec.retries > 0:
                retry = _RetryPolicy(
                    max_retries=rec.retries,
                    delay_ms=rec.retry_delay_ms,
                )
            nodes.append(
                _DagNode(
                    task_id=rec.task_id,
                    name=rec.name,
                    payload=rec.payload,
                    retry=retry,
                    timeout_ms=rec.timeout_ms,
                    priority=rec.priority,
                )
            )
        return nodes

    def to_edges(self) -> list[tuple[str, str]]:
        """返回 DAG 依赖边，仅保留两端都存在于本 DAG 的边。

        依赖可能指向 flow 之外提交的任务（外部 ``AsyncResult``）；Orchestrator
        要求边两端节点都存在，故此处过滤，避免提交失败。
        """
        return [
            (frm, to)
            for frm, to in self._edges
            if frm in self._nodes and to in self._nodes
        ]


class _FlowState:
    """单个 flow 实例的状态：workflow_id + 取消协作标记 + DAG 记录器。

    ``cancel_event`` 用于超时协作：主线程超时后设置它，子线程中的
    ``Task.submit`` 在提交新任务前检查它，若已设置则抛出
    ``ActantTimeoutError``，阻止 orphan 任务继续创建。

    ``dag`` 为本实例的 ``FlowDAG`` 记录器，flow 函数体执行期间由
    ``Task.submit`` / ``submit_batch`` 增量写入节点与依赖边。
    """

    def __init__(self, workflow_id: str) -> None:
        self.workflow_id = workflow_id
        self.cancel_event = threading.Event()
        self.dag = FlowDAG()

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()


def current_workflow_id() -> str | None:
    """返回当前线程活跃的 workflow_id（若在 ``@flow`` 上下文中）。"""
    state = getattr(_flow_local, "state", None)
    return state.workflow_id if state is not None else None


def current_flow_dag() -> FlowDAG | None:
    """返回当前 flow 上下文的 ``FlowDAG`` 记录器（若在 ``@flow`` 上下文中）。

    由 ``Task.submit`` / ``submit_batch`` 在提交任务时调用，把节点与依赖边
    增量记录到当前 flow 的 DAG。不在 flow 上下文中时返回 ``None``。
    """
    state = getattr(_flow_local, "state", None)
    return state.dag if state is not None else None


def is_flow_cancelled() -> bool:
    """当前 flow 是否已被取消（超时或显式取消）。

    由 ``Task.submit`` 在提交新任务前检查；若返回 ``True``，
    调用方应抛出 ``ActantTimeoutError`` 以阻止 orphan 任务创建。
    不在 flow 上下文中时返回 ``False``。
    """
    state = getattr(_flow_local, "state", None)
    return state is not None and state.is_cancelled()


def _cancel_flow_tasks(workflow_id: str) -> None:
    """取消指定 workflow 下的所有本地任务（flow 失败/超时时调用）。"""
    from actant._runtime import get_current_runtime

    runtime = get_current_runtime()
    if runtime is None:
        return
    for tid in list(runtime.list_tasks()):
        handle = runtime.get_task(tid)
        if handle is not None and handle.workflow_id == workflow_id:
            try:
                handle.cancel(propagate=False)
            except Exception:
                # 批量取消：单个任务取消失败不应阻止后续任务被取消，
                # 记录 warning 后继续（最终由 flow 失败异常向上传播）。
                _logger.warning(
                    "flow %s: failed to cancel task %s", workflow_id, tid,
                    exc_info=True,
                )


class _FlowContext:
    """``with`` 上下文：设置/恢复 ``_flow_local.state``。

    可接受外部 ``cancel_event``，使主线程能在超时后设置子线程的取消信号。
    """

    def __init__(
        self,
        workflow_id: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self._state = _FlowState(workflow_id)
        if cancel_event is not None:
            self._state.cancel_event = cancel_event
        self._prev: _FlowState | None = None

    def __enter__(self) -> _FlowState:
        self._prev = getattr(_flow_local, "state", None)
        _flow_local.state = self._state
        return self._state

    def __exit__(self, *exc: object) -> None:
        _flow_local.state = self._prev


def flow(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay_ms: int = 0,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
) -> Any:
    """装饰器：将函数标记为工作流，提供生命周期事件、重试、超时与上下文校验。

    被装饰的函数行为：

    1. 调用前校验存在活跃 ``Runtime``（否则抛 ``InvalidStateError``）。
    2. 生成 ``workflow_id`` 并设置 flow 上下文，使函数体内的 ``task.submit()``
       自动携带该 ``workflow_id``（配套 ``TaskEvent`` 归属）。
    3. 广播 ``WorkflowLifecycle`` 事件：``submitted``/``started`` 在函数体执行前
       实时广播；``completed``/``failed`` 由 Orchestrator 实际持久化状态驱动
       （任务结果经 ``complete_workflow`` 回灌后，与 ``get_workflow_state`` 一致）。
    4. Flow 级重试（``retries``）：函数体抛异常时整体重试。
    5. Flow 级超时（``timeout_ms``）：函数体在子线程执行，超时抛 ``ActantTimeoutError``。

    Args:
        func: 被装饰的编排函数（无参装饰器时由 ``flow`` 自动填充）。
        name: 工作流名称（默认 ``func.__qualname__``），用于日志与事件。
        retries: 失败后的重试次数（0=不重试）。
        retry_delay_ms: 重试间隔毫秒。
        timeout_ms: flow 函数体的总执行超时毫秒（0=不限制）。

            .. warning::
                **软超时（soft timeout）**：超时后 ``@flow`` 调用立即返回
                ``ActantTimeoutError``，但 Python 无法强制中断线程——正在
                子线程中运行的同步代码会继续执行直到函数返回或抛出异常。
                超时后通过 ``cancel_event`` 拦截后续 ``Task.submit`` 调用，
                阻止 orphan 任务继续创建，但**不释放执行中任务占用的资源**。
                长时间运行的同步函数应在内部轮询 ``cancel_event`` 或拆分为
                多个 ``Task.submit``，以获得及时的取消响应。

                **超时 × 重试**：超时触发的重试前会等待上一轮孤儿线程结束
                （join 带上限）；若孤儿线程在上限内仍未结束，则记录 warning
                并放弃等待、**不再重试**直接抛出 ``ActantTimeoutError``——
                宁可失败也不让流程体被并发重复执行。非超时异常的重试不受
                影响（前次执行已正常结束）。
        failure_strategy: DAG 提交到 Orchestrator 时的失败策略。``None``
            （默认）不显式传递该参数，由 Rust 侧应用默认策略
            ``FailureStrategy::FailFast``（任一任务失败立即标记工作流失败）。
            ``"fail_fast"`` 显式 fail-fast；``"continue"`` 任务失败后工作流
            继续执行直到所有任务到达终态。装饰时校验，非法值抛 ``ValueError``。

    Raises:
        InvalidStateError: 无活跃 Runtime。
        ActantTimeoutError: flow 执行超时。
        ValueError: ``failure_strategy`` 非法（仅接受 ``"fail_fast"`` /
            ``"continue"`` / ``None``）。
    """

    def _make(f: Callable[P, R]) -> Callable[P, R]:
        if retries < 0:
            raise ValueError(f"flow: retries must be >= 0, got {retries}")
        if retry_delay_ms < 0:
            raise ValueError(
                f"flow: retry_delay_ms must be >= 0, got {retry_delay_ms}"
            )
        if failure_strategy is not None and failure_strategy not in _VALID_FAILURE_STRATEGIES:
            raise ValueError(
                f"flow: failure_strategy must be one of "
                f"{_VALID_FAILURE_STRATEGIES} or None, got {failure_strategy!r}"
            )
        flow_name = name or f.__qualname__

        @wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            runtime = get_current_runtime()
            if runtime is None:
                raise InvalidStateError(
                    f"flow {flow_name!r}: no active Runtime; "
                    "wrap your code in `with actant.Runtime() as rt:`"
                )
            workflow_id = f"{flow_name}-{uuid.uuid4().hex[:8]}"
            _safe_emit(workflow_id, "submitted")
            _safe_emit(workflow_id, "started")
            try:
                result, dag = _run_flow_with_retry(
                    f, args, kwargs, workflow_id,
                    retries=retries,
                    retry_delay_ms=retry_delay_ms,
                    timeout_ms=timeout_ms,
                    runtime=runtime,
                )
            except _FlowFailure as ff:
                # 级联取消：flow 失败后取消其所有子任务（最佳-effort）。
                _cancel_flow_tasks(workflow_id)
                # 即使函数体失败，也提交执行期已记录的部分 DAG 并回灌失败结果，
                # 使 Orchestrator 状态机推进到 Failed——终态事件由实际状态驱动。
                if ff.dag is not None and not ff.dag.is_empty():
                    _submit_and_backfill(
                        workflow_id, ff.dag, runtime, timeout_ms=timeout_ms,
                        failure_strategy=failure_strategy,
                    )
                else:
                    # 无任务提交的 flow 失败：没有可回灌的 DAG，Orchestrator 无
                    # 状态机可驱动，直接广播 failed（与空 flow 成功时直接广播
                    # completed 对称，见下方 else 分支）。
                    _safe_emit(
                        workflow_id, "failed",
                        error=f"{type(ff.cause).__name__}: {ff.cause}",
                    )
                raise ff.cause from None
            except BaseException as exc:
                _safe_emit(workflow_id, "failed", error=f"{type(exc).__name__}: {exc}")
                # 级联取消：flow 失败/超时后取消其所有子任务（最佳-effort）。
                _cancel_flow_tasks(workflow_id)
                raise
            # 将执行期记录的动态 DAG 提交到 Rust Orchestrator 持久化，并回灌任务
            # 结果驱动状态机到终态；终态生命周期事件由 Orchestrator 实际状态决定。
            # 未提交任何任务的 flow 无 DAG 可提交，直接广播 completed。
            if dag is not None and not dag.is_empty():
                _submit_and_backfill(
                    workflow_id, dag, runtime, timeout_ms=timeout_ms,
                    failure_strategy=failure_strategy,
                )
            else:
                _safe_emit(workflow_id, "completed")
            return result

        return wrapper

    if func is None:
        return _make
    return _make(func)


def _submit_and_backfill(
    workflow_id: str,
    dag: FlowDAG,
    runtime: Any,
    *,
    timeout_ms: int,
    failure_strategy: str | None = None,
) -> None:
    """提交 DAG 到 Orchestrator 并回灌结果，驱动状态机到终态（成功/失败共用）。

    提交后回灌任务结果并依据 ``get_workflow_state`` 的实际终态广播事件，
    使生命周期事件与 Orchestrator 持久化状态一致。

    ``failure_strategy`` 为 ``None`` 时不传递该参数，由 Rust 侧应用默认
    ``FailureStrategy::FailFast``；否则原样透传（``"fail_fast"`` /
    ``"continue"``，装饰时已校验）。
    """
    extra: dict[str, Any] = {}
    if failure_strategy is not None:
        extra["failure_strategy"] = failure_strategy
    runtime.submit_dag(
        workflow_id,
        dag.to_nodes(),
        dag.to_edges(),
        **extra,
    )
    _backfill_and_emit_terminal(
        workflow_id, dag, runtime, wait_timeout_ms=timeout_ms,
    )


def _run_flow_with_retry(
    func: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    workflow_id: str,
    *,
    retries: int,
    retry_delay_ms: int,
    timeout_ms: int,
    runtime: Any = None,
) -> tuple[R, FlowDAG | None]:
    """在 flow 上下文中执行函数体，支持重试与超时。

    重试语义：

    - **非超时异常**：前次执行已在同一线程正常结束（异常从函数体同步抛出），
      重试不会产生并发重复执行，维持原有重试路径。
    - **软超时**：超时发生时孤儿线程可能仍在执行函数体。重试前必须 join
      该孤儿线程（带上限 ``_FLOW_ORPHAN_JOIN_TIMEOUT_S``）；join 成功才允许
      重试。若孤儿线程在上限内仍未结束，记录 warning 并放弃等待、**不再
      重试**，直接包装为 ``_FlowFailure`` 抛出——宁可失败也不让流程体被
      并发重复执行产生重复副作用。

    返回 ``(result, dag)``：``dag`` 为最后一次尝试记录的 ``FlowDAG``（无论
    成功与否都会填充），供调用方在成功或失败路径提交到 Orchestrator。
    """
    attempt = 0
    dag_box: list[FlowDAG | None] = [None]
    while True:
        try:
            return _run_with_timeout_in_context(
                func, args, kwargs, workflow_id, timeout_ms, runtime,
                dag_box=dag_box,
            )
        except _OrphanFlowTimeout as exc:
            # 软超时：先等待孤儿线程结束（带上限），避免两次并发执行流程体。
            orphan = exc.thread
            orphan.join(timeout=_FLOW_ORPHAN_JOIN_TIMEOUT_S)
            if orphan.is_alive():
                _logger.warning(
                    "flow %s: orphan thread %r still running after %.1fs; "
                    "giving up waiting and failing without retry to avoid "
                    "concurrent duplicate execution of the flow body",
                    workflow_id, orphan.name, _FLOW_ORPHAN_JOIN_TIMEOUT_S,
                )
                raise _FlowFailure(exc, dag_box[0]) from None
            # 孤儿线程已结束，超时与其他异常一样进入下方统一重试判定。
            failure: Exception = exc
        except Exception as exc:
            failure = exc
        if attempt < retries:
            _logger.warning(
                "flow %s: attempt %d failed (%s: %s), retrying",
                workflow_id, attempt, type(failure).__name__, failure,
            )
            # 重试前清理上一轮已提交的子任务，避免 orphan 任务继续执行
            # 产生副作用（如重复写库）。
            _cancel_flow_tasks(workflow_id)
            if retry_delay_ms > 0:
                time.sleep(retry_delay_ms / 1000)
            attempt += 1
            continue
        # 重试耗尽：携带最后尝试记录的 DAG 抛出，供调用方提交部分 DAG。
        # 包装为 flow 已知的原子异常对象，避免绑定 __cause__ 产生额外噪音。
        raise _FlowFailure(failure, dag_box[0]) from None


def _run_with_timeout_in_context(
    func: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    workflow_id: str,
    timeout_ms: int,
    runtime: Any = None,
    dag_box: list[FlowDAG | None] | None = None,
) -> tuple[R, FlowDAG | None]:
    """在 flow 上下文中执行 func，可选超时。

    无超时（``timeout_ms <= 0``）时直接在当前线程执行，保留 flow 上下文。
    有超时时在子线程执行，主线程等待结果；超时抛 ``_OrphanFlowTimeout``
    （``ActantTimeoutError`` 子类，携带仍在运行的孤儿线程引用）。
    子线程内因 ``cancel_event`` 中断产生的异常仍以普通 ``ActantTimeoutError``
    形式经 ``exc_box`` 抛出（此时线程已结束，可直接重试）。

    超时治理：
    主线程超时后设置 ``cancel_event``，子线程中后续的 ``Task.submit`` 调用
    会检查该事件并抛出 ``ActantTimeoutError``，阻止 orphan 任务继续创建。
    注意：Python 无法强制中断线程，正在运行的同步代码不受影响，但新任务
    提交会被拦截。

    返回 ``(result, dag)``：``dag`` 为本尝试记录的 ``FlowDAG``。异常路径下
    亦会将 ``dag`` 写入传入的 ``dag_box``，使调用方在失败时也能拿到记录器。
    """
    if dag_box is None:
        dag_box = [None]
    if timeout_ms <= 0:
        # 直接在当前线程执行；先记录 dag 引用，保证即使 func 抛异常，
        # 调用方仍能在 dag_box 中拿到已记录的 DAG。
        with _FlowContext(workflow_id) as state:
            dag_box[0] = state.dag
            result = func(*args, **kwargs)
        return result, state.dag

    # 超时模式：子线程执行，主线程等待。
    # 共享 cancel_event：主线程超时后 set()，子线程中 Task.submit 检查它。
    cancel_event = threading.Event()
    result_box: list[Any] = [None]
    exc_box: list[BaseException | None] = [None]
    done_event = threading.Event()

    def _runner() -> None:
        # 子线程需显式继承 Runtime 上下文（threading.local 不跨线程传播）。
        rt_ctx = use_runtime(runtime) if runtime is not None else nullcontext()
        try:
            with rt_ctx, _FlowContext(workflow_id, cancel_event=cancel_event) as state:
                dag_box[0] = state.dag
                result_box[0] = func(*args, **kwargs)
        except BaseException as e:
            # 若主线程已设置 cancel_event，将子线程异常替换为 ActantTimeoutError，
            # 避免将 orphan 线程的后续异常泄漏给调用方。
            if state.cancel_event.is_set():
                exc_box[0] = ActantTimeoutError(
                    f"flow {workflow_id!r} exceeded timeout_ms={timeout_ms}"
                )
            else:
                exc_box[0] = e
        finally:
            done_event.set()

    thread = threading.Thread(
        target=_runner,
        name=f"actant-flow-{workflow_id}",
        daemon=True,
    )
    # 注册到 Runtime 供 stop() join。
    if runtime is not None:
        runtime.register_flow_thread(thread)
    thread.start()
    try:
        completed = done_event.wait(timeout=timeout_ms / 1000)
    finally:
        if runtime is not None:
            runtime.unregister_flow_thread(thread)
    if not completed:
        # 超时：设置 cancel_event，使子线程中后续 Task.submit 抛出 ActantTimeoutError。
        cancel_event.set()
        # 携带孤儿线程引用抛出：`_run_flow_with_retry` 重试前须 join 该线程，
        # 防止流程体被并发重复执行。
        raise _OrphanFlowTimeout(
            f"flow {workflow_id!r} exceeded timeout_ms={timeout_ms}", thread,
        )
    if exc_box[0] is not None:
        raise exc_box[0]
    return cast("R", result_box[0]), dag_box[0]


def _backfill_and_emit_terminal(
    workflow_id: str,
    dag: FlowDAG,
    runtime: Any,
    *,
    wait_timeout_ms: int,
) -> None:
    """回灌任务结果并依据 Orchestrator 实际状态广播终态生命周期事件。

    flow 函数体成功返回后，任务结果经 ``complete_workflow`` 回灌给 Orchestrator
    状态机（成功项 ``COMPLETE_TASK``，失败项 ``FAIL_TASK``），随后查询
    ``get_workflow_state``，仅当状态机真正到达 Completed / Failed 时广播对应事件
    ——使终态事件与 Orchestrator 持久化状态一致。

    Args:
        workflow_id: 工作流唯一标识。
        dag: 已提交的 ``FlowDAG`` 记录器（含节点/边/结果）。
        runtime: 活跃的 ``Runtime``。
        wait_timeout_ms: 等待任务终态结果的超时毫秒（0 表示无限等待）。
    """
    # 阻塞等待所有已提交任务产生终态结果（函数体通常已 await，立即返回；仅
    # fire-and-forget 提交会在此阻塞）。超时后不回灌，工作流保持 Pending——
    # 终态事件不广播，调用方可轮询 get_workflow_state。
    if not dag.wait_all(
        timeout=wait_timeout_ms / 1000 if wait_timeout_ms > 0 else None
    ):
        _logger.warning(
            "flow %s: not all task outcomes settled within timeout, "
            "workflow left Pending (no terminal event emitted)",
            workflow_id,
        )
        return
    runtime.complete_workflow(workflow_id, dag.to_outcomes())
    state = runtime.get_workflow_state(workflow_id)
    if state is None:
        _logger.warning(
            "flow %s: workflow missing after backfill", workflow_id,
        )
        return
    state_name = state.get("state")
    if state_name == WORKFLOW_STATE_COMPLETED:
        _safe_emit(workflow_id, "completed")
    elif state_name == WORKFLOW_STATE_FAILED:
        _safe_emit(workflow_id, "failed", error=state.get("error") or "")
    else:
        _logger.warning(
            "flow %s: workflow reached unexpected state %r",
            workflow_id, state_name,
        )


def _safe_emit(
    workflow_id: str,
    kind: str,
    *,
    error: str = "",
    on_error: CallbackErrorPolicy = "log",
) -> Exception | None:
    """广播 WorkflowLifecycle 事件，失败时按 ``on_error`` 策略处理。

    Args:
        on_error: ``log``（默认）仅记录 warning；``raise`` 立即抛出；
            ``collect`` 记录并返回异常。

    Returns:
        捕获到的异常（仅当 ``on_error="collect"`` 时）。
    """
    try:
        emit(
            WORKFLOW_LIFECYCLE,
            WorkflowEvent(
                kind=kind,  # type: ignore[arg-type]
                workflow_id=workflow_id,
                error=error,
            ),
        )
    except Exception as e:
        if on_error == "raise":
            raise
        _logger.warning(
            "flow %r: WorkflowLifecycle emit (%s) failed", workflow_id, kind,
            exc_info=True,
        )
        if on_error == "collect":
            return e
    return None


__all__ = [
    "current_workflow_id",
    "flow",
]
