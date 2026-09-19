"""工作流编排：``@flow`` 装饰器（重放模型）。

`@flow` 提供与 Prefect ``@flow`` 等价的编排入口：在函数体内调用 ``task.submit()``
即可组合任务，``AsyncResult`` 作为下游 ``submit`` 参数时自动解析依赖。

设计说明
========

**Orchestrator 驱动的持久化执行**：flow 函数体在
调用线程执行；函数体内每次 ``task.submit()`` 经 ``add_workflow_node`` 把节点
增量写入 Rust Orchestrator（先持久化再派发），任务由 orchestrator 经既有调度
路径派发——ready 条件、fail-fast、重试、fencing、恢复全部真实生效。
函数体返回后**不**做任何结果回灌：工作流终态由最后一个任务完成触发（与 DAG
语义一致：全部任务终态 → 工作流终态）。

**重放（续跑）**：flow 体重放时第 n 次 ``submit`` 查工作流历史——
节点已完成 → 返回记录结果（不重跑）；进行中 → 返回绑定句柄；不存在 → 新提交。
节点标识由 ``(任务名, 序号, workflow_id)`` 确定性生成，跨重放稳定；节点定义
指纹（name / payload / 超时 / 优先级 / 重试策略 / 依赖边）不一致时抛
:class:`FlowReplayError`，fail-fast 防止提交序列静默错位。

**确定性契约**：flow 体不得依赖 wall-clock、随机数、非任务副作用来决定提交
序列；提交序列（第 n 次 submit 对应第 n 个节点）必须可复现。上游结果已内联
进下游 payload，因此要求任务函数本身确定性。

**重试单层化**：flow 任务的唯一重试执行者是 orchestrator（节点
RetryPolicy，由 ``@task(retries=...)`` 映射）；派发给 worker 的 payload 头部
retries 置 0，worker 层不重试。独立 ``@task`` 直调（非 flow）保持 worker 层
重试不变。flow 级整体重试已删除——重放模型下函数体重试由续跑机制承载。

flow 生命周期通过 ``WorkflowLifecycle`` capability 广播：``submitted``/
``started`` 在工作流**真正建槽之后**才广播（工作流是惰性创建的，见下）；
``completed``/``failed`` 由 Orchestrator 实际持久化状态驱动（与
``Runtime.get_workflow_state`` 一致）。函数体若从未创建工作流（无
``Task.submit``、无等待点、无 ``suspend``），则**只有终态事件**。

**Flow 级超时（强还原）**：``timeout_ms`` 是**工作流级 deadline**，唯一决策者
是 orchestrator 的超时 watcher（``start_timeout_watcher``）——到期即把工作流标为
``Failed``（error = ``workflow timeout exceeded``）并取消所有运行中任务。取消走
**两条腿**：gossip 广播（远端执行的任务）与本地自投递（本节点执行的任务；gossip
不会把广播发回发送者）。本地在途 ``AsyncResult`` 被结算后，阻塞在任务等待上的
函数体抛出任务级 ``Cancelled``，本模块将其归一为 :class:`ActantTimeoutError`。

.. warning::
    **超时是"唤醒后抛出"，不是"立即返回"**：函数体在
    deadline 之后才被唤醒，因此返回时刻在 ``deadline + 轮询周期``（默认
    ``state_poll_interval_ms`` = 500ms）量级。另外，没有被任何挂起点/任务
    等待包住的**纯 CPU 段不会被中断**（Python 无法中断线程）——函数体应在
    内部自查 ``Runtime.get_workflow_state`` 或把长循环拆成任务。

**等待点**：flow 体内可用 :func:`sleep_until` 挂起到指定时刻、
:func:`wait_signal` 等待外部信号。二者注册等待点到 orchestrator 并 park
当前线程。对方经 ``Runtime.signal_wait_point(workflow_id, name)`` 递交；
**信号可先于等待点抵达**（注册时命中缓冲即刻返回），递交方无需重试。

跨重启：``recover`` = **快照 + 其后事件重放**（``Orchestrator::recover`` 先加载
``orch:dag:/exec:/pending:/wait:/sigbuf:`` 快照，再按 ``orch:eventseq:`` 水位重放
其后的事件），故等待点与已注册的等待点状态都能跨重启存活，水位之后的增量也不会丢。

"信号先于等待点抵达"在两条路上都成立：运行期走内存缓冲表，重放期
``apply_replayed_event`` 也会把先到的 ``SignalReceived`` 入缓冲，二者并随
``orch:sigbuf:`` 快照落盘兜底。唯一会丢的窗口是"缓冲写入后、尚未落盘就崩溃"。

**挂起与恢复**：:func:`suspend` 追加一个**挂起条件**等待点并 park，等待
``Runtime.resume_suspended(workflow_id)`` 的恢复指令。它与 :func:`wait_signal`
的区别是语义来源——signal 等一个具名业务事件，suspend 等操作员的动作——故
``resume`` 只唤醒挂起点，不会冒名顶替一个业务信号。中止走既有的取消通道
（``Runtime.cancel_workflow``）：工作流进入终态时 orchestrator 会**释放该工作流的
全部 park 等待者**，park 中的函数体据此被唤醒并抛出与终态相符的异常
（``WorkflowCancelledError`` / ``WorkflowFailedError`` / ``ActantTimeoutError``）。
注：等待点 park 与 ``AsyncResult`` 等待是两条不同的阻塞原语，这条释放路径对二者
都必须成立。

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
from functools import wraps
from types import SimpleNamespace
from typing import Any, NamedTuple, ParamSpec, TypeVar, cast

from actant._effects import emit
from actant._runtime import (
    WORKFLOW_STATE_CANCELLED,
    WORKFLOW_STATE_COMPLETED,
    WORKFLOW_STATE_FAILED,
    get_current_runtime,
    use_runtime,
)
from actant.capabilities import WORKFLOW_LIFECYCLE, WorkflowEvent
from actant.exceptions import (
    ActantTimeoutError,
    InvalidStateError,
    WorkflowCancelledError,
    WorkflowFailedError,
)
from actant.task._helpers import CallbackErrorPolicy

_logger = logging.getLogger("actant.flow")

P = ParamSpec("P")
R = TypeVar("R")

# DAG 合法失败策略（镜像 Rust FailureStrategy::parse 接受的规范字符串）。
_VALID_FAILURE_STRATEGIES = ("fail_fast", "continue")

# flow 体返回后轮询工作流终态的间隔（秒）。终态由最后一个任务完成触发，
# 无推送通道可用（TerminalWaiterRegistry 未暴露 PyO3 桥），轮询复用
# get_workflow_state 既有查询面。
_FLOW_TERMINAL_POLL_INTERVAL_S = 0.05

#: 挂起等待点的条件种类（Rust 侧 `WaitCondition::Suspend`）。
_SUSPEND_KIND = "suspend"

#: 挂起等待点的键前缀。与 `timer-{n}` 同构：键由**等待序号**决定，故同一
#: flow 内多次挂起各自独立（`resume_suspended` 按 workflow_id 唤醒，不要求
#: 调用方知道具体键）。
_SUSPEND_WAIT_PREFIX = "suspend-"


class _FlowRecovery(NamedTuple):
    """续跑所需的"函数体 + 调用参数"二元组。"""

    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


#: flow 名 → 恢复登记（续跑驱动用）。
#:
#: `@flow` 在导入时登记"flow 名 → 函数体"，使 `resume_flows()` 能从
#: `workflow_id` 前缀恢复出函数体。同名后注册
#: 者覆盖先注册者。
_FLOW_RECOVERY: dict[str, _FlowRecovery] = {}

# 线程局部：当前 flow 的上下文状态，供 Task.submit 读取。
# None 表示不在任何 flow 上下文中。
_flow_local = threading.local()


class _FlowState:
    """单个 flow 实例的运行状态（重放模型）。

    - ``workflow_id``：任务的归属工作流（TaskEvent / 节点写入目标）。
    - ``timeout_ms``：工作流级 deadline。**不在 Python 侧判定**——到期
      由 orchestrator 的超时 watcher 强还原，本类只负责把它转交
      ``submit_workflow``（惰性创建时携带）。
    - ``_seq``：提交序号计数器。每次 flow 内 ``submit``（含重放命中）递增，
      与节点确定性标识 ``{任务名}-{序号}-{workflow_id}`` 共同构成
      "第 n 次提交" 的身份；重放体执行到同一序号时据此查历史。
    - ``workflow_created``：工作流外壳是否已持久化（惰性创建——空 flow
      不产生编排数据，不创建工作流）。
    """

    def __init__(
        self,
        workflow_id: str,
        *,
        failure_strategy: str | None = None,
        timeout_ms: int = 0,
        announce: bool = True,
    ) -> None:
        self.workflow_id = workflow_id
        self.failure_strategy = failure_strategy
        self.timeout_ms = timeout_ms
        self._seq = 0
        self._wait_seq = 0
        self.workflow_created = False
        #: 惰性创建工作流时是否广播 ``submitted`` / ``started``。
        #: ``False`` 仅用于 :func:`_replay_flow`——原始执行已广播过，重放体不得
        #: 重复。
        self.announce = announce

    def next_seq(self) -> int:
        """分配下一次提交的序号（从 1 开始递增）。"""
        self._seq += 1
        return self._seq

    def next_wait_seq(self) -> int:
        """分配下一次**等待点**调用序号（从 1 开始递增）。

        与 :meth:`next_seq` 分开计数，原因是二者构成两条独立的确定性序列：
        节点的跨重放身份是 ``{任务名}-{提交序号}-{workflow_id}``，等待点的
        跨重放身份是 ``timer-{等待序号}``。若共用计数器，新增/删减一个等待点
        会平移全部节点标识，放大重放指纹冲突面。
        """
        self._wait_seq += 1
        return self._wait_seq

    @property
    def node_count(self) -> int:
        """本次 flow 执行已提交（或重放命中）的节点数。"""
        return self._seq


def current_workflow_id() -> str | None:
    """返回当前线程活跃的 workflow_id（若在 ``@flow`` 上下文中）。"""
    state = getattr(_flow_local, "state", None)
    return state.workflow_id if state is not None else None


def current_flow_state() -> _FlowState | None:
    """返回当前 flow 上下文状态（若在 ``@flow`` 上下文中，否则 ``None``）。

    由 ``Task.submit`` / ``submit_batch`` 调用：非 ``None`` 表示本次提交
    走 orchestrator 驱动的编排路径；``None`` 保持独立 ``@task``
    直调的任务队列语义不变。
    """
    state = getattr(_flow_local, "state", None)
    return state


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
    """``with`` 上下文：设置/恢复 ``_flow_local.state``。"""

    def __init__(self, state: _FlowState) -> None:
        self._state = state
        self._prev: _FlowState | None = None

    def __enter__(self) -> _FlowState:
        self._prev = getattr(_flow_local, "state", None)
        _flow_local.state = self._state
        return self._state

    def __exit__(self, *exc: object) -> None:
        _flow_local.state = self._prev


def _ensure_workflow_created(runtime: Any, state: _FlowState) -> None:
    """惰性创建工作流外壳，并在**真正创建之后**广播生命周期事件。

    这是工作流外壳的**唯一**创建入口：``Task.submit`` 的编排路径与等待点路径
    （:func:`_wait_context`）都必须经此，否则会再次出现"两处各写一遍创建块"的
    漂移（守则：同一事实只有一个源）。

    时序上刻意把 ``submitted`` / ``started`` 放在 ``submit_workflow`` **之后**：
    ``@flow`` 的 wrapper 曾在函数体开始前就广播这两个事件，而工作流那时还没
    建槽——等于把一个此刻不存在的 ``workflow_id`` 交给外部观察者，其立即调用
    ``Runtime.signal_wait_point`` 会撞 ``NotFound``（实测 1/5 复现）。事件与状态
    同源后，"看到 submitted ⇒ 工作流已存在"成立。
    """
    if state.workflow_created:
        return
    runtime.submit_workflow(
        state.workflow_id,
        failure_strategy=state.failure_strategy,
        timeout_ms=state.timeout_ms,
    )
    state.workflow_created = True
    if state.announce:
        _safe_emit(state.workflow_id, "submitted")
        _safe_emit(state.workflow_id, "started")


def _wait_context() -> tuple[Any, _FlowState]:
    """返回当前 flow 的 ``(runtime, state)``，并确保工作流外壳已持久化。

    三条前置全部显式报错而非静默降级：

    - 不在 ``@flow`` 函数体内（线程局部无 state）→ :class:`InvalidStateError`；
    - 无活跃 Runtime → :class:`InvalidStateError`；
    - 工作流外壳尚未持久化 → 复用 ``Task.submit`` 的惰性创建路径。等待点必须
      落在已存在的工作流上，否则 Rust 侧 ``register_wait_point`` 报 ``NotFound``
      （等待点随工作流快照落盘，无工作流即无宿主）。
    """
    state = getattr(_flow_local, "state", None)
    if state is None:
        raise InvalidStateError(
            "sleep_until / wait_signal 只能在 @flow 函数体内调用（当前无 flow 上下文）"
        )
    runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            "no active runtime: sleep_until / wait_signal 需要 Runtime 上下文"
        )
    _ensure_workflow_created(runtime, state)
    return runtime, state


def _raise_if_workflow_terminal(runtime: Any, workflow_id: str) -> None:
    """park 未被唤醒时判定真实原因：工作流终态 ⇒ 抛与该终态相符的异常。

    ``Runtime.wait_wait_point`` 返回 ``None`` 有两种含义：**上界超时**，或
    **等待者被释放**——工作流进入终态时 orchestrator 会释放该工作流的全部 park
    等待者（``complete_terminal`` → ``release_wait_waiters``）。二者必须区分：

    - 不区分 ⇒ abort 会被伪装成"信号返回了空 payload"，函数体在已死的工作流上
      继续跑（其后续 ``submit`` 会被权威层以 ``InvalidState`` 拒绝）；
    - 事实源是**工作流终态**，与 :func:`_deadline_failure` 同一模式——
      不在 Python 侧复算任何计时器。

    非终态直接返回（那确实只是上界超时，沿用既有约定）。

    ``Completed`` 不会出现在这条路径：``check_workflow_completion`` 有
    ``nodes_sealed`` 守卫，而封口发生在函数体返回之后，故 park 期间工作流不可能
    被判为 ``Completed``。
    """
    try:
        state = runtime.get_workflow_state(workflow_id)
    except Exception:
        # 查询失败（Runtime 已关停等）：无法再区分，交给调用方按上界超时处理。
        _logger.debug(
            "flow %s: could not read workflow state after park release",
            workflow_id,
            exc_info=True,
        )
        return
    if state is None:
        # 工作流已被移除/淘汰：不可再恢复，不静默继续。
        raise WorkflowCancelledError(
            f"flow {workflow_id!r} no longer exists while parked "
            "(removed or evicted); the flow body cannot continue"
        )
    state_name = state.get("state")
    if state_name == WORKFLOW_STATE_FAILED:
        if _deadline_failure(state):
            raise ActantTimeoutError(
                f"flow {workflow_id!r} exceeded its workflow deadline "
                f"(error={_WORKFLOW_TIMEOUT_ERROR!r})"
            )
        raise WorkflowFailedError(
            f"flow {workflow_id!r} failed while parked "
            f"(error={state.get('error')!r})"
        )
    if state_name == WORKFLOW_STATE_CANCELLED:
        raise WorkflowCancelledError(
            f"flow {workflow_id!r} was aborted (workflow cancelled)"
        )


def _park_payload(
    runtime: Any, workflow_id: str, wait_key: str, *, bound_ms: int
) -> bytes | None:
    """在已注册的等待点上 park，返回唤醒 payload（**唯一** park 入口）。

    ``bound_ms == 0`` 表示不设上界。返回 ``None`` 的唯一含义是"**真正的上界
    超时**"：若 park 是被**终态释放**唤醒的（``complete_terminal`` 释放了本工作流
    的 park 等待者），:func:`_raise_if_workflow_terminal` 会直接抛错而不返回。

    三个 park 调用点（``sleep_until`` / ``wait_signal`` / ``suspend``）都必须经
    本函数——``wait_signal`` 曾直接调 ``wait_wait_point``，于是终态释放被它当成
    "信号返回了空 payload"，函数体在已死的工作流上继续跑到返回。单一入口是为了
    让这条判定不可能漂移。
    """
    payload = runtime.wait_wait_point(workflow_id, wait_key, timeout_ms=bound_ms)
    if payload is None:
        _raise_if_workflow_terminal(runtime, workflow_id)
    return cast("bytes | None", payload)


def _park(runtime: Any, workflow_id: str, wait_key: str, *, bound_ms: int) -> bool:
    """[`_park_payload`] 的布尔视图：``False`` 仅表示**真正的上界超时**。

    调用方的"未唤醒 ⇒ 抛超时 / 返回 None"分支因此只处理真正的超时；
    终态释放已经在 :func:`_park_payload` 内抛错，不会走到这里。
    """
    return _park_payload(runtime, workflow_id, wait_key, bound_ms=bound_ms) is not None


def sleep_until(deadline_ms: int, *, timeout_ms: int = 0) -> None:
    """在 flow 函数体内挂起到指定时刻（Timers）。

    等待点以 ``timer-{等待序号}`` 为键注册到 orchestrator 并随工作流快照落盘：
    即使等待期间节点崩溃重启，恢复后的 flow 重放会命中同一序号与同一键，
    **已到期的等待点直接返回**（不重复挂起），未到期的在原 deadline 上继续等待。
    到期由 Rust 侧超时 watcher 的轮询周期驱动（``state_poll_interval_ms``，
    默认 500ms），故唤醒延迟上界 = 该周期。

    确定性契约：``deadline_ms`` 是 wall-clock 绝对时刻，**不参与**提交序列指纹；
    跨重放稳定的是"第 n 次 sleep"这个身份，不是具体时刻。

    Args:
        deadline_ms: 绝对 epoch 毫秒到期时刻（``int(time.time() * 1000) + 延迟``）。
        timeout_ms: 安全上界；``0`` 表示取 flow 自身的 ``timeout_ms``
            （仍为 0 则不设上界）。

    Raises:
        InvalidStateError: 不在 flow 内 / 无 Runtime。
        ActantTimeoutError: 上界内未被唤醒（含 flow 自身 deadline 到期）。
            睡眠提前返回会破坏 flow 的确定性契约，故此处**不**静默返回。
    """
    runtime, state = _wait_context()
    wait_key = f"timer-{state.next_wait_seq()}"
    runtime.register_wait_point(
        state.workflow_id, wait_key, kind="timer", deadline_ms=deadline_ms
    )
    bound = timeout_ms or state.timeout_ms
    if not _park(runtime, state.workflow_id, wait_key, bound_ms=bound):
        raise ActantTimeoutError(
            f"flow {state.workflow_id}: sleep_until({deadline_ms}) 在 {bound}ms 内未被唤醒"
        )


def wait_signal(name: str, *, timeout_ms: int = 0) -> bytes | None:
    """在 flow 函数体内等待名为 ``name`` 的外部信号（Signals）。

    ``name`` 即等待点键：外部用 ``Runtime.signal_wait_point(workflow_id, name)``
    递交。等待点为**闭锁（latch）语义**——同一名字在工作流内只注册一次，
    重复 await 直接返回已收到的 payload，这是重放体天然幂等的前提
    （"已收到 → 直接返回"）。

    **信号可先于等待点抵达**：递交方无需等 flow 跑到本行——等待点注册时若
    已有同名缓冲信号，会**立即生成为已唤醒态**，本函数随即返回。递交方不必
    按返回值重试。

    缓冲**跨重启存活**（随 `orch:sigbuf:` 落盘，重放路径也会缓冲先到的信号）；
    唯一会丢的窗口是"缓冲写入后、尚未落盘就崩溃"。

    Args:
        name: 信号名（同时作为等待点键，工作流内唯一）。
        timeout_ms: ``>0`` 时为**有界**等待；``0`` 表示不设调用方上界，
            仅受 flow 自身 ``timeout_ms`` 约束。

    Returns:
        收到信号时的 payload（当前恒为空 ``bytes``，payload 通道预留给后续扩展）。
        仅在有界等待（``timeout_ms > 0``）超时时返回 ``None``。

    Raises:
        InvalidStateError: 不在 flow 内 / 无 Runtime。
        ActantTimeoutError: 未显式指定上界，而 flow 自身 deadline 已到期。
            未请求的提前返回会破坏确定性契约，故报错而非静默返回。
    """
    runtime, state = _wait_context()
    runtime.register_wait_point(state.workflow_id, name, kind="signal", name=name)
    if timeout_ms > 0:
        return _park_payload(
            runtime, state.workflow_id, name, bound_ms=timeout_ms
        )
    bound = state.timeout_ms
    payload = _park_payload(runtime, state.workflow_id, name, bound_ms=bound)
    if payload is None and bound > 0:
        raise ActantTimeoutError(
            f"flow {state.workflow_id}: wait_signal({name!r}) 在 flow deadline "
            f"{bound}ms 内未收到信号"
        )
    return payload


def suspend(*, timeout_ms: int = 0) -> None:
    """在 flow 函数体内挂起，等待外部显式恢复。

    注册一个**挂起条件**等待点（``kind="suspend"``）后 park 当前线程，由
    ``Runtime.resume_suspended(workflow_id)`` 唤醒。等待点随工作流快照落盘，
    故挂起可以跨越节点重启：重放体再次执行到本函数时，若该序号对应的等待点
    已被恢复（已 ``Signaled``）则**立即返回**，不重复挂起。

    与 :func:`wait_signal` 的区别是**语义来源**：``wait_signal`` 等一个具名业务
    事件，``suspend`` 等操作员的恢复指令。因此 ``resume_suspended`` **只唤醒挂起
    点**，不会冒名顶替一个业务信号；反之 ``signal_wait_point`` 也必须用正确的
    信号名才能唤醒对应的 ``wait_signal``。

    键取 ``suspend-{等待序号}``（与 ``timer-{n}`` 同构），故同一 flow 内多次挂起
    各自独立，且每次挂起是独立的历史条目。

    中止不需要专用 API：``Runtime.cancel_workflow(workflow_id)`` 会让工作流进入
    ``Cancelled`` 终态，orchestrator 随即释放本工作流的 park 等待者，本函数因此
    抛出 :class:`WorkflowCancelledError`（工作流因其他原因失败时抛
    :class:`WorkflowFailedError`；deadline 到期抛 :class:`ActantTimeoutError`）。

    Args:
        timeout_ms: 安全上界；``0`` 表示取 flow 自身的 ``timeout_ms``
            （仍为 0 则无限等待，直到被恢复或被终态释放）。

    Raises:
        InvalidStateError: 不在 flow 内 / 无 Runtime。
        ActantTimeoutError: 上界内未被恢复（含 flow 自身 deadline 到期）。
        WorkflowCancelledError: 挂起期间工作流被取消（含工作流已被移除）。
        WorkflowFailedError: 挂起期间工作流因其他原因失败。

    Note:
        挂起是**协作式**的：函数体在等待点 park，Python 无法抢占正在执行的字节码。
        没有挂起点包住的纯 CPU 段不会被中止或超时打断（同 :func:`sleep_until`）。
    """
    runtime, state = _wait_context()
    wait_key = f"{_SUSPEND_WAIT_PREFIX}{state.next_wait_seq()}"
    runtime.register_wait_point(state.workflow_id, wait_key, kind=_SUSPEND_KIND)
    bound = timeout_ms or state.timeout_ms
    if not _park(runtime, state.workflow_id, wait_key, bound_ms=bound):
        raise ActantTimeoutError(
            f"flow {state.workflow_id}: suspend() 在 {bound}ms 内未被恢复"
        )


def register_flow_recovery(
    name: str,
    func: Callable[..., Any],
    *,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
) -> None:
    """登记 flow 的续跑恢复点。

    ``@flow`` 已在导入时登记"flow 名 → 函数体"，本函数额外提供**调用参数**——
    参数无法从 ``workflow_id`` 反推（``{flow_name}-{uuid}`` 只编码名字），框架
    也不持久化任意 Python 对象（那会退化成函数体快照，与"持久化只登记
    函数体与调用参数、不存任意对象"的边界冲突）。因此由调用方显式给出。

    Args:
        name: flow 名（即 ``@flow(name=...)`` 的值；未显式命名时为函数 ``__qualname__``）。
        func: flow 函数体。**传 ``@flow`` 装饰后的对象或装饰前的原函数都可以**：
            装饰后的对象会被自动解包到原函数——若不解包，重放会再次进入
            ``@flow`` 包装器并**生成新的 workflow_id**，于是"重放"退化为"新建一个
            工作流从头跑"，已完成节点全部重跑（危险且静默）。
        args: 重放时传给函数体的位置参数。须与首次执行**语义一致**——
            参数参与不了指纹校验，不一致会导致提交序列漂移（重放体靠
            ``FlowReplayError`` 兜底，但更早暴露更好）。
        kwargs: 重放时传给函数体的关键字参数。

    Raises:
        ValueError: ``func`` 是 ``@flow`` 包装器但无法解包出原函数。

    Note:
        同名重复登记 = 覆盖。零参 flow 无需调用本函数（``@flow`` 已登记空参数）。
    """
    decorated_name = getattr(func, "__actant_flow_name__", None)
    if decorated_name is not None:
        raw = getattr(func, "__wrapped__", None)
        if raw is None:
            raise ValueError(
                f"register_flow_recovery({name!r}): got an @flow wrapper for "
                f"{decorated_name!r} without __wrapped__; pass the undecorated function"
            )
        func = raw
    _FLOW_RECOVERY[name] = _FlowRecovery(func, tuple(args), dict(kwargs or {}))


def _flow_name_from_id(workflow_id: str) -> str | None:
    """从 ``{flow_name}-{uuid8}`` 恢复 flow 名；无法识别时返回 ``None``。

    只剥掉最后一个 ``-`` 之后的段。形如 ``wp-restart-wf``（手工创建的
    workflow_id）会得到 ``wp-restart``——识别不出就返回 ``None`` 更安全，
    故这里要求尾段是 8 位十六进制（``uuid4().hex[:8]`` 的形态）。
    """
    head, sep, tail = workflow_id.rpartition("-")
    if not sep or len(tail) != 8:
        return None
    if any(c not in "0123456789abcdef" for c in tail):
        return None
    return head or None


def resume_flows(
    *,
    runtime: Any = None,
    timeout_ms: int = 0,
) -> list[str]:
    """扫描并重放未终态工作流（续跑驱动）。

    枚举来源是 ``Runtime.list_workflows()``——它返回**非终态**活跃工作流
    （``active_workflow_ids()`` 已过滤终态），故无需新增查询原语。对每个工作流：

    - 从 ``workflow_id`` 前缀恢复 flow 名；
    - 查恢复登记（``@flow`` 导入登记 + :func:`register_flow_recovery` 补充参数）；
    - 命中 → 经 :func:`_replay_flow` 重放：已完成节点**不重跑**，缺口节点补提交，
      重放体再次执行到等待点时"已唤醒的直接返回"；
    - 未命中 → 记录 warning 并跳过（**不静默**：否则"重启后什么都没发生"是最难
      排查的失败形态）。

    Args:
        runtime: 目标 Runtime；``None`` 时取当前上下文。
        timeout_ms: 重放体的**工作流级 deadline**（0 表示不设；等待上界由工作流级 deadline 决定）。已在历史中的工作流沿用其持久化 deadline。

    Returns:
        实际发起重放的 ``workflow_id`` 列表（跳过的不计入）。

    Raises:
        InvalidStateError: 无可用 Runtime。

    Note:
        本函数**显式调用**，不在 ``Runtime.start()` 时自动执行——自动恢复会在启动
        路径里执行用户 Python 代码（可能 ImportError）。
    """
    if runtime is None:
        runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            "resume_flows: no active Runtime; wrap your code in `with actant.Runtime() as rt:`"
        )
    resumed: list[str] = []
    # 重放体会调用 `Task.submit`，它从**线程局部**取 Runtime——`_run_body` 本身
    # 不建立该上下文（正常入口由 `@flow` 的调用方持有）。故此处显式建立，避免
    # 调用方必须自己套 `use_runtime`。
    with use_runtime(runtime):
        for workflow_id in runtime.list_workflows():
            name = _flow_name_from_id(workflow_id)
            entry = _FLOW_RECOVERY.get(name) if name is not None else None
            if entry is None:
                _logger.warning(
                    "resume_flows: workflow %s has no recovery registration "
                    "(resolved flow name %r); skipping",
                    workflow_id,
                    name,
                )
                continue
            _logger.info(
                "resume_flows: replaying workflow %s (flow %r)", workflow_id, name
            )
            try:
                _replay_flow(
                    entry.func,
                    workflow_id,
                    entry.args,
                    entry.kwargs,
                    runtime=runtime,
                    timeout_ms=timeout_ms,
                )
            except ActantTimeoutError:
                # 单个工作流的 deadline 在重放期间到期：它已被 orchestrator
                # 置为终态（Failed），不是本轮的失败。跳过它继续恢复其余
                # 工作流——驱动器的职责是"尽量多恢复"，不是"一遇异常全停"。
                _logger.warning(
                    "resume_flows: workflow %s exceeded its deadline "
                    "during replay; skipping",
                    workflow_id,
                )
                continue
            resumed.append(workflow_id)
    return resumed


def flow(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay_ms: int = 0,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
) -> Any:
    """装饰器：将函数标记为工作流，提供生命周期事件、超时与上下文校验。

    被装饰的函数行为：

    1. 调用前校验存在活跃 ``Runtime``（否则抛 ``InvalidStateError``）。
    2. 生成 ``workflow_id`` 并设置 flow 上下文，使函数体内的 ``task.submit()``
       走 orchestrator 驱动的持久化编排路径（节点先持久化再派发）。
    3. 广播 ``WorkflowLifecycle`` 事件：``submitted``/``started`` 在函数体执行前
       实时广播；``completed``/``failed`` 由 Orchestrator 实际持久化状态驱动
       （最后一个任务完成触发工作流终态后广播）。
    4. Flow 级超时（``timeout_ms``）：作为**工作流级 deadline** 传给
       Orchestrator，到期由超时 watcher 强还原并抛
       ``ActantTimeoutError``。

    Args:
        func: 被装饰的编排函数（无参装饰器时由 ``flow`` 自动填充）。
        name: 工作流名称（默认 ``func.__qualname__``），用于日志与事件。
        retries: 不使用（仅为签名兼容保留）。重试的唯一执行者是 orchestrator：
            任务级 ``@task(retries=...)`` 映射为节点 RetryPolicy，函数体重试由
            续跑/重放机制承载。传入非零值只记录 warning，不生效。
        retry_delay_ms: 未使用（保留签名兼容，与 ``retries`` 一同废弃）。
        timeout_ms: 工作流级 deadline 毫秒（0=不限制）。到期由 orchestrator
            强还原：标记工作流 ``Failed``（``workflow timeout exceeded``）并
            取消全部运行中任务；阻塞在任务等待上的函数体随之被唤醒并抛
            ``ActantTimeoutError``。

            .. warning::
                **超时不再"立即返回"**：函数体在 deadline 之后才被唤醒，
                返回时刻在 ``deadline + state_poll_interval_ms``（默认 500ms）
                量级。没有挂起点/任务等待包住的**纯 CPU 段不会被中断**
                （Python 无法中断线程），应在内部自查
                ``Runtime.get_workflow_state`` 或拆分为多个 ``Task.submit``。
                函数体若**已正常返回**而 deadline 已到期，调用方**同样抛**
                ``ActantTimeoutError``——返回值不得被当作成功（否则失败看起来
                像成功）；判定式与异常路径共用同一事实源（工作流终态）。

        failure_strategy: 工作流的失败策略。``None``
            （默认）不显式传递该参数，由 Rust 侧应用默认策略
            ``FailureStrategy::FailFast``（任一任务失败立即标记工作流失败）。
            ``"fail_fast"`` 显式 fail-fast；``"continue"`` 任务失败后工作流
            继续执行直到所有任务到达终态。装饰时校验，非法值抛 ``ValueError``。

    Raises:
        InvalidStateError: 无活跃 Runtime。
        ActantTimeoutError: 工作流 deadline 到期且函数体因此被中断。
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
        if retries > 0:
            _logger.warning(
                "flow: the `retries` argument is not used; use "
                "@task(retries=...) instead, which is orchestrator-driven for "
                "flow tasks"
            )
        flow_name = name or f.__qualname__
        # 续跑登记：重放需要"函数体"，而 workflow_id 只能恢复出 flow 名
        # （`{flow_name}-{uuid}`），故函数体必须在导入时登记。
        # 参数留空，由 `register_flow_recovery` 显式提供。
        _FLOW_RECOVERY[flow_name] = _FlowRecovery(f, (), {})

        @wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            runtime = get_current_runtime()
            if runtime is None:
                raise InvalidStateError(
                    f"flow {flow_name!r}: no active Runtime; "
                    "wrap your code in `with actant.Runtime() as rt:`"
                )
            workflow_id = f"{flow_name}-{uuid.uuid4().hex[:8]}"
            # **不在此处广播 submitted / started**：工作流是惰性创建的，
            # 此刻它可能还不存在——广播一个不可用的 id 会让外部观察者按该 id
            # 立即递交信号时撞 NotFound。事件改由 `_ensure_workflow_created`
            # 在真正建槽之后广播。
            state = _FlowState(
                workflow_id,
                failure_strategy=failure_strategy,
                timeout_ms=timeout_ms,
            )
            try:
                result = _run_body(f, args, kwargs, state)
            except BaseException as exc:
                # 函数体被工作流 deadline 强还原唤醒时，任务级终态是
                # Cancelled（worker 确实被取消了，这是事实）。但对调用方而言
                # 事实源是工作流超时，故归一为 ActantTimeoutError，保持该参数
                # 既有错误类型契约。
                if _workflow_deadline_expired(runtime, state):
                    timeout_exc = ActantTimeoutError(
                        f"flow {state.workflow_id!r} exceeded its workflow "
                        f"deadline (error={_WORKFLOW_TIMEOUT_ERROR!r})"
                    )
                    _settle_flow_failure(runtime, state, timeout_exc)
                    raise timeout_exc from exc
                _settle_flow_failure(runtime, state, exc)
                raise
            # 函数体返回：不回灌任何结果。先封口节点集（增量提交期间
            # 全部已知节点终态不代表提交序列结束），再等待终态并广播与实际
            # 状态一致的生命周期事件。
            if state.workflow_created:
                runtime.seal_workflow(workflow_id)
            terminal = _wait_terminal_and_emit(runtime, workflow_id)
            # 工作流以 deadline 到期收尾时，调用方必须看到超时——函数体自我
            # 完成（如纯 CPU 段跑完）不代表成功。`failed` 事件已由上面广播。
            if _deadline_failure(terminal):
                raise ActantTimeoutError(
                    f"flow {workflow_id!r} exceeded its workflow deadline "
                    f"(error={_WORKFLOW_TIMEOUT_ERROR!r})"
                )
            return result

        # 标记：使 `register_flow_recovery` 能识别"传进来的是包装器而非原函数"
        # 并自动解包（否则重放会生成新 workflow_id，静默退化为从头重跑）。
        setattr(wrapper, "__actant_flow_name__", flow_name)  # noqa: B010
        return wrapper

    if func is None:
        return _make
    return _make(func)


#: 工作流级 deadline 到期时 orchestrator 写入的错误串（`execution.rs`
#: 的 `mark_workflow_failed("workflow timeout exceeded")`）。Python 侧据此
#: 判定"这次失败是 deadline 强还原"，而非普通任务失败。
_WORKFLOW_TIMEOUT_ERROR = "workflow timeout exceeded"


def _deadline_failure(state: dict[str, Any] | None) -> bool:
    """工作流终态是否为「deadline 到期」这一事实（唯一判定式）。

    依据 orchestrator 写入的错误串判定，不在 Python 侧复算计时器——
    ``mark_workflow_failed("workflow timeout exceeded")`` 是唯一写入点。
    """
    return bool(
        state
        and state.get("state") == WORKFLOW_STATE_FAILED
        and (state.get("error") or "") == _WORKFLOW_TIMEOUT_ERROR
    )


def _workflow_deadline_expired(runtime: Any, state: _FlowState) -> bool:
    """函数体异常退出后，判断这次失败是否源于工作流级 deadline 强还原。

    查询失败（Runtime 已关停等）时返回 ``False`` 放行原始异常——此时原始
    异常信息量更大。
    """
    if not state.workflow_created:
        return False
    try:
        wf_state = runtime.get_workflow_state(state.workflow_id)
    except Exception:
        _logger.debug(
            "flow %s: could not read workflow state after body failure",
            state.workflow_id,
            exc_info=True,
        )
        return False
    return _deadline_failure(wf_state)


def _run_body(
    func: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    state: _FlowState,
) -> R:
    """在 flow 上下文中于**调用线程**执行函数体。

    不再有子线程/``cancel_event``/孤儿 join：超时的唯一决策者是 orchestrator
    的超时 watcher（工作流级 deadline），它取消运行中任务后，本次阻塞在任务
    等待上的调用自然被唤醒。函数体全程与调用方同线程，因此
    ``Runtime.stop()`` 无需 join 任何 flow 线程。
    """
    with _FlowContext(state):
        return func(*args, **kwargs)


def _settle_flow_failure(runtime: Any, state: _FlowState, exc: BaseException) -> None:
    """flow 函数体失败/超时的收尾：工作流终态化 + 级联取消 + 失败事件。

    - ``cancel_workflow`` 把仍在运行/排队的工作流置为 Cancelled 终态
      （已终态或未创建时幂等/忽略）——防止编排节点在失败后继续执行；
    - 本地句柄级联取消（best-effort，与既有语义一致）；
    - 广播 ``failed`` 事件。错误源是函数体异常本身（任务失败经
      ``AsyncResult.result()`` 重抛的同源异常）。
    """
    workflow_id = state.workflow_id
    try:
        if state.workflow_created:
            runtime.cancel_workflow(workflow_id)
    except Exception:
        # 工作流可能已终态（fail-fast 已触发）：取消是兜底动作，失败不应
        # 掩盖原始异常。
        _logger.debug(
            "flow %s: cancel_workflow skipped (%s)",
            workflow_id, type(exc).__name__,
        )
    _cancel_flow_tasks(workflow_id)
    _safe_emit(workflow_id, "failed", error=f"{type(exc).__name__}: {exc}")


def _wait_terminal_and_emit(
    runtime: Any, workflow_id: str
) -> dict[str, Any] | None:
    """等待工作流到达终态，广播生命周期事件，并返回该终态快照。

    轮询 ``get_workflow_state``（终态由最后一个任务完成触发）。
    工作流不存在（空 flow 未创建 / 已被淘汰）时直接广播 ``completed`` 并返回
    ``None``（空 flow 无编排数据，语义上立即完成）。

    返回终态快照供调用方做终局裁决（``failed`` 且 error 为 deadline 到期
    时，调用方须抛 ``ActantTimeoutError`` 而非返回结果）。

    Python 侧不设等待上界：工作流带
    deadline 时，到期由 orchestrator 超时 watcher 把工作流置为终态，本循环
    因此必然退出；不带 deadline 的工作流本就允许任意时长运行。
    """
    while True:
        state = cast("dict[str, Any] | None", runtime.get_workflow_state(workflow_id))
        if state is None:
            _safe_emit(workflow_id, "completed")
            return None
        state_name = state.get("state")
        if state_name == WORKFLOW_STATE_COMPLETED:
            _safe_emit(workflow_id, "completed")
            return state
        if state_name == WORKFLOW_STATE_FAILED:
            _safe_emit(workflow_id, "failed", error=state.get("error") or "")
            return state
        if state_name == WORKFLOW_STATE_CANCELLED:
            _safe_emit(
                workflow_id, "failed",
                error=state.get("error") or "workflow cancelled",
            )
            return state
        time.sleep(_FLOW_TERMINAL_POLL_INTERVAL_S)


def _replay_flow(
    func: Callable[..., R],
    workflow_id: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    runtime: Any = None,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
) -> R:
    """续跑入口：以既有 ``workflow_id`` 重放 flow 函数体。

    第 n 次 ``submit`` 查工作流历史：节点已完成 → 返回记录结果（不重跑）；
    进行中 → 返回绑定句柄；不存在 → 新提交。指纹不一致抛 ``FlowReplayError``。
    不重复广播 ``submitted``/``started``（原始执行已广播）。

    ``timeout_ms`` 是**工作流级 deadline**，只在重放体惰性创建/更新工作流
    时生效；已在历史中的工作流沿用其持久化 deadline。

    重放由 :func:`resume_flows` 等上层恢复逻辑显式触发，不在启动路径自动执行。
    """
    if runtime is None:
        runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            f"flow replay {workflow_id!r}: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    state = _FlowState(
        workflow_id,
        failure_strategy=failure_strategy,
        timeout_ms=timeout_ms,
        # 重放体不得重复广播 submitted/started（原始执行已广播）。它走的是同一条
        # 惰性创建路径，故靠该标志关闭广播。
        announce=False,
    )
    try:
        result = _run_body(func, args, kwargs, state)
    except BaseException as exc:
        # 与 `@flow` 入口同构：deadline 强还原导致的失败对调用方呈现为超时。
        if _workflow_deadline_expired(runtime, state):
            raise ActantTimeoutError(
                f"flow {workflow_id!r} exceeded its workflow deadline "
                f"(error={_WORKFLOW_TIMEOUT_ERROR!r})"
            ) from exc
        raise
    # 重放命中既存节点时工作流外壳已在历史中，无需创建；封口同样必须执行
    # （重放体的提交序列在函数体返回后才允许终态判定）。
    runtime.seal_workflow(workflow_id)
    terminal = _wait_terminal_and_emit(runtime, workflow_id)
    if _deadline_failure(terminal):
        raise ActantTimeoutError(
            f"flow {workflow_id!r} exceeded its workflow deadline "
            f"(error={_WORKFLOW_TIMEOUT_ERROR!r})"
        )
    return result


def _replay_node_outcome(runtime: Any, handle: Any, outcome: dict[str, Any]) -> None:
    """按重放命中的历史状态重建句柄终态（供 ``Task._flow_submit`` 调用）。

    ``outcome`` 为 ``add_workflow_node`` 返回的 ``created=False`` 结果：
    ``Completed`` → 经 ``Runtime._on_task_result`` 复用结果解析链（含大结果
    Ref 降级），``Failed``/``Cancelled``/``Skipped`` → 对应失败/取消终态；
    ``Pending``/``Running`` → 句柄保持等待（事件到达时解析）。
    """
    state_name = outcome.get("state")
    task_id = handle.task_id
    if state_name == "Completed":
        runtime._on_task_result(
            SimpleNamespace(
                task_id=task_id, state="Completed",
                result=outcome.get("result"), error=None,
            )
        )
    elif state_name == "Failed":
        runtime._on_task_result(
            SimpleNamespace(
                task_id=task_id, state="Failed", result=None,
                error=outcome.get("error") or "task failed (recorded in workflow history)",
            )
        )
    elif state_name == "Cancelled":
        runtime._on_task_result(
            SimpleNamespace(task_id=task_id, state="Cancelled", result=None, error=None)
        )
    elif state_name == "Skipped":
        handle._set_error("task skipped")
    # Pending / Running：保持等待，orchestrator 派发结果经事件回灌解析。


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
    "register_flow_recovery",
    "resume_flows",
    "sleep_until",
    "wait_signal",
]
