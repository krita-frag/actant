"""工作流编排：``@flow`` 装饰器（重放模型）。

``@flow`` 把普通函数变成可持久化、可重放的工作流：函数体内调用 ``task.submit()``
组合任务，``AsyncResult`` 作为下游参数时自动解析依赖。节点由 Rust Orchestrator
先持久化再派发；崩溃后按「快照 + 事件重放」续跑，已完成节点不重跑。

设计细节见 ``docs/guide/flows.md``：返回值与终态、提交与重放、重试归属、
确定性契约、代码演进、超时、等待点、挂起与恢复。

用法::

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

    with actant.Runtime() as rt:
        rt.layer("WorkflowLifecycle", "emit").chain(
            lambda e: print(f"flow {e.kind}: {e.workflow_id}")
        )
        print(pipeline("data.txt"))
"""

from __future__ import annotations

import logging
import random
import struct
import threading
import time
import uuid
from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass
from functools import partial, wraps
from types import SimpleNamespace
from typing import Any, NoReturn, ParamSpec, TypeVar, cast

import cloudpickle

from actant._internal.runtime import get_current_runtime, use_runtime
from actant._internal.runtime_base import (
    _CONTINUED_TO_CONTEXT_KEY as _CONTINUED_TO_KEY,
)
from actant._internal.runtime_base import (
    WORKFLOW_STATE_CANCELLED,
    WORKFLOW_STATE_COMPLETED,
    WORKFLOW_STATE_FAILED,
)
from actant._internal.tasks.helpers import CallbackErrorPolicy
from actant.capabilities import WORKFLOW_LIFECYCLE, WorkflowEvent
from actant.driver import BlockingDriver, FlowDriver
from actant.effects import emit
from actant.exceptions import (
    ActantTimeoutError,
    FlowReplayError,
    InvalidStateError,
    NotFoundError,
    WorkflowCancelledError,
    WorkflowFailedError,
)

_logger = logging.getLogger("actant.flow")

P = ParamSpec("P")
R = TypeVar("R")

# DAG 合法失败策略（镜像 Rust FailureStrategy::parse 接受的规范字符串）。
_VALID_FAILURE_STRATEGIES = ("fail_fast", "continue")

# flow 体返回后轮询工作流终态的间隔（秒）。终态由最后一个任务完成触发，
# 无推送通道可用（TerminalWaiterRegistry 未暴露 PyO3 桥），轮询复用
# get_workflow_state 既有查询面。
_FLOW_TERMINAL_POLL_INTERVAL_S = 0.05

# host loop 在一轮扫描没有可驱动工作流时的休眠间隔（秒），避免忙等。
_HOST_IDLE_POLL_INTERVAL_S = 0.5

#: 挂起等待点的条件种类（Rust 侧 `WaitCondition::Suspend`）。
_SUSPEND_KIND = "suspend"

#: 挂起等待点的键前缀。与 `timer-{n}` 同构：键由**等待序号**决定，故同一
#: flow 内多次挂起各自独立（`resume_suspended` 按 workflow_id 唤醒，不要求
#: 调用方知道具体键）。
_SUSPEND_WAIT_PREFIX = "suspend-"

#: 当前 flow 上下文状态。
#:
#: 用 ``ContextVar`` 而非 ``threading.local``：同步 flow 仍在单线程内执行，行为
#: 不变；``@aio.flow`` 的多个协程共享同一个 event loop 线程，只有 contextvar
#: 能随 asyncio Task 隔离——``await`` 让出控制权后，其他协程设置的状态不会
#: 污染本协程。
_flow_state: ContextVar[_FlowState | None] = ContextVar("actant_flow_state", default=None)

#: flow 定义版本在确定性上下文表中的保留键/种类。
#:
#: 版本与 ``ctx-{n}`` 共用同一张记录表（它本质上也是一个"首次写入、重放读回"
#: 的确定性值），但键空间独立：``ctx-{n}`` 由调用序号决定，版本键是单例。
_FLOW_VERSION_KEY = "@flow_version"
_FLOW_VERSION_KIND = "workflow_version"

#: ``patched()`` 分支决策在确定性上下文表中的键前缀/种类。
#:
#: 键是 ``patch:{change_id}``（按 change_id 寻址，而非调用序号）：同一个
#: change_id 在重放体里无论执行多少次都取回同一条决策。
_PATCH_KEY_PREFIX = "patch:"
_PATCH_KIND = "patched"

#: ``continue_as_new`` 携带参数在**新工作流**上下文表中的保留键/种类。
#:
#: 续跑的持久性靠它：新工作流外壳先落盘、携带参数随之入历史，之后函数体才
#: 在新工作流上执行。进程若在两者之间崩溃，``host_flows`` 读到携带参数即可
#: 用正确入参恢复新工作流，而不是沿用上一代的 invocation 入参。
_CONTINUE_CARRY_KEY = "@continue_args"
_CONTINUE_CARRY_KIND = "continue_args"

#: ``continue_as_new`` 写在**旧工作流**上的"下一代 id"记录键/种类。
#:
#: 旧代重放（"新代已建、旧代未封口"时崩溃）会再次执行到 ``continue_as_new``：
#: 记录值使重放复用同一个新代 id 与同一份携带参数，而不是每重放一次就分裂出
#: 一代新工作流。续跑因此是幂等的。
_CONTINUED_TO_KIND = "continued_to"


@dataclass(frozen=True)
class FlowDefinition:
    """可重放的 flow 定义：函数体 + 装饰时声明的执行参数。

    与具体调用无关——调用参数由 ``WorkflowInvocation`` 随工作流落盘，故同一
    定义可在任意进程、任意 ``Runtime`` 上被 :func:`host_flows` 重放。
    """

    name: str
    func: Callable[..., Any]
    version: int = 0
    timeout_ms: int = 0
    failure_strategy: str | None = None
    #: 函数体是否为 ``async def``。``host_flows`` 据此选择协程化重放闭包。
    is_async: bool = False


class FlowRegistry:
    """flow 名 → :class:`FlowDefinition` 的进程内登记表。

    ``@flow`` 在导入时登记定义，使 host loop 能只凭 journal 中的 flow 名
    找到函数体。同名后注册者覆盖先注册者（热重载/测试场景）。
    """

    def __init__(self) -> None:
        self._definitions: dict[str, FlowDefinition] = {}
        self._lock = threading.RLock()

    def register(self, definition: FlowDefinition) -> None:
        with self._lock:
            self._definitions[definition.name] = definition

    def get(self, name: str) -> FlowDefinition | None:
        with self._lock:
            return self._definitions.get(name)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._definitions)

    def __contains__(self, name: object) -> bool:
        with self._lock:
            return name in self._definitions


#: 进程级 flow 登记表。host loop 与 ``@flow`` 共享同一事实源。
FLOW_REGISTRY = FlowRegistry()


class _FlowState:
    """单个 flow 实例的运行状态（重放模型）。

    - ``workflow_id``：任务的归属工作流（TaskEvent / 节点写入目标）。
    - ``timeout_ms``：工作流级 deadline。**不在 Python 侧判定**——到期
      由 orchestrator 的超时 watcher 强还原，本类只负责把它转交
      ``_submit_workflow``（惰性创建时携带）。
    - ``_seq``：提交序号计数器。每次 flow 内 ``submit``（含重放命中）递增，
      与节点确定性标识 ``{任务名}-{序号}-{workflow_id}`` 共同构成
      "第 n 次提交" 的身份；重放体执行到同一序号时据此查历史。
    - ``workflow_created``：工作流外壳是否已持久化（惰性创建——空 flow
      不产生编排数据，不创建工作流）。
    - ``version``：本次执行的 flow 定义版本（``@flow(version=...)``）。只在
      工作流外壳创建时与历史记录比对，不参与提交序列指纹。
    - ``decisions``：本次执行中 ``patched(change_id)`` 的调用参数快照，用于
      拒绝"同一 change_id 在一次执行里被赋予矛盾默认值"的漂移。
    - ``flow_name``：flow 名（``continue_as_new`` 用它派生新工作流 id）。
    """

    def __init__(
        self,
        workflow_id: str,
        *,
        flow_name: str = "",
        failure_strategy: str | None = None,
        timeout_ms: int = 0,
        version: int = 0,
        announce: bool = True,
        invocation_args: bytes = b"",
        existing: bool = False,
    ) -> None:
        self.workflow_id = workflow_id
        self.flow_name = flow_name
        self.failure_strategy = failure_strategy
        self.timeout_ms = timeout_ms
        self.version = version
        self.invocation_args = invocation_args
        self.decisions: dict[str, bool] = {}
        self.existing = existing
        self._seq = 0
        self._wait_seq = 0
        self._ctx_seq = 0
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

    def next_ctx_seq(self) -> int:
        """分配下一次确定性上下文调用的序号（从 1 开始递增）。

        与提交序号、等待序号分开计数：上下文值的身份是"第 n 次 ctx 调用"，
        新增/删减任务提交不应平移上下文值的历史键。
        """
        self._ctx_seq += 1
        return self._ctx_seq

    @property
    def node_count(self) -> int:
        """本次 flow 执行已提交（或重放命中）的节点数。"""
        return self._seq


class FlowHandle:
    """已提交（detached）flow 的句柄。

    :func:`start_flow` 返回它时工作流外壳与 invocation 已落盘，但函数体尚未
    执行；函数体由 :meth:`Runtime.host_flows` 在任一持有相同 ``data_dir`` 与
    flow 定义的进程中驱动。句柄本身可跨进程重建（只需 ``workflow_id``）。
    """

    def __init__(self, runtime: Any, workflow_id: str, flow_name: str) -> None:
        self._runtime = runtime
        self.workflow_id = workflow_id
        self.flow_name = flow_name

    def __repr__(self) -> str:
        return f"FlowHandle(workflow_id={self.workflow_id!r}, flow={self.flow_name!r})"

    def state(self) -> dict[str, Any] | None:
        """工作流执行状态快照（``None`` 表示工作流不存在）。"""
        return cast(
            "dict[str, Any] | None",
            self._runtime.get_workflow_state(self.workflow_id),
        )

    def journal(self) -> list[dict[str, Any]]:
        """该工作流的稳定 journal（见 :meth:`Runtime.get_workflow_journal`）。"""
        return cast(
            "list[dict[str, Any]]",
            self._runtime.get_workflow_journal(self.workflow_id),
        )

    def result(self, *, timeout_ms: int = 0) -> Any:
        """等待工作流进入终态并返回持久化结果。

        **不执行函数体**：本方法只读工作流状态与返回值，故可在提交进程之外
        调用。需要驱动执行时用 :meth:`Runtime.host_flows`。

        Args:
            timeout_ms: 本地等待上界（0=不限）。超时只停止等待，不取消工作流。

        Raises:
            NotFoundError: 工作流不存在或返回值未落盘。
            ActantTimeoutError: 本地等待超时，或工作流因 deadline 到期失败。
            WorkflowCancelledError: 工作流被取消。
            WorkflowFailedError: 工作流以其他原因失败。
        """
        terminal = _wait_terminal(self._runtime, self.workflow_id, timeout_ms=timeout_ms)
        state_name = terminal.get("state")
        if state_name == WORKFLOW_STATE_COMPLETED:
            return self._runtime.get_workflow_result(self.workflow_id)
        if state_name == WORKFLOW_STATE_CANCELLED:
            raise WorkflowCancelledError(
                f"flow {self.workflow_id!r} was cancelled: "
                f"{terminal.get('error') or 'workflow cancelled'}"
            )
        if _deadline_failure(terminal):
            raise _workflow_timeout_error(self.workflow_id)
        raise WorkflowFailedError(
            f"flow {self.workflow_id!r} failed: {terminal.get('error') or 'workflow failed'}"
        )


def current_workflow_id() -> str | None:
    """返回当前上下文活跃的 workflow_id（若在 ``@flow`` / ``@aio.flow`` 中）。"""
    state = _flow_state.get()
    return state.workflow_id if state is not None else None


def _current_flow_state() -> _FlowState | None:
    """返回当前 flow 上下文状态（若在 flow 中，否则 ``None``）。

    由 ``Task.submit`` / ``submit_batch`` 调用：非 ``None`` 表示本次提交
    走 orchestrator 驱动的编排路径；``None`` 保持独立 ``@task``
    直调的任务队列语义不变。
    """
    return _flow_state.get()


def _cancel_flow_tasks(workflow_id: str) -> None:
    """取消指定 workflow 下的所有本地任务（flow 失败/超时时调用）。"""
    from actant._internal.runtime import get_current_runtime

    runtime = get_current_runtime()
    if runtime is None:
        return
    for tid in list(runtime._list_tasks()):
        handle = runtime._get_task(tid)
        if handle is not None and handle.workflow_id == workflow_id:
            try:
                handle.cancel(propagate=False)
            except Exception:
                # 批量取消：单个任务取消失败不应阻止后续任务被取消，
                # 记录 warning 后继续（最终由 flow 失败异常向上传播）。
                _logger.warning(
                    "flow %s: failed to cancel task %s",
                    workflow_id,
                    tid,
                    exc_info=True,
                )


class _FlowContext:
    """``with`` 上下文：设置/恢复当前 context 的 flow 状态。

    ``ContextVar.reset(token)`` 必须与 ``set`` 在同一个 context 中执行。同步
    flow 的函数体在单线程内完成，天然满足；async flow 的每个 ``asyncio.Task``
    持有独立 context 副本，``with`` 块跨越 ``await`` 也不会串到其他协程。
    """

    def __init__(self, state: _FlowState) -> None:
        self._state = state
        self._token: Token[_FlowState | None] | None = None

    def __enter__(self) -> _FlowState:
        self._token = _flow_state.set(self._state)
        return self._state

    def __exit__(self, *exc: object) -> None:
        if self._token is not None:
            _flow_state.reset(self._token)
            self._token = None


def _ensure_workflow_created(runtime: Any, state: _FlowState) -> None:
    """惰性创建工作流外壳，并在**真正创建之后**广播生命周期事件。

    这是工作流外壳的**唯一**创建入口：``Task.submit`` 的编排路径与等待点路径
    （:func:`_wait_context`）都必须经此，否则会再次出现"两处各写一遍创建块"的
    漂移（守则：同一事实只有一个源）。

    时序上刻意把 ``submitted`` / ``started`` 放在 ``_submit_workflow`` **之后**：
    ``@flow`` 的 wrapper 曾在函数体开始前就广播这两个事件，而工作流那时还没
    建槽——等于把一个此刻不存在的 ``workflow_id`` 交给外部观察者，其立即调用
    ``Runtime.signal_wait_point`` 会撞 ``NotFound``（实测 1/5 复现）。事件与状态
    同源后，"看到 submitted ⇒ 工作流已存在"成立。
    """
    if state.workflow_created:
        return
    runtime._submit_workflow(
        state.workflow_id,
        failure_strategy=state.failure_strategy,
        timeout_ms=state.timeout_ms,
        name=state.flow_name or None,
        version=state.version,
        args=state.invocation_args,
    )
    # 版本裁决必须发生在 ``workflow_created`` 置位之前：不兼容的重放（历史
    # 版本高于当前代码）不得被当成"已创建"，否则调用方会继续在无法正确重放
    # 的工作流上提交节点。
    _adopt_workflow_version(runtime, state)
    state.workflow_created = True
    if state.announce:
        _safe_emit(state.workflow_id, "submitted")
        _safe_emit(state.workflow_id, "started")


def _adopt_workflow_version(runtime: Any, state: _FlowState) -> None:
    """记录/校验 flow 定义版本（工作流外壳创建时的唯一步骤）。

    版本与 ``ctx.*`` 走同一张确定性记录表：首次执行写入当前的
    ``state.version``，重放读回记录值。裁决规则：

    - 记录版本 == 当前版本：正常（相同代码重放）。
    - 记录版本 < 当前版本：允许。代码已升级，旧工作流按 ``patched()`` 记录的
      分支决策续跑，新分支默认不启用。
    - 记录版本 > 当前版本：抛 :class:`FlowReplayError`。当前代码比创建工作流的
      代码更旧，无法保证消费得了历史里已有的分支决策；静默重放会以错误语义
      推进工作流，故显式失败。
    """
    raw = cast(
        "bytes",
        runtime._resolve_workflow_context_value(
            state.workflow_id,
            _FLOW_VERSION_KEY,
            _FLOW_VERSION_KIND,
            struct.pack(">q", state.version),
        ),
    )
    if len(raw) != 8:
        raise FlowReplayError(
            f"flow {state.workflow_id!r}: recorded workflow version payload has "
            f"{len(raw)} bytes, expected 8"
        )
    recorded = struct.unpack(">q", raw)[0]
    if recorded > state.version:
        raise FlowReplayError(
            f"flow {state.workflow_id!r} was recorded with flow version "
            f"{recorded}, but this code declares version {state.version}; "
            "a workflow recorded by newer code cannot be replayed by older code"
        )
    if recorded < state.version:
        _logger.info(
            "flow %s: replaying workflow recorded at version %d with code "
            "version %d; recorded patched() decisions are preserved",
            state.workflow_id,
            recorded,
            state.version,
        )


def _wait_context() -> tuple[Any, _FlowState]:
    """返回当前 flow 的 ``(runtime, state)``，并确保工作流外壳已持久化。

    三条前置全部显式报错而非静默降级：

    - 不在 ``@flow`` 函数体内（线程局部无 state）→ :class:`InvalidStateError`；
    - 无活跃 Runtime → :class:`InvalidStateError`；
    - 工作流外壳尚未持久化 → 复用 ``Task.submit`` 的惰性创建路径。等待点必须
      落在已存在的工作流上，否则 Rust 侧 ``_register_wait_point`` 报 ``NotFound``
      （等待点随工作流快照落盘，无工作流即无宿主）。
    """
    state = _flow_state.get()
    if state is None:
        raise InvalidStateError(
            "sleep_until / wait_signal / suspend / patched / ctx.* 只能在 "
            "@flow 或 @aio.flow 函数体内调用（当前无 flow 上下文）"
        )
    runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            "no active runtime: sleep_until / wait_signal / suspend / patched / "
            "ctx.* 需要 Runtime 上下文"
        )
    _ensure_workflow_created(runtime, state)
    return runtime, state


class FlowContext:
    """flow 体的确定性上下文：wall-clock / UUID / 随机数。

    这三个值在重放时必须与首次执行一致，否则同一 ``workflow_id`` 的续跑会产生
    不同的提交序列。实现是"首次生成并写入工作流历史，重放命中历史返回记录值"，
    故值**跨进程重启存活**（随 orchestration 快照落盘）。

    只能在 ``@flow`` 函数体内使用；同序号调用了不同种类的方法（如首次是
    ``now_ms``、重放时是 ``uuid``）抛 :class:`FlowReplayError`——这是提交
    序列漂移的显式信号，不静默替换。

    用法::

        @actant.flow
        def pipeline():
            started = actant.workflow.flow_ctx.now_ms()
            run_id = actant.workflow.flow_ctx.uuid()
            jitter = actant.workflow.flow_ctx.random()
            ...
    """

    def _resolve(self, kind: str, candidate: bytes) -> bytes:
        runtime, state = _wait_context()
        key = f"ctx-{state.next_ctx_seq()}"
        return cast(
            "bytes",
            runtime._resolve_workflow_context_value(state.workflow_id, key, kind, candidate),
        )

    def now_ms(self) -> int:
        """返回 flow 内稳定的当前时刻（epoch 毫秒）。

        首次调用取 ``time.time()``；重放返回首次记录值。
        """
        raw = self._resolve("now_ms", struct.pack(">q", int(time.time() * 1000)))
        return cast("int", struct.unpack(">q", raw)[0])

    def uuid(self) -> str:
        """返回 flow 内稳定的 UUID 字符串（32 位十六进制，无连字符）。

        首次调用生成 ``uuid4().hex``；重放返回首次记录值。
        """
        raw = self._resolve("uuid", uuid.uuid4().hex.encode("utf-8"))
        return raw.decode("utf-8")

    def random(self) -> float:
        """返回 flow 内稳定的伪随机数（``[0.0, 1.0)``）。

        首次调用取 ``random.random()``；重放返回首次记录值。
        """
        raw = self._resolve("random", struct.pack(">d", random.random()))
        return cast("float", struct.unpack(">d", raw)[0])


#: 确定性上下文单例（``actant.workflow.flow_ctx``）。方法只能在 ``@flow`` 函数体内调用。
ctx = FlowContext()


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
            raise _workflow_timeout_error(workflow_id)
        raise WorkflowFailedError(
            f"flow {workflow_id!r} failed while parked (error={state.get('error')!r})"
        )
    if state_name == WORKFLOW_STATE_CANCELLED:
        raise WorkflowCancelledError(f"flow {workflow_id!r} was aborted (workflow cancelled)")


def _park_payload(runtime: Any, workflow_id: str, wait_key: str, *, bound_ms: int) -> bytes | None:
    """在已注册的等待点上 park，返回唤醒 payload（**唯一** park 入口）。

    ``bound_ms == 0`` 表示不设上界。返回 ``None`` 的唯一含义是"**真正的上界
    超时**"：若 park 是被**终态释放**唤醒的（``complete_terminal`` 释放了本工作流
    的 park 等待者），:func:`_raise_if_workflow_terminal` 会直接抛错而不返回。

    三个 park 调用点（``sleep_until`` / ``wait_signal`` / ``suspend``）都必须经
    本函数——``wait_signal`` 曾直接调 ``_wait_wait_point``，于是终态释放被它当成
    "信号返回了空 payload"，函数体在已死的工作流上继续跑到返回。单一入口是为了
    让这条判定不可能漂移。
    """
    payload = runtime._wait_wait_point(workflow_id, wait_key, timeout_ms=bound_ms)
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
    runtime._register_wait_point(state.workflow_id, wait_key, kind="timer", deadline_ms=deadline_ms)
    bound = timeout_ms or state.timeout_ms
    if not _park(runtime, state.workflow_id, wait_key, bound_ms=bound):
        raise ActantTimeoutError(
            f"flow {state.workflow_id}: sleep_until({deadline_ms}) 在 {bound}ms 内未被唤醒"
        )


def wait_signal(name: str, *, timeout_ms: int = 0) -> bytes | None:
    """在 flow 函数体内等待名为 ``name`` 的外部信号（Signals）。

    ``name`` 即等待点键：外部用
    ``Runtime.signal_wait_point(workflow_id, name, payload)`` 递交。等待点为
    **闭锁（latch）语义**——同一名字在工作流内只注册一次，重复 await 直接返回
    已收到的 payload，这是重放体天然幂等的前提（"已收到 → 直接返回"）。

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
        收到信号时返回递交方携带的 ``payload``（``bytes``；未携带时为空串）。
        信号是**闭锁**：同一 ``name`` 重复 await 返回**首次**递交的 payload，
        重放与跨重启恢复返回同一份字节。仅在有界等待（``timeout_ms > 0``）
        超时时返回 ``None``。

    Raises:
        InvalidStateError: 不在 flow 内 / 无 Runtime。
        ActantTimeoutError: 未显式指定上界，而 flow 自身 deadline 已到期。
            未请求的提前返回会破坏确定性契约，故报错而非静默返回。
    """
    runtime, state = _wait_context()
    runtime._register_wait_point(state.workflow_id, name, kind="signal", name=name)
    if timeout_ms > 0:
        return _park_payload(runtime, state.workflow_id, name, bound_ms=timeout_ms)
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
    runtime._register_wait_point(state.workflow_id, wait_key, kind=_SUSPEND_KIND)
    bound = timeout_ms or state.timeout_ms
    if not _park(runtime, state.workflow_id, wait_key, bound_ms=bound):
        raise ActantTimeoutError(f"flow {state.workflow_id}: suspend() 在 {bound}ms 内未被恢复")


def patched(change_id: str, *, enabled: bool = True) -> bool:
    """声明一个向后兼容的代码分支，并返回该工作流**实际采用**的决策。

    用于在不打断在途工作流的前提下修改 flow 体：把新逻辑包进
    ``if patched("change-id"):``，已存在的旧工作流仍走旧分支，新工作流走新
    分支。

    决策是**首次执行时写入工作流历史**的确定性值（按 ``change_id`` 寻址，
    随 orchestration 快照跨重启存活）：

    - 第一次执行到本调用：记录 ``enabled`` 并返回它；
    - 之后的重放（含代码升级后）：返回**记录值**，忽略 ``enabled``；
    - 同一次执行内用同一 ``change_id`` 传入不同 ``enabled``：抛
      :class:`FlowReplayError`（分支决策自相矛盾）。

    ``change_id`` 一经使用不得复用给另一个分支——历史里已记录的决策会被
    后者继承，等于让新分支错误地沿用旧决策。

    Args:
        change_id: 分支标识，在工作流内唯一且语义稳定（如 ``"new-router-v2"``）。
        enabled: 首次执行时采用的默认决策（``True`` 表示新分支）。

    Returns:
        本工作流记录的决策（``True`` = 新分支）。

    Raises:
        InvalidStateError: 不在 flow 内 / 无 Runtime。
        FlowReplayError: 同一次执行内 ``change_id`` 的默认值前后矛盾。
        ValueError: ``change_id`` 为空。
    """
    if not change_id:
        raise ValueError("patched: change_id must not be empty")
    runtime, state = _wait_context()
    seen = state.decisions.get(change_id)
    if seen is not None and seen != enabled:
        raise FlowReplayError(
            f"flow {state.workflow_id!r}: patched({change_id!r}) was called with "
            f"enabled={enabled} after enabled={seen}; a change id must carry a "
            "single decision within one workflow run"
        )
    state.decisions[change_id] = enabled
    raw = cast(
        "bytes",
        runtime._resolve_workflow_context_value(
            state.workflow_id,
            f"{_PATCH_KEY_PREFIX}{change_id}",
            _PATCH_KIND,
            b"\x01" if enabled else b"\x00",
        ),
    )
    if raw not in (b"\x00", b"\x01"):
        raise FlowReplayError(
            f"flow {state.workflow_id!r}: recorded decision for patched("
            f"{change_id!r}) is not a single boolean byte (got {raw!r})"
        )
    return raw == b"\x01"


class _ContinueAsNew(BaseException):
    """内部控制流：``continue_as_new`` 要求以新工作流续跑。

    刻意继承 ``BaseException``：续跑不是业务失败，函数体里的
    ``except Exception`` 不应吞掉它而继续提交（那会把两代工作流的提交序列
    混在一起）。包装器在 ``except BaseException`` 之前单独捕获它。
    """

    def __init__(
        self,
        workflow_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        super().__init__(workflow_id)
        self.workflow_id = workflow_id
        self.args = args
        self.kwargs = kwargs


def continue_as_new(*args: Any, **kwargs: Any) -> NoReturn:
    """以新一代工作流续跑当前 flow，并重置编排状态。

    长生命周期工作流（持续追加节点的循环 / 每分钟触发一次的守护流程）会让
    DAG、执行表与事件历史无界增长，重放成本随之线性上升。``continue_as_new``
    在当前代**封口后**用新工作流 id 重新执行同一函数体，把历史与 DAG 重置为
    只有新一代的状态。

    语义与 Prefect/Temporal 的 continue-as-new 一致：函数体从**开头**重新执行，
    业务进度必须通过 ``*args``/``**kwargs`` 显式传给下一代。新工作流的外壳与
    携带参数先落盘，之后才开始执行——进程若在两者之间崩溃，
    :func:`host_flows` 会用落盘的入参恢复新一代，不会退回到原始入参。

    调用点之后本代不再提交任何节点（函数体立即中止）。

    Args:
        *args: 传给下一代工作流的全部位置参数。
        **kwargs: 传给下一代工作流的全部关键字参数。

    Raises:
        InvalidStateError: 不在 flow 内 / 无 Runtime / flow 名不可用。

    Note:
        新旧两代是**独立工作流**：``list_workflows()`` 会同时看到两者，调用方
        若按 ``workflow_id`` 观测请以最后一代为准。本代一旦封口即按正常流程
        到达终态，不会标记为特殊状态。
    """
    runtime, state = _wait_context()
    if not state.flow_name:
        raise InvalidStateError(
            f"flow {state.workflow_id!r}: continue_as_new 需要 flow 名，当前执行缺少该上下文"
        )
    new_id = f"{state.flow_name}-{uuid.uuid4().hex[:8]}"
    # 先在旧工作流上记录"下一代 id"：重放旧代时返回记录值，故续跑只发生
    # 一次（候选 id 只在首次执行时生效）。
    recorded_to = runtime._resolve_workflow_context_value(
        state.workflow_id,
        _CONTINUED_TO_KEY,
        _CONTINUED_TO_KIND,
        new_id.encode(),
    )
    new_id = bytes(recorded_to).decode()
    runtime._submit_workflow(
        new_id,
        failure_strategy=state.failure_strategy,
        timeout_ms=state.timeout_ms,
        name=state.flow_name,
        version=state.version,
        args=cloudpickle.dumps((args, kwargs)),
    )
    carry = cloudpickle.dumps((args, kwargs))
    # 携带参数同样幂等：重放旧代时 resolve 返回首次写入的入参，即使调用点
    # 传入不同值也不会改写新代的事实。进程内续跑也使用解析后的记录值，
    # 与"崩溃后按落盘入参恢复"保持同一条事实源。
    resolved_carry = runtime._resolve_workflow_context_value(
        new_id,
        _CONTINUE_CARRY_KEY,
        _CONTINUE_CARRY_KIND,
        carry,
    )
    carried_args, carried_kwargs = cloudpickle.loads(bytes(resolved_carry))
    raise _ContinueAsNew(new_id, carried_args, carried_kwargs)


def start_flow(
    name: str,
    *args: Any,
    runtime: Any = None,
    version: int | None = None,
    timeout_ms: int | None = None,
    failure_strategy: str | None = None,
    **kwargs: Any,
) -> FlowHandle:
    """登记一次 flow 调用并返回句柄，**不在调用方执行函数体**。

    与直接调用 ``@flow`` 函数的区别：直接调用是"提交 + 本进程立即驱动"的
    嵌入式语法糖；本函数只把工作流外壳与 invocation（flow 名、版本、入参）
    落盘，函数体留给 :func:`host_flows` 在任意进程执行。因此提交方可以在
    返回后立即退出，工作流生命周期与客户端进程解耦。

    当前进程**未导入** flow 定义时，可显式传入 ``version``（及可选的
    ``timeout_ms`` / ``failure_strategy``）完成提交：提交方只需持有定义
    标识，函数体与其余执行参数由宿主进程的代码解析。定义已登记时，显式
    参数必须与登记值一致——不一致视为定义漂移，显式报错而不静默采用任一
    一方。

    Args:
        name: flow 名（``@flow(name=...)`` 的值）。
        *args: 传给 flow 函数体的位置参数，随 invocation 持久化。
        runtime: 目标 Runtime；``None`` 时取当前上下文。
        version: flow 定义版本。当前进程无登记定义时**必填**；有登记定义时
            可省略，若提供则须与登记版本一致。
        timeout_ms: 工作流级 deadline 毫秒。无登记定义时作为提交侧声明；
            有登记定义时若提供则须与登记值一致。
        failure_strategy: ``"fail_fast"`` / ``"continue"``。语义同
            ``timeout_ms``。
        **kwargs: 传给 flow 函数体的关键字参数。

    Returns:
        :class:`FlowHandle`；跨进程可仅凭 ``workflow_id`` 重建读取。

    Raises:
        InvalidStateError: 无可用 Runtime。
        NotFoundError: 当前进程未登记该 flow 名，且未提供 ``version``。
        ValueError: 显式参数与已登记定义不一致，或 ``failure_strategy`` 非法。
    """
    if runtime is None:
        runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            f"start_flow({name!r}): no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    definition = FLOW_REGISTRY.get(name)
    if definition is None:
        if version is None:
            raise NotFoundError(
                f"start_flow({name!r}): no flow definition registered in this "
                f"process and no explicit version given; known flows: "
                f"{FLOW_REGISTRY.names()}"
            )
        if failure_strategy is not None and failure_strategy not in _VALID_FAILURE_STRATEGIES:
            raise ValueError(
                f"start_flow({name!r}): failure_strategy must be one of "
                f"{_VALID_FAILURE_STRATEGIES}, got {failure_strategy!r}"
            )
        flow_version = version
        flow_timeout = 0 if timeout_ms is None else timeout_ms
        flow_strategy = failure_strategy
    else:
        _check_definition_match(name, definition, version, timeout_ms, failure_strategy)
        flow_version = definition.version
        flow_timeout = definition.timeout_ms
        flow_strategy = definition.failure_strategy
    workflow_id = f"{name}-{uuid.uuid4().hex[:8]}"
    state = _FlowState(
        workflow_id,
        flow_name=name,
        failure_strategy=flow_strategy,
        timeout_ms=flow_timeout,
        version=flow_version,
        invocation_args=cloudpickle.dumps((args, kwargs)),
    )
    # detached 路径必须**先落盘再返回**：调用方可能在 host 启动前退出。
    _ensure_workflow_created(runtime, state)
    return FlowHandle(runtime, workflow_id, name)


def _check_definition_match(
    name: str,
    definition: FlowDefinition,
    version: int | None,
    timeout_ms: int | None,
    failure_strategy: str | None,
) -> None:
    """校验提交侧显式参数与进程内登记定义是否一致。

    显式参数只为"客户端未加载定义"的提交服务；定义已加载时它们是冗余声明，
    与登记值不符意味着提交方与宿主代码对同一 flow 的认知已漂移，故显式失败。
    """
    if version is not None and version != definition.version:
        raise ValueError(
            f"start_flow({name!r}): explicit version {version} does not match "
            f"registered definition version {definition.version}"
        )
    if timeout_ms is not None and timeout_ms != definition.timeout_ms:
        raise ValueError(
            f"start_flow({name!r}): explicit timeout_ms {timeout_ms} does not "
            f"match registered definition timeout_ms {definition.timeout_ms}"
        )
    if failure_strategy is not None and failure_strategy != definition.failure_strategy:
        raise ValueError(
            f"start_flow({name!r}): explicit failure_strategy "
            f"{failure_strategy!r} does not match registered definition "
            f"failure_strategy {definition.failure_strategy!r}"
        )


def _flow_continuation_ids(runtime: Any, active: list[str]) -> set[str]:
    """返回由某个前驱 ``continue_as_new`` 引入的 workflow_id 集合。

    旧代未封口而新代已落盘时，旧代的重放会在**同一线程内**续跑新代。闭包内的
    代只能由根代的重放链导入，不能作为独立起点重放（否则同一代被驱动两遍）。
    按图结构计算而非依赖 ``list_workflows()`` 的哈希表顺序。
    """
    successor_of: dict[str, str] = {}
    for candidate in active:
        successor = runtime._get_workflow_context_value(candidate, _CONTINUED_TO_KEY)
        if successor is not None:
            successor_of[candidate] = bytes(successor).decode()
    covered: set[str] = set()
    for candidate in active:
        successor = successor_of.get(candidate)
        while successor is not None and successor not in covered:
            covered.add(successor)
            successor = successor_of.get(successor)
    return covered


#: 同一进程内只允许一个 host loop，避免两个线程重复驱动同一工作流。
#: 跨进程的排他性由 ``data_dir`` 的独占打开保证；跨节点 ownership 由
#: FailoverManager 的租约承担（见 P2 的 role 拓扑）。
_HOST_LOCK = threading.Lock()


def host_flows(
    *,
    runtime: Any = None,
    once: bool = True,
    timeout_ms: int = 0,
    driver: FlowDriver | None = None,
) -> list[str]:
    """驱动本 ``data_dir`` 中尚未完成、且带 invocation 的 flow。

    这是分离式执行的核心：提交进程只需 :func:`start_flow`；任意持有相同
    ``data_dir`` 与 flow 定义的进程调用本函数即可重放并完成任务。

    定义与入参都来自 journal：``Submitted.invocation`` 给出 flow 名与
    ``cloudpickle`` 入参，进程内 :data:`FLOW_REGISTRY` 给出函数体与执行参数。
    缺失定义或 invocation 时记录 warning 并跳过（不静默——否则"重启后什么都
    没发生"是最难排查的失败形态）。

    Args:
        runtime: 目标 Runtime；``None`` 时取当前上下文。
        once: ``True`` 扫描一轮后返回；``False`` 持续轮询新工作流（host 服务
            形态），仅在被中断时退出。
        timeout_ms: 重放体的工作流级 deadline（仅在需要新建外壳时生效）。
        driver: 重放执行策略；``None`` 时使用同步的
            :class:`~actant.driver.BlockingDriver`。使用
            :class:`~actant.driver.ThreadedDriver` 可让多个 flow
            并发执行；``@aio.flow`` 应使用
            :class:`~actant.driver.AsyncDriver`——单个 event loop 承载
            万级 parked 协程。异步驱动下 ``once=True`` 只保证重放已派发，
            调用方须在停止 Runtime 前调用 ``driver.join()``。

    Returns:
        实际发起重放的 ``workflow_id`` 列表（去重）。``once=False`` 时是本次
        调用期间驱动过的全部 id。

    Raises:
        InvalidStateError: 无可用 Runtime，或同进程已有 host loop 在运行。
    """
    if runtime is None:
        runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            "host_flows: no active Runtime; wrap your code in `with actant.Runtime() as rt:`"
        )
    if str(getattr(runtime, "role", "both")) == "worker":
        _logger.warning(
            "host_flows: runtime role is 'worker'; flow hosting is disabled on worker-only nodes"
        )
        return []
    if not _HOST_LOCK.acquire(blocking=False):
        raise InvalidStateError("host_flows: another host loop is already running in this process")
    active_driver: FlowDriver = driver if driver is not None else BlockingDriver()
    if isinstance(active_driver, BlockingDriver):
        async_names = [
            name
            for name in FLOW_REGISTRY.names()
            if (definition := FLOW_REGISTRY.get(name)) is not None and definition.is_async
        ]
        if async_names:
            _logger.warning(
                "host_flows: async flow definition(s) %s are hosted by BlockingDriver; "
                "pass driver=AsyncDriver() to run them concurrently on one event loop",
                ", ".join(async_names[:3]) + ("..." if len(async_names) > 3 else ""),
            )
    driven: list[str] = []
    try:
        while True:
            progressed = _host_once(runtime, driven, timeout_ms=timeout_ms, driver=active_driver)
            if once:
                return driven
            if not progressed:
                # 没有可驱动的工作流：休眠后继续等待新提交，避免忙等。
                time.sleep(_HOST_IDLE_POLL_INTERVAL_S)
    finally:
        _HOST_LOCK.release()


def _host_once(
    runtime: Any,
    driven: list[str],
    *,
    timeout_ms: int,
    driver: FlowDriver,
) -> bool:
    """扫描一轮并驱动；返回本轮是否驱动了至少一个工作流。"""
    active = runtime.list_workflows()
    covered = _flow_continuation_ids(runtime, active)
    progressed = False
    for workflow_id in active:
        if workflow_id in covered:
            _logger.info(
                "host_flows: workflow %s is replayed by its predecessor's "
                "continue_as_new; skipping",
                workflow_id,
            )
            continue
        invocation = runtime.get_workflow_invocation(workflow_id)
        if invocation is None:
            _logger.warning(
                "host_flows: workflow %s has no invocation recorded; "
                "cannot recover its flow name and args; skipping",
                workflow_id,
            )
            continue
        name = str(invocation["name"])
        definition = FLOW_REGISTRY.get(name)
        if definition is None:
            _logger.warning(
                "host_flows: no flow definition registered for %r (workflow %s); skipping",
                name,
                workflow_id,
            )
            continue
        try:
            replay_args, replay_kwargs = cloudpickle.loads(invocation["args"])
        except Exception:
            _logger.warning(
                "host_flows: workflow %s invocation args are not decodable; skipping",
                workflow_id,
                exc_info=True,
            )
            continue
        # continue-as-new 的新一代：携带参数随工作流入历史，与 invocation
        # 指向同一事实；carry 存在时以它为准（旧记录可能没有 invocation）。
        carry = runtime._get_workflow_context_value(workflow_id, _CONTINUE_CARRY_KEY)
        if carry is not None:
            replay_args, replay_kwargs = cloudpickle.loads(carry)
        _logger.info("host_flows: replaying workflow %s (flow %r)", workflow_id, name)
        # 闭包的"形状"由定义决定：async flow 的闭包是协程函数，驱动据
        # ``inspect.iscoroutinefunction`` 选择 event loop 或执行器。惰性导入
        # ``flow_async`` 打破 flow ↔ flow_async 的循环依赖。
        if definition.is_async:
            from actant._internal.flow_async import _replay_async_in_context

            replay: Callable[[], Any] = partial(
                _replay_async_in_context,
                runtime,
                definition.func,
                workflow_id,
                tuple(replay_args),
                dict(replay_kwargs),
                timeout_ms=timeout_ms or definition.timeout_ms,
                failure_strategy=definition.failure_strategy,
                version=definition.version,
                flow_name=name,
            )
        else:
            replay = partial(
                _replay_in_context,
                runtime,
                definition.func,
                workflow_id,
                tuple(replay_args),
                dict(replay_kwargs),
                timeout_ms=timeout_ms or definition.timeout_ms,
                failure_strategy=definition.failure_strategy,
                version=definition.version,
                flow_name=name,
            )
        try:
            scheduled = driver.spawn(workflow_id, replay)
        except ActantTimeoutError:
            # 单个工作流的 deadline 到期：它已被 orchestrator 置为终态，
            # 不是本轮失败。继续驱动其余工作流。
            _logger.warning(
                "host_flows: workflow %s exceeded its deadline during replay; skipping",
                workflow_id,
            )
            continue
        if not scheduled:
            # 该工作流已在执行中（异步驱动的重复扫描），不重复计入本轮。
            continue
        if workflow_id not in driven:
            driven.append(workflow_id)
        progressed = True
    return progressed


def _replay_in_context(
    runtime: Any,
    func: Callable[..., Any],
    workflow_id: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    timeout_ms: int,
    failure_strategy: str | None,
    version: int,
    flow_name: str,
) -> None:
    """在目标 Runtime 上下文中执行一次重放。

    驱动实现可能在其他线程执行该闭包，故 Runtime 与全部参数都在
    ``functools.partial`` 中绑定；``use_runtime`` 保证 flow 体内的
    ``Task.submit`` / 事件广播能解析到同一个 Runtime。
    """
    with use_runtime(runtime):
        _replay_flow(
            func,
            workflow_id,
            args,
            kwargs,
            runtime=runtime,
            timeout_ms=timeout_ms,
            failure_strategy=failure_strategy,
            version=version,
            flow_name=flow_name,
            announce=False,
        )


def flow(
    func: Callable[P, R] | None = None,
    *,
    name: str | None = None,
    timeout_ms: int = 0,
    failure_strategy: str | None = None,
    version: int = 0,
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
        version: flow 定义版本（非负整数，默认 0）。首次执行时随工作流持久化，
            重放时与代码声明的版本比对：历史版本更高（回滚代码）抛
            :class:`FlowReplayError`；历史版本更低（升级代码）允许续跑，
            分支决策由 :func:`patched` 的历史记录决定。修改 flow 体时**递增**
            本值并在新分支处使用 :func:`patched`，即可保证在途工作流不被破坏。

    Raises:
        InvalidStateError: 无活跃 Runtime。
        ActantTimeoutError: 工作流 deadline 到期且函数体因此被中断。
        FlowReplayError: 重放的工作流记录版本高于当前代码版本。
        ValueError: ``failure_strategy`` 非法（仅接受 ``"fail_fast"`` /
            ``"continue"`` / ``None``），或 ``version`` 为负。
    """

    def _make(f: Callable[P, R]) -> Callable[P, R]:
        if version < 0:
            raise ValueError(f"flow: version must be >= 0, got {version}")
        if failure_strategy is not None and failure_strategy not in _VALID_FAILURE_STRATEGIES:
            raise ValueError(
                f"flow: failure_strategy must be one of "
                f"{_VALID_FAILURE_STRATEGIES} or None, got {failure_strategy!r}"
            )
        flow_name = name or f.__qualname__
        # 导入时登记定义：host loop 只凭 journal 中的 flow 名即可找回函数体。
        # 调用参数不在登记里——它们随 invocation 持久化，与进程无关。
        FLOW_REGISTRY.register(
            FlowDefinition(
                name=flow_name,
                func=f,
                version=version,
                timeout_ms=timeout_ms,
                failure_strategy=failure_strategy,
            )
        )

        @wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            runtime = get_current_runtime()
            if runtime is None:
                raise InvalidStateError(
                    f"flow {flow_name!r}: no active Runtime; "
                    "wrap your code in `with actant.Runtime() as rt:`"
                )
            # 直接调用 = 嵌入式执行：创建（惰性）工作流 + 在本进程立即驱动。
            # 分离式执行请用 `start_flow` / `host_flows`。
            return _replay_flow(
                f,
                f"{flow_name}-{uuid.uuid4().hex[:8]}",
                args,
                dict(kwargs),
                runtime=runtime,
                timeout_ms=timeout_ms,
                failure_strategy=failure_strategy,
                version=version,
                flow_name=flow_name,
                announce=True,
            )

        # 标记：host loop 与工具可从包装器反查 flow 名/版本。
        setattr(wrapper, "__actant_flow_name__", flow_name)  # noqa: B010
        setattr(wrapper, "__actant_flow_version__", version)  # noqa: B010
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


def _workflow_timeout_error(workflow_id: str) -> ActantTimeoutError:
    """构造「flow 超过工作流 deadline」的异常（错误消息单点生成）。"""
    return ActantTimeoutError(
        f"flow {workflow_id!r} exceeded its workflow deadline (error={_WORKFLOW_TIMEOUT_ERROR!r})"
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
        if state.workflow_created or state.existing:
            runtime.cancel_workflow(workflow_id)
    except Exception as cancel_exc:
        # 工作流可能已终态（fail-fast 已触发）：取消是兜底动作，失败不应掩盖
        # 原始异常，且日志应记录真正的**取消失败**原因（cancel_exc），而不是误记
        # 函数体原始异常（exc）。
        _logger.debug(
            "flow %s: cancel_workflow skipped (%s)",
            workflow_id,
            type(cancel_exc).__name__,
        )
    _cancel_flow_tasks(workflow_id)
    _safe_emit(workflow_id, "failed", error=f"{type(exc).__name__}: {exc}")


def _wait_terminal(
    runtime: Any,
    workflow_id: str,
    *,
    timeout_ms: int = 0,
) -> dict[str, Any]:
    """轮询工作流直到终态，返回终态快照；**不广播**任何生命周期事件。

    与 :func:`_wait_terminal_and_emit` 的分工：本函数是纯读取路径，供
    :meth:`FlowHandle.result` 在提交进程之外使用——终态事件已由驱动方或原始
    执行广播，消费者不能重复广播。

    ``timeout_ms`` 是本地等待上界：到期抛 :class:`ActantTimeoutError`，但**不**
    取消工作流（取消属于驱动方职责）。
    """
    deadline = 0.0 if timeout_ms <= 0 else time.monotonic() + timeout_ms / 1000.0
    while True:
        state = cast("dict[str, Any] | None", runtime.get_workflow_state(workflow_id))
        if state is None:
            raise NotFoundError(f"flow {workflow_id!r} not found")
        state_name = state.get("state")
        if state_name in (
            WORKFLOW_STATE_COMPLETED,
            WORKFLOW_STATE_FAILED,
            WORKFLOW_STATE_CANCELLED,
        ):
            return state
        if deadline and time.monotonic() >= deadline:
            raise ActantTimeoutError(
                f"flow {workflow_id!r} did not reach a terminal state within {timeout_ms}ms"
            )
        time.sleep(_FLOW_TERMINAL_POLL_INTERVAL_S)


def _terminal_kind(state: dict[str, Any] | None) -> str:
    """把工作流快照归类为终态种类（同步与异步等待路径共用的唯一判定）。

    返回 ``"missing"`` / ``"completed"`` / ``"failed"`` / ``"cancelled"`` /
    ``"pending"``。两条等待路径只在"如何让出执行权"上不同（``time.sleep`` vs
    ``await asyncio.sleep``），终态判定必须同源，否则完成/失败事件时序会漂移。
    """
    if state is None:
        return "missing"
    state_name = state.get("state")
    if state_name == WORKFLOW_STATE_COMPLETED:
        return "completed"
    if state_name == WORKFLOW_STATE_FAILED:
        return "failed"
    if state_name == WORKFLOW_STATE_CANCELLED:
        return "cancelled"
    return "pending"


def _wait_terminal_and_emit(
    runtime: Any,
    workflow_id: str,
    *,
    on_completed: Callable[[], None] | None = None,
) -> dict[str, Any] | None:
    """等待工作流到达终态，广播生命周期事件，并返回该终态快照。

    轮询 ``get_workflow_state``（终态由最后一个任务完成触发）。
    工作流不存在（空 flow 未创建 / 已被淘汰）时直接广播 ``completed`` 并返回
    ``None``（空 flow 无编排数据，语义上立即完成）。

    ``on_completed`` 在**广播 ``completed`` 之前**调用：事件处理器读到
    ``completed`` 时应能立即查询到 workflow 返回值，否则事件与可查询状态
    之间存在确定性窗口。

    返回终态快照供调用方做终局裁决（``failed`` 且 error 为 deadline 到期
    时，调用方须抛 ``ActantTimeoutError`` 而非返回结果）。

    Python 侧不设等待上界：工作流带
    deadline 时，到期由 orchestrator 超时 watcher 把工作流置为终态，本循环
    因此必然退出；不带 deadline 的工作流本就允许任意时长运行。
    """
    while True:
        state = cast("dict[str, Any] | None", runtime.get_workflow_state(workflow_id))
        kind = _terminal_kind(state)
        if kind in ("missing", "completed"):
            if on_completed is not None:
                on_completed()
            _safe_emit(workflow_id, "completed")
            return state
        if kind == "failed":
            _safe_emit(workflow_id, "failed", error=(state or {}).get("error") or "")
            return state
        if kind == "cancelled":
            _safe_emit(
                workflow_id,
                "failed",
                error=(state or {}).get("error") or "workflow cancelled",
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
    version: int = 0,
    flow_name: str = "",
    announce: bool = False,
) -> R:
    """续跑入口：以既有 ``workflow_id`` 重放 flow 函数体。

    第 n 次 ``submit`` 查工作流历史：节点已完成 → 返回记录结果（不重跑）；
    进行中 → 返回绑定句柄；不存在 → 新提交。指纹不一致抛 ``FlowReplayError``。

    ``announce`` 控制惰性创建时是否广播 ``submitted``/``started``：
    ``False``（重放 / host loop）表示原始执行已广播过；``True`` 仅由 ``@flow``
    直接调用（首次执行）使用。

    ``timeout_ms`` 是**工作流级 deadline**，只在重放体惰性创建/更新工作流
    时生效；已在历史中的工作流沿用其持久化 deadline。

    ``version`` 是当前代码声明的 flow 定义版本：惰性创建路径会与工作流历史中
    记录的版本比对，历史版本更高时抛 :class:`FlowReplayError`。

    ``flow_name`` 供 ``continue_as_new`` 派生下一代工作流 id；为空时续跑会
    显式报错而不是生成一个无法被恢复登记的 id。

    重放由 :func:`host_flows` 显式触发，不在启动路径自动执行。
    """
    if runtime is None:
        runtime = get_current_runtime()
    if runtime is None:
        raise InvalidStateError(
            f"flow replay {workflow_id!r}: no active Runtime; "
            "wrap your code in `with actant.Runtime() as rt:`"
        )
    run_args = args
    run_kwargs = dict(kwargs)
    # 与 `@flow` 入口同构：continue_as_new 在恢复路径同样拆成多代顺序重放。
    while True:
        state = _FlowState(
            workflow_id,
            flow_name=flow_name,
            failure_strategy=failure_strategy,
            timeout_ms=timeout_ms,
            version=version,
            invocation_args=cloudpickle.dumps((run_args, run_kwargs)),
            # 重放体不得重复广播 submitted/started（原始执行已广播）。它走的是同一条
            # 惰性创建路径，故靠该标志关闭广播。
            announce=announce,
            # host loop 的起点是已落盘的工作流；函数体在首次 submit 前抛错时
            # 仍需能取消/封口它，避免下一轮 host 无限重试同一个毒工作流。
            existing=not announce,
        )
        try:
            result = _run_body(func, run_args, run_kwargs, state)
        except _ContinueAsNew as signal:
            if state.workflow_created or state.existing:
                runtime._seal_workflow(workflow_id)
            terminal = _wait_terminal_and_emit(runtime, workflow_id)
            if _deadline_failure(terminal):
                raise _workflow_timeout_error(workflow_id) from signal
            workflow_id = signal.workflow_id
            run_args = signal.args
            run_kwargs = signal.kwargs
            continue
        except BaseException as exc:
            # 与 `@flow` 入口同构：deadline 强还原导致的失败对调用方呈现为超时。
            if _workflow_deadline_expired(runtime, state):
                timeout_exc = _workflow_timeout_error(workflow_id)
                _settle_flow_failure(runtime, state, timeout_exc)
                raise timeout_exc from exc
            _settle_flow_failure(runtime, state, exc)
            raise
        # 重放命中既存节点时工作流外壳已在历史中，无需创建；封口同样必须执行
        # （重放体的提交序列在函数体返回后才允许终态判定）。
        if state.workflow_created or state.existing:
            runtime._seal_workflow(workflow_id)
        terminal = _wait_terminal_and_emit(
            runtime,
            workflow_id,
            on_completed=partial(runtime._persist_flow_result, workflow_id, result),
        )
        if _deadline_failure(terminal):
            raise _workflow_timeout_error(workflow_id)
        return result


def _replay_node_outcome(runtime: Any, handle: Any, outcome: dict[str, Any]) -> None:
    """按重放命中的历史状态重建句柄终态（供 ``Task._flow_submit`` 调用）。

    ``outcome`` 为 ``add_workflow_node`` 返回的 ``created=False`` 结果：
    ``Completed`` → 经 ``Runtime._on_task_result`` 复用结果解析链（含大结果
    Ref 降级），``Failed``/``Cancelled`` → 对应失败/取消终态；
    ``Pending``/``Running`` → 句柄保持等待（事件到达时解析）。
    """
    state_name = outcome.get("state")
    task_id = handle.task_id
    if state_name == "Completed":
        runtime._on_task_result(
            SimpleNamespace(
                task_id=task_id,
                state="Completed",
                result=outcome.get("result"),
                error=None,
            )
        )
    elif state_name == "Failed":
        runtime._on_task_result(
            SimpleNamespace(
                task_id=task_id,
                state="Failed",
                result=None,
                error=outcome.get("error") or "task failed (recorded in workflow history)",
            )
        )
    elif state_name == "Cancelled":
        runtime._on_task_result(
            SimpleNamespace(task_id=task_id, state="Cancelled", result=None, error=None)
        )
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
            "flow %r: WorkflowLifecycle emit (%s) failed",
            workflow_id,
            kind,
            exc_info=True,
        )
        if on_error == "collect":
            return e
    return None


__all__ = [
    "FLOW_REGISTRY",
    "FlowDefinition",
    "FlowHandle",
    "continue_as_new",
    "current_workflow_id",
    "flow",
    "host_flows",
    "patched",
    "sleep_until",
    "start_flow",
    "suspend",
    "wait_signal",
]
