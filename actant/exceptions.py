"""Actant 异常类层次。

与 Rust 侧 ActantError 枚举一一对应，确保跨层错误类型不丢失。
"""


class ActantError(RuntimeError):
    """Actant 基础异常。"""

    def __init__(self, message: str, *, kind: str = "internal") -> None:
        super().__init__(message)
        self.kind = kind


class _HintedActantError(ActantError):
    """持有 `kind` 与可诊断 `hint` 的 ActantError 子类模板。

    子类只需声明 `kind` 与 `hint` 两个类属性，构造时自动拼接 hint 后缀，
    避免大量子类重复同一份 __init__ 样板。`hint` 为空则与普通 ActantError
    一致（如 `FlowReplayError`）。
    """

    kind: str = "internal"
    hint: str = ""

    def __init__(self, message: str) -> None:
        suffix = f" {self.hint}" if self.hint else ""
        super().__init__(message + suffix, kind=self.kind)


class StorageError(_HintedActantError):
    """存储层错误。"""

    kind = "storage"
    hint = (
        " Check data_dir permissions, ensure only one process accesses the same LMDB path"
        " (LMDB uses process-level locking — multiple processes on the same data_dir will fail),"
        " and verify disk space."
    )


class NetworkError(_HintedActantError):
    """网络层错误。"""

    kind = "network"
    hint = (
        " Ensure all nodes are reachable, ports are open, and bootstrap addresses are correct."
    )


class SerializationError(_HintedActantError):
    """序列化/反序列化错误。"""

    kind = "serialization"
    hint = " Ensure task arguments and return values are picklable."


class ActorError(_HintedActantError):
    """Actor 系统错误。"""

    kind = "actor"
    hint = " Check actor mailbox capacity, message serialization, and that the target actor is still alive."


class WorkflowError(_HintedActantError):
    """Workflow 编排错误。"""

    kind = "workflow"
    hint = " Verify DAG structure (no cycles, all task_ids referenced in edges exist), and that task payloads are valid."


class TaskError(_HintedActantError):
    """任务执行错误。"""

    kind = "task"
    hint = " Check task function implementation, argument types, and that all dependencies are importable in the worker process."


class WorkerError(_HintedActantError):
    """Worker 运行时错误。"""

    kind = "worker"
    hint = " Check worker logs, max_concurrent_tasks capacity, and that the Runtime is not in drain mode."


class ConfigError(_HintedActantError):
    """配置错误。"""

    kind = "config"
    hint = " Review ActantConfig fields — failover params must satisfy heartbeat < failure_timeout < lease_duration, and data_dir must be writable."


class MetricsError(_HintedActantError):
    """指标管道错误（初始化或采集失败）。"""

    kind = "metrics"
    hint = " Check that the metrics port is not already in use and prometheus_client is installed."


class NotFoundError(_HintedActantError):
    """资源未找到。"""

    kind = "not_found"
    hint = " Verify the resource ID and that it hasn't been garbage-collected."


class AlreadyExistsError(_HintedActantError):
    """资源已存在。"""

    kind = "already_exists"
    hint = " Use a different name or ID."


class ActantTimeoutError(_HintedActantError):
    """操作超时。"""

    kind = "timeout"
    hint = " Consider increasing the timeout, checking worker availability, or verifying network connectivity."


class TaskCancelledError(_HintedActantError):
    """操作被取消。"""

    kind = "cancelled"
    hint = " The task was cancelled via Runtime.cancel_task() or a parent flow was cancelled. Use AsyncResult.state to check cancellation status."


class InvalidStateError(_HintedActantError):
    """无效状态操作（如在 drain 模式下提交任务）。"""

    kind = "invalid_state"
    hint = " Ensure Runtime.start() has been called and the Runtime is not stopped/draining. Use 'with Runtime(...) as rt:' to manage lifecycle."


class InternalError(_HintedActantError):
    """内部错误。"""

    kind = "internal"
    hint = " This is likely a bug in Actant — please report it with the full stack trace and reproduction steps."


class PayloadTooLargeError(ActantError):
    """序列化载荷超过网络消息大小上限。"""

    def __init__(self, actual: int, limit: int) -> None:
        self.actual = actual
        self.limit = limit
        hint = (
            " Reduce task argument/return value size, avoid passing large objects (use references or external storage),"
            " or increase the message size limit in ActantConfig."
        )
        super().__init__(
            f"serialized payload size {actual} bytes exceeds limit {limit} bytes. {hint}",
            kind="payload_too_large",
        )


# Workflow 状态异常（面向用户的高层异常）
class WorkflowFailedError(ActantError):
    """Workflow 执行失败。"""

    def __init__(
        self, message: str, *, task_name: str | None = None, task_error: str | None = None
    ) -> None:
        self.task_name = task_name
        self.task_error = task_error
        hint = " Inspect task_name/task_error attributes for the failing task. Consider adding retries or adjusting failure_strategy in the DAG."
        super().__init__(message + " " + hint, kind="workflow_failed")


class WorkflowCancelledError(_HintedActantError):
    """Workflow 被取消。"""

    kind = "workflow_cancelled"
    hint = (
        " The workflow was cancelled — either via Runtime.cancel_workflow(), or as"
        " part of a terminal transition (a parked flow body is released and"
        " surfaces this error). This is terminal: the workflow will not resume;"
        " start a new one if the work is still needed."
    )


class FlowReplayError(_HintedActantError):
    """flow 重放冲突（提交序列指纹 fail-fast）。

    flow 体重放时第 n 次 ``task.submit()`` 与工作流历史中同序位节点的指纹
    （name / payload / 超时 / 优先级 / 重试策略 / 依赖边集合）不一致——
    提交序列确定性契约被破坏。重放模型要求 flow 体确定性（不得依赖
    wall-clock / 随机数 / 非任务副作用决定提交序列），此异常表示该约束
    在本次重放中被违反，显式失败而非静默错位。
    """

    kind = "replay"
    hint = ""


# Rust ActantError variant → Python exception class
#
# 注意：以下三个 Python 异常类故意不在此表中，原因如下：
# - PayloadTooLargeError (kind="payload_too_large")：无对应 Rust ActantError 变体，
#   由 Python 层在序列化后直接检查大小并抛出。其构造签名为 (actual, limit) 而非
#   (message)，与 raise_for_kind(kind, message) 接口不兼容。
# - WorkflowFailedError (kind="workflow_failed")：workflow 终态异常，由
#   raise_for_state() 根据 workflow 终态 "Failed" 映射，而非来自 Rust ActantError。
# - WorkflowCancelledError (kind="workflow_cancelled")：同上，对应终态 "Cancelled"。
_KIND_TO_EXCEPTION: dict[str, type[ActantError]] = {
    "storage": StorageError,
    "network": NetworkError,
    "serialization": SerializationError,
    "actor": ActorError,
    "workflow": WorkflowError,
    "task": TaskError,
    "worker": WorkerError,
    "config": ConfigError,
    "metrics": MetricsError,
    "not_found": NotFoundError,
    "already_exists": AlreadyExistsError,
    "timeout": ActantTimeoutError,
    "cancelled": TaskCancelledError,
    "invalid_state": InvalidStateError,
    "replay": FlowReplayError,
    "internal": InternalError,
}

# Workflow 终态 → Python exception class
_STATE_TO_EXCEPTION: dict[str, type[ActantError]] = {
    "Timeout": ActantTimeoutError,
    "Failed": WorkflowFailedError,
    "Cancelled": WorkflowCancelledError,
}


def raise_for_kind(kind: str, message: str) -> None:
    """根据 Rust ActantError kind 抛出对应的 Python 异常。"""
    exc_cls = _KIND_TO_EXCEPTION.get(kind)
    if exc_cls is not None:
        raise exc_cls(message)
    raise ActantError(message, kind=kind)


# 跨语言错误类型保留协议：
#
# Rust 端 ``TaskCompletion::Failed.error`` 是 ``String``，无法直接携带 Python
# 异常类信息。为保留错误类型，约定在 error 字符串前缀编码 kind：
#
#     ``[actant:KIND] message``
#
# Python 侧 ``_decode_error_kind`` 解析前缀，调用 ``raise_for_kind`` 重建子类。
# 无前缀的 error 字符串视为 ``task`` kind（任务执行失败的默认语义）。
#
# 编码格式选择 ``[actant:KIND] `` 而非 JSON：
# - Rust 端只需 ``format!("[actant:{}] {}", kind, msg)``，零依赖。
# - 前缀可被人类直接阅读（日志/trace 友好）。
# - 解析只需一次字符串分割，O(1)。
_ERROR_KIND_PREFIX = "[actant:"


def encode_error_kind(kind: str, message: str) -> str:
    """将 kind 编码到错误消息前缀，供跨语言传播。

    格式：``[actant:KIND] message``。

    与 ``_decode_error_kind`` 配对使用。Rust 端生成 ``TaskCompletion::Failed``
    时应使用此格式（Rust 端直接 ``format!`` 即可，无需调用 Python）。
    """
    return f"{_ERROR_KIND_PREFIX}{kind}] {message}"


def decode_error_kind(error_str: str) -> tuple[str, str]:
    """解析 ``encode_error_kind`` 编码的错误字符串。

    无前缀时返回 ``("task", error_str)``（任务执行失败的默认 kind）。

    Returns:
        ``(kind, message)`` 元组。
    """
    if not error_str:
        return "task", error_str
    if not error_str.startswith(_ERROR_KIND_PREFIX):
        return "task", error_str
    # 寻找闭合 ']'
    end = error_str.find("]", len(_ERROR_KIND_PREFIX))
    if end < 0:
        return "task", error_str
    kind = error_str[len(_ERROR_KIND_PREFIX):end]
    # 跳过 "] "（若有）
    message_start = end + 1
    if message_start < len(error_str) and error_str[message_start] == " ":
        message_start += 1
    message = error_str[message_start:]
    return kind, message


def reconstruct_error(error_str: str) -> ActantError:
    """从 error 字符串重建对应的 ActantError 子类。

    用于 ``Runtime._on_task_result`` 处理 ``state == "Failed"`` 路径：
    Rust 端 ``TaskCompletion::Failed.error`` 是字符串，通过此函数解析
    kind 前缀并重建对应 Python 异常子类。

    无 kind 前缀时返回 ``TaskError``（任务执行失败的默认语义）。
    """
    kind, message = decode_error_kind(error_str)
    exc_cls = _KIND_TO_EXCEPTION.get(kind)
    if exc_cls is not None:
        # 各子类 __init__ 会添加 hint 后缀，这里直接调用。
        return exc_cls(message)
    return ActantError(message, kind=kind)


def raise_for_state(state: str, error: str, *, failed_tasks: list[list[str]] | None = None) -> None:
    """根据 workflow 终态抛出对应的 Python 异常。

    Args:
        state: Workflow 终态（如 "Failed", "Cancelled", "Timeout"）。
        error: 错误消息字符串。
        failed_tasks: 结构化失败任务列表，每项为 [task_id, task_name, error]。
    """
    exc_cls = _STATE_TO_EXCEPTION.get(state)
    if exc_cls is not None:
        if exc_cls is WorkflowFailedError:
            if failed_tasks:
                # 使用 Rust 端的结构化数据
                first = failed_tasks[0]
                task_name = first[1] if len(first) > 1 else None
                task_error = first[2] if len(first) > 2 else error
                raise WorkflowFailedError(error, task_name=task_name, task_error=task_error)
            # 回退：解析错误字符串以保持向后兼容
            task_name = None
            task_error = error
            if error.startswith("task ") and " failed" in error:
                parts = error.split(" failed", 1)
                task_name = parts[0][5:]  # strip "task " prefix
                if len(parts) > 1 and parts[1].startswith(": "):
                    task_error = parts[1][2:]
            raise WorkflowFailedError(error, task_name=task_name, task_error=task_error)
        raise exc_cls(error)
    raise ActantError(error, kind=state.lower())
