"""Actant 异常类层次。

与 Rust 侧 ActantError 枚举一一对应，确保跨层错误类型不丢失。
"""


class ActantError(RuntimeError):
    """Actant 基础异常。"""

    def __init__(self, message: str, *, kind: str = "internal") -> None:
        super().__init__(message)
        self.kind = kind


class StorageError(ActantError):
    """存储层错误。"""

    def __init__(self, message: str) -> None:
        hint = (
            " Check data_dir permissions, ensure only one process accesses the same LMDB path"
            " (LMDB uses process-level locking — multiple processes on the same data_dir will fail),"
            " and verify disk space."
        )
        super().__init__(message + hint, kind="storage")


class NetworkError(ActantError):
    """网络层错误。"""

    def __init__(self, message: str) -> None:
        hint = (
            " Ensure all nodes are reachable, ports are open, and bootstrap addresses are correct."
        )
        super().__init__(message + hint, kind="network")


class SerializationError(ActantError):
    """序列化/反序列化错误。"""

    def __init__(self, message: str) -> None:
        hint = " Ensure task arguments and return values are picklable."
        super().__init__(message + hint, kind="serialization")


class ActorError(ActantError):
    """Actor 系统错误。"""

    def __init__(self, message: str) -> None:
        hint = " Check actor mailbox capacity, message serialization, and that the target actor is still alive."
        super().__init__(message + hint, kind="actor")


class WorkflowError(ActantError):
    """Workflow 编排错误。"""

    def __init__(self, message: str) -> None:
        hint = " Verify DAG structure (no cycles, all task_ids referenced in edges exist), and that task payloads are valid."
        super().__init__(message + hint, kind="workflow")


class TaskError(ActantError):
    """任务执行错误。"""

    def __init__(self, message: str) -> None:
        hint = " Check task function implementation, argument types, and that all dependencies are importable in the worker process."
        super().__init__(message + hint, kind="task")


class WorkerError(ActantError):
    """Worker 运行时错误。"""

    def __init__(self, message: str) -> None:
        hint = " Check worker logs, max_concurrent_tasks capacity, and that the Runtime is not in drain mode."
        super().__init__(message + hint, kind="worker")


class ConfigError(ActantError):
    """配置错误。"""

    def __init__(self, message: str) -> None:
        hint = " Review ActantConfig fields — failover params must satisfy heartbeat < failure_timeout < lease_duration, and data_dir must be writable."
        super().__init__(message + hint, kind="config")


class MetricsError(ActantError):
    """指标管道错误（初始化或采集失败）。"""

    def __init__(self, message: str) -> None:
        hint = " Check that the metrics port is not already in use and prometheus_client is installed."
        super().__init__(message + hint, kind="metrics")


class NotFoundError(ActantError):
    """资源未找到。"""

    def __init__(self, message: str) -> None:
        hint = " Verify the resource ID and that it hasn't been garbage-collected."
        super().__init__(message + hint, kind="not_found")


class AlreadyExistsError(ActantError):
    """资源已存在。"""

    def __init__(self, message: str) -> None:
        hint = " Use a different name or ID."
        super().__init__(message + hint, kind="already_exists")


class ActantTimeoutError(ActantError):
    """操作超时。"""

    def __init__(self, message: str) -> None:
        hint = " Consider increasing the timeout, checking worker availability, or verifying network connectivity."
        super().__init__(message + hint, kind="timeout")


class TaskCancelledError(ActantError):
    """操作被取消。"""

    def __init__(self, message: str) -> None:
        hint = " The task was cancelled via Runtime.cancel_task() or a parent flow was cancelled. Use AsyncResult.state to check cancellation status."
        super().__init__(message + hint, kind="cancelled")


class InvalidStateError(ActantError):
    """无效状态操作（如在 drain 模式下提交任务）。"""

    def __init__(self, message: str) -> None:
        hint = " Ensure Runtime.start() has been called and the Runtime is not stopped/draining. Use 'with Runtime(...) as rt:' to manage lifecycle."
        super().__init__(message + hint, kind="invalid_state")


class InternalError(ActantError):
    """内部错误。"""

    def __init__(self, message: str) -> None:
        hint = " This is likely a bug in Actant — please report it with the full stack trace and reproduction steps."
        super().__init__(message + hint, kind="internal")


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


class WorkflowCancelledError(ActantError):
    """Workflow 被取消。"""

    def __init__(self, message: str) -> None:
        hint = " The workflow was cancelled by a user call or a parent flow cancellation. Check cancel_event propagation in nested flows."
        super().__init__(message + " " + hint, kind="workflow_cancelled")


# Rust ActantError variant → Python exception class
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
