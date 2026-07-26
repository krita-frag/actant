"""`actant.exceptions` 纯单元测试。

覆盖异常构造、kind 镜像、`raise_for_kind` / `raise_for_state` 全分支，
以及结构化失败任务的解析回退路径。不依赖 Rust 运行时。
"""

from __future__ import annotations

import pytest

from actant import exceptions as exc
from actant.exceptions import (
    ActantError,
    ActantTimeoutError,
    AlreadyExistsError,
    ConfigError,
    InternalError,
    InvalidStateError,
    MetricsError,
    NetworkError,
    NotFoundError,
    PayloadTooLargeError,
    SerializationError,
    StorageError,
    TaskCancelledError,
    TaskError,
    WorkerError,
    WorkflowCancelledError,
    WorkflowError,
    WorkflowFailedError,
)

# ---------------------------------------------------------------------------
# 异常类型与 kind 镜像
# ---------------------------------------------------------------------------

KIND_CASES = [
    (StorageError, "storage"),
    (NetworkError, "network"),
    (SerializationError, "serialization"),
    (exc.ActorError, "actor"),
    (WorkflowError, "workflow"),
    (TaskError, "task"),
    (WorkerError, "worker"),
    (ConfigError, "config"),
    (MetricsError, "metrics"),
    (NotFoundError, "not_found"),
    (AlreadyExistsError, "already_exists"),
    (ActantTimeoutError, "timeout"),
    (TaskCancelledError, "cancelled"),
    (InvalidStateError, "invalid_state"),
    (InternalError, "internal"),
]


@pytest.mark.parametrize("exc_cls,kind", KIND_CASES, ids=lambda v: v if isinstance(v, str) else v.__name__)
def test_exception_kind_mirror(exc_cls: type[ActantError], kind: str) -> None:
    """每个异常类的 `.kind` 与 Rust ActantError variant 一致。"""
    e = exc_cls("boom")
    assert e.kind == kind
    assert isinstance(e, ActantError)
    assert isinstance(e, RuntimeError)
    assert "boom" in str(e)


def test_base_actant_error_default_kind_is_internal() -> None:
    e = ActantError("x")
    assert e.kind == "internal"


def test_base_actant_error_custom_kind_preserved() -> None:
    e = ActantError("x", kind="custom")
    assert e.kind == "custom"


def test_storage_error_hint_appended() -> None:
    """StorageError 追加 LMDB 提示，帮助定位数据目录问题。"""
    e = StorageError("open failed")
    assert "data_dir" in str(e)
    assert "open failed" in str(e)


def test_network_error_hint_appended() -> None:
    e = NetworkError("dial failed")
    assert "reachable" in str(e)


def test_serialization_error_hint_appended() -> None:
    e = SerializationError("bad bytes")
    assert "picklable" in str(e)


def test_not_found_and_already_exists_hints() -> None:
    assert "garbage-collected" in str(NotFoundError("missing"))
    assert "different name" in str(AlreadyExistsError("dup"))


def test_timeout_hint_appended() -> None:
    assert "timeout" in str(ActantTimeoutError("slow"))


def test_payload_too_large_carries_sizes() -> None:
    e = PayloadTooLargeError(actual=2048, limit=1024)
    assert e.actual == 2048
    assert e.limit == 1024
    assert e.kind == "payload_too_large"
    assert "2048" in str(e)
    assert "1024" in str(e)
    assert "exceeds" in str(e)


def test_payload_too_large_subclass_of_actant_error() -> None:
    assert isinstance(PayloadTooLargeError(1, 2), ActantError)


# ---------------------------------------------------------------------------
# raise_for_kind
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exc_cls,kind", KIND_CASES, ids=lambda v: v if isinstance(v, str) else v.__name__)
def test_raise_for_kind_raises_matching_class(exc_cls: type[ActantError], kind: str) -> None:
    with pytest.raises(exc_cls, match="msg"):
        exc.raise_for_kind(kind, "msg")


def test_raise_for_kind_unknown_kind_falls_back_to_base() -> None:
    with pytest.raises(ActantError) as ei:
        exc.raise_for_kind("totally_unknown", "msg")
    assert ei.value.kind == "totally_unknown"
    # 未知 kind 不应是某个具体子类
    assert type(ei.value) is ActantError


def test_raise_for_kind_does_not_swallow_re_raise() -> None:
    """raise_for_kind 对所有已知 kind 都抛异常，不会静默返回。"""
    for kind, _ in KIND_CASES:
        with pytest.raises(ActantError):
            exc.raise_for_kind(kind, "x")


# ---------------------------------------------------------------------------
# raise_for_state
# ---------------------------------------------------------------------------


def test_raise_for_state_failed_with_structured_tasks() -> None:
    failed_tasks = [["t1", "task-one", "boom: reason"]]
    with pytest.raises(WorkflowFailedError) as ei:
        exc.raise_for_state("Failed", "task failed", failed_tasks=failed_tasks)
    assert ei.value.task_name == "task-one"
    assert ei.value.task_error == "boom: reason"
    assert ei.value.kind == "workflow_failed"


def test_raise_for_state_failed_without_structured_falls_back_to_string_parse() -> None:
    """无结构化数据时，回退解析 "task <name> failed: <error>" 字符串。"""
    with pytest.raises(WorkflowFailedError) as ei:
        exc.raise_for_state("Failed", "task my-task failed: network down", failed_tasks=None)
    assert ei.value.task_name == "my-task"
    assert ei.value.task_error == "network down"


def test_raise_for_state_failed_without_structured_no_colon() -> None:
    """错误串不含 ": " 时，task_error 回退为整串。"""
    with pytest.raises(WorkflowFailedError) as ei:
        exc.raise_for_state("Failed", "task t failed", failed_tasks=None)
    assert ei.value.task_name == "t"
    assert ei.value.task_error == "task t failed"


def test_raise_for_state_failed_empty_failed_tasks_uses_string_parse() -> None:
    """空列表视为无结构化数据，走字符串解析路径。"""
    with pytest.raises(WorkflowFailedError) as ei:
        exc.raise_for_state("Failed", "task x failed: oom", failed_tasks=[])
    assert ei.value.task_name == "x"
    assert ei.value.task_error == "oom"


def test_raise_for_state_timeout_raises_timeout() -> None:
    with pytest.raises(ActantTimeoutError, match="slow"):
        exc.raise_for_state("Timeout", "slow")


def test_raise_for_state_cancelled_raises_cancelled() -> None:
    with pytest.raises(WorkflowCancelledError, match="abort"):
        exc.raise_for_state("Cancelled", "abort")


def test_raise_for_state_unknown_state_falls_back_to_base() -> None:
    with pytest.raises(ActantError) as ei:
        exc.raise_for_state("Running", "still going")
    # 未知状态 kind 为小写状态名
    assert ei.value.kind == "running"


def test_raise_for_state_structured_task_short_entry() -> None:
    """结构化失败任务条目不足 3 个元素时优雅降级。"""
    # 仅 1 个元素：task_name/error 取默认
    with pytest.raises(WorkflowFailedError) as ei:
        exc.raise_for_state("Failed", "err", failed_tasks=[["only-id"]])
    assert ei.value.task_name is None
    assert ei.value.task_error == "err"


# ---------------------------------------------------------------------------
# 子类继承关系
# ---------------------------------------------------------------------------


def test_all_workflow_state_exceptions_are_actant_errors() -> None:
    for cls in (WorkflowFailedError, WorkflowCancelledError):
        assert issubclass(cls, ActantError)


def test_kind_to_exception_map_covers_core_kinds() -> None:
    """_KIND_TO_EXCEPTION 应包含所有 Rust ActantError variant 的映射。"""
    keys = set(exc._KIND_TO_EXCEPTION.keys())
    expected = {
        "storage", "network", "serialization", "actor", "workflow",
        "task", "worker", "config", "metrics", "not_found",
        "already_exists", "timeout", "cancelled", "invalid_state", "internal",
    }
    assert expected.issubset(keys)


def test_state_to_exception_map_covers_terminal_states() -> None:
    assert set(exc._STATE_TO_EXCEPTION.keys()) == {"Timeout", "Failed", "Cancelled"}
