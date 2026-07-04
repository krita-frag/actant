"""exceptions 模块单元测试：异常层次、raise_for_kind、raise_for_state。"""

from __future__ import annotations

import pytest

from actant.exceptions import (
    ActantError,
    ActantTimeoutError,
    ActorError,
    AlreadyExistsError,
    ConfigError,
    InternalError,
    InvalidStateError,
    MetricsError,
    NetworkError,
    NotFoundError,
    SerializationError,
    StorageError,
    TaskCancelledError,
    TaskError,
    WorkerError,
    WorkflowCancelledError,
    WorkflowError,
    WorkflowFailedError,
    raise_for_kind,
    raise_for_state,
)

# ---------------------------------------------------------------------------
# 异常层次与 kind 标签
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """所有异常继承自 ActantError，kind 标签与 Rust variant 对应。"""

    @pytest.mark.parametrize("exc_cls,kind", [
        (StorageError, "storage"),
        (NetworkError, "network"),
        (SerializationError, "serialization"),
        (ActorError, "actor"),
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
    ])
    def test_kind_label(self, exc_cls, kind):
        exc = exc_cls("test message")
        assert exc.kind == kind
        assert isinstance(exc, ActantError)
        assert isinstance(exc, RuntimeError)

    def test_workflow_failed_kind(self):
        exc = WorkflowFailedError("wf failed")
        assert exc.kind == "workflow_failed"

    def test_workflow_cancelled_kind(self):
        exc = WorkflowCancelledError("wf cancelled")
        assert exc.kind == "workflow_cancelled"


# ---------------------------------------------------------------------------
# 提示信息附加（hint）
# ---------------------------------------------------------------------------


class TestHintMessages:
    """特定异常类型会附加诊断 hint。"""

    def test_storage_hint(self):
        exc = StorageError("db locked")
        assert "data_dir permissions" in str(exc)
        assert "LMDB" in str(exc)

    def test_network_hint(self):
        exc = NetworkError("conn refused")
        assert "reachable" in str(exc)

    def test_serialization_hint(self):
        exc = SerializationError("bad payload")
        assert "picklable" in str(exc)

    def test_base_actant_no_hint(self):
        """ActantError 基类不附加 hint。"""
        exc = ActantError("plain")
        assert str(exc) == "plain"

    def test_actor_no_hint(self):
        """ActorError 不附加 hint。"""
        assert str(ActorError("err")) == "err"

    def test_workflow_failed_no_hint(self):
        """WorkflowFailedError 不附加 hint。"""
        assert str(WorkflowFailedError("err")) == "err"


# ---------------------------------------------------------------------------
# WorkflowFailedError 字段
# ---------------------------------------------------------------------------


class TestWorkflowFailedError:
    def test_default_fields_none(self):
        exc = WorkflowFailedError("failed")
        assert exc.task_name is None
        assert exc.task_error is None

    def test_explicit_fields(self):
        exc = WorkflowFailedError("failed", task_name="t1", task_error="boom")
        assert exc.task_name == "t1"
        assert exc.task_error == "boom"

    def test_inherits_actant_error(self):
        exc = WorkflowFailedError("failed")
        assert isinstance(exc, ActantError)
        assert isinstance(exc, RuntimeError)


# ---------------------------------------------------------------------------
# raise_for_kind
# ---------------------------------------------------------------------------


class TestRaiseForKind:
    """raise_for_kind 根据 kind 字符串抛出对应异常。"""

    @pytest.mark.parametrize("kind,exc_cls", [
        ("storage", StorageError),
        ("network", NetworkError),
        ("serialization", SerializationError),
        ("actor", ActorError),
        ("workflow", WorkflowError),
        ("task", TaskError),
        ("worker", WorkerError),
        ("config", ConfigError),
        ("metrics", MetricsError),
        ("not_found", NotFoundError),
        ("already_exists", AlreadyExistsError),
        ("timeout", ActantTimeoutError),
        ("cancelled", TaskCancelledError),
        ("invalid_state", InvalidStateError),
        ("internal", InternalError),
    ])
    def test_known_kind(self, kind, exc_cls):
        with pytest.raises(exc_cls) as ei:
            raise_for_kind(kind, "boom")
        assert ei.value.kind == kind
        # 消息原样传递（部分类型会附加 hint，但 "boom" 应在消息中）
        assert "boom" in str(ei.value)

    def test_unknown_kind_falls_back_to_actant_error(self):
        """未知 kind 抛 ActantError，kind 透传。"""
        with pytest.raises(ActantError) as ei:
            raise_for_kind("future_kind", "msg")
        assert ei.value.kind == "future_kind"
        # 但不应是更具体的子类
        assert type(ei.value) is ActantError

    def test_unknown_kind_message_preserved(self):
        with pytest.raises(ActantError) as ei:
            raise_for_kind("xyz", "raw message")
        assert "raw message" in str(ei.value)


# ---------------------------------------------------------------------------
# raise_for_state
# ---------------------------------------------------------------------------


class TestRaiseForState:
    """raise_for_state 根据 workflow 终态抛出对应异常。"""

    def test_failed_state_without_failed_tasks(self):
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "workflow failed")
        assert ei.value.kind == "workflow_failed"
        assert ei.value.task_name is None
        assert ei.value.task_error == "workflow failed"

    def test_failed_state_with_failed_tasks(self):
        """使用 Rust 端的结构化数据 [task_id, task_name, error]。"""
        failed_tasks = [["t0", "step_a", "ValueError: bad input"]]
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "wf failed", failed_tasks=failed_tasks)
        assert ei.value.task_name == "step_a"
        assert ei.value.task_error == "ValueError: bad input"

    def test_failed_state_with_multiple_failed_tasks_uses_first(self):
        """多个失败任务时取第一个。"""
        failed_tasks = [
            ["t0", "step_a", "err_a"],
            ["t1", "step_b", "err_b"],
        ]
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "wf failed", failed_tasks=failed_tasks)
        assert ei.value.task_name == "step_a"
        assert ei.value.task_error == "err_a"

    def test_failed_state_with_partial_failed_tasks(self):
        """failed_tasks 项只有 1 个元素时不崩溃，task_name/error 回退。"""
        failed_tasks = [["t0"]]
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "fallback", failed_tasks=failed_tasks)
        assert ei.value.task_name is None
        assert ei.value.task_error == "fallback"

    def test_failed_state_with_empty_failed_tasks(self):
        """failed_tasks 为空列表时回退到字符串解析。"""
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "workflow failed", failed_tasks=[])
        assert ei.value.task_name is None
        assert ei.value.task_error == "workflow failed"

    def test_failed_state_parses_legacy_message(self):
        """回退路径解析 'task <name> failed: <error>' 格式。"""
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "task step_x failed: ValueError: bad")
        assert ei.value.task_name == "step_x"
        assert ei.value.task_error == "ValueError: bad"

    def test_failed_state_legacy_message_without_colon(self):
        """legacy 消息无 ': error' 后缀时 task_error=error 全文。"""
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "task step_x failed")
        assert ei.value.task_name == "step_x"
        # 无 ': ' 后缀，task_error 回退为完整 error 字符串
        assert "step_x failed" in ei.value.task_error

    def test_failed_state_legacy_message_not_task_prefix(self):
        """消息不以 'task ' 开头时不解析 task_name。"""
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "general failure")
        assert ei.value.task_name is None
        assert ei.value.task_error == "general failure"

    def test_timeout_state(self):
        with pytest.raises(ActantTimeoutError) as ei:
            raise_for_state("Timeout", "wf timed out")
        assert "wf timed out" in str(ei.value)

    def test_cancelled_state(self):
        with pytest.raises(WorkflowCancelledError) as ei:
            raise_for_state("Cancelled", "wf cancelled")
        assert ei.value.kind == "workflow_cancelled"

    def test_completed_state_falls_back(self):
        """非失败终态（如 Completed）抛 ActantError with kind=lower(state)。"""
        with pytest.raises(ActantError) as ei:
            raise_for_state("Completed", "done")
        assert ei.value.kind == "completed"
        assert type(ei.value) is ActantError

    def test_unknown_state_falls_back(self):
        with pytest.raises(ActantError) as ei:
            raise_for_state("Unknown", "msg")
        assert ei.value.kind == "unknown"

    def test_failed_state_with_failed_tasks_short_item(self):
        """failed_tasks 项有 2 个元素 [id, name]，无 error 字段。"""
        failed_tasks = [["t0", "step_a"]]
        with pytest.raises(WorkflowFailedError) as ei:
            raise_for_state("Failed", "fallback error", failed_tasks=failed_tasks)
        assert ei.value.task_name == "step_a"
        # len(first) == 2 不满足 > 2，所以 task_error 回退为 error 参数
        assert ei.value.task_error == "fallback error"


# ---------------------------------------------------------------------------
# 可捕获性
# ---------------------------------------------------------------------------


class TestCatchability:
    """确保所有异常都能通过 ActantError 统一捕获。"""

    @pytest.mark.parametrize("exc_cls", [
        StorageError, NetworkError, SerializationError, ActorError,
        WorkflowError, TaskError, WorkerError, ConfigError,
        MetricsError, NotFoundError, AlreadyExistsError,
        ActantTimeoutError, TaskCancelledError, InvalidStateError,
        InternalError, WorkflowFailedError, WorkflowCancelledError,
    ])
    def test_catchable_as_actant_error(self, exc_cls):
        try:
            raise exc_cls("test")
        except ActantError as e:
            assert isinstance(e, exc_cls)

    def test_catchable_as_runtime_error(self):
        try:
            raise StorageError("db")
        except RuntimeError:
            pass
        else:
            pytest.fail("ActantError 应可被 RuntimeError 捕获")
