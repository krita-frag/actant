"""_api.py 单元测试：模块级 API（submit/start/stop/list_workflows/cancel 等）。

覆盖目标：100% 行覆盖 + 分支覆盖。
通过 mock _Node 类避免真实网络/持久化。
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

import actant._api as api_mod
from actant._api import (
    cancel,
    cancel_task,
    get_active_node,
    list_workflows,
    start,
    stop,
    submit,
    workflow_state,
    workflow_status,
)
from actant.result import AsyncResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_active_node():
    """每个测试前后重置模块级 _active_node。"""
    api_mod._active_node = None
    yield
    api_mod._active_node = None


@pytest.fixture
def mock_node():
    """构造一个 mock _Node 实例，_runtime 非 None。"""
    node = MagicMock()
    node._runtime = MagicMock()  # 非 None，表示已启动
    node.shutdown = MagicMock()
    node.submit = MagicMock(return_value=MagicMock(spec=AsyncResult))
    node.list_workflows = MagicMock(return_value=[("wf-1", "Running"), ("wf-2", "Completed")])
    node.workflow_state = MagicMock(return_value="Running")
    node.cancel = MagicMock()
    node.cancel_task = MagicMock(return_value=True)
    node.get_workflow_status = MagicMock(return_value={"workflow_id": "wf-1", "state": "Running"})
    return node


@pytest.fixture
def mock_node_class(mock_node):
    """patch _Node 类，使其返回 mock_node。"""
    with patch.object(api_mod, "_Node") as mock_cls:
        mock_cls.return_value = mock_node
        yield mock_cls


# ---------------------------------------------------------------------------
# _get_or_create_transient
# ---------------------------------------------------------------------------


class TestGetOrCreateTransient:
    def test_creates_transient_when_no_active_node(self, mock_node_class, mock_node):
        """无活跃节点时创建瞬态节点。"""
        node = api_mod._get_or_create_transient("test-key")
        assert node is mock_node
        mock_node_class.assert_called_once_with(
            "actant-transient", _executing=False, signing_key="test-key"
        )
        mock_node.start.assert_called_once()

    def test_reuses_active_node_when_runtime_exists(self, mock_node):
        """有活跃常驻节点且 runtime 存在时复用之。"""
        api_mod._active_node = mock_node
        node = api_mod._get_or_create_transient("test-key")
        assert node is mock_node

    def test_creates_transient_when_active_node_runtime_none(self, mock_node_class, mock_node):
        """活跃节点 runtime 为 None（已停止）时创建新的瞬态节点。"""
        stopped_node = MagicMock()
        stopped_node._runtime = None
        api_mod._active_node = stopped_node

        node = api_mod._get_or_create_transient("test-key")
        assert node is mock_node
        mock_node_class.assert_called_once_with(
            "actant-transient", _executing=False, signing_key="test-key"
        )

    def test_transient_node_written_to_active_node_for_reuse(
        self, mock_node_class, mock_node
    ):
        """B1 回归：瞬态节点写入 _active_node 以便后续 submit 复用。

        旧实现瞬态节点从不写入 _active_node，导致每次 submit 都新建+启动节点，
        造成资源泄漏（线程/网络连接堆积）。
        """
        node = api_mod._get_or_create_transient("test-key")
        assert node is mock_node
        # 关键断言：_active_node 被写入，使下次调用复用而非重建
        assert api_mod._active_node is mock_node

    def test_second_call_reuses_transient_node(self, mock_node_class, mock_node):
        """B1 回归：第二次调用复用第一次创建的瞬态节点，不重复创建。"""
        first = api_mod._get_or_create_transient("test-key")
        second = api_mod._get_or_create_transient("test-key")
        assert first is second is mock_node
        # _Node 构造函数仅调用一次（复用而非重建）
        mock_node_class.assert_called_once()
        # start 仅调用一次
        mock_node.start.assert_called_once()

    def test_concurrent_start_during_transient_start_reuses_persistent(
        self, mock_node_class, mock_node
    ):
        """B1 边界：瞬态节点 start 期间其他线程已 start 常驻节点时，
        关闭瞬态节点并复用常驻节点。"""
        persistent_node = MagicMock()
        persistent_node._runtime = MagicMock()

        # 模拟：在 node.start() 调用后、再次获取锁前，另一线程写入常驻节点
        def start_side_effect():
            api_mod._active_node = persistent_node

        mock_node.start.side_effect = start_side_effect

        node = api_mod._get_or_create_transient("test-key")
        # 应复用常驻节点
        assert node is persistent_node
        # 瞬态节点应被关闭
        mock_node.shutdown.assert_called_once_with(timeout=5.0)


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


class TestSubmit:
    def test_submit_returns_async_result(self, mock_node_class, mock_node):
        """submit 返回节点的 submit 结果。"""
        flow = MagicMock()
        result = submit(flow, 1, 2, signing_key="test-key", key="value")
        mock_node.submit.assert_called_once_with(flow, 1, 2, key="value")
        assert result is mock_node.submit.return_value

    def test_submit_with_no_args(self, mock_node_class, mock_node):
        """submit 无参数调用。"""
        flow = MagicMock()
        submit(flow, signing_key="test-key")
        mock_node.submit.assert_called_once_with(flow)

    def test_submit_uses_env_var_when_signing_key_none(
        self, mock_node_class, mock_node, monkeypatch
    ):
        """H2: signing_key=None 时从 ACTANT_SIGNING_KEY 环境变量读取。"""
        monkeypatch.setenv("ACTANT_SIGNING_KEY", "env-secret")
        flow = MagicMock()
        submit(flow)
        # 验证 _Node 用 env key 创建
        mock_node_class.assert_called_once_with(
            "actant-transient", _executing=False, signing_key="env-secret"
        )

    def test_submit_raises_when_no_key_and_no_env(
        self, mock_node_class, mock_node, monkeypatch
    ):
        """H2: 既无 signing_key 也无环境变量时抛 ValueError。"""
        monkeypatch.delenv("ACTANT_SIGNING_KEY", raising=False)
        flow = MagicMock()
        with pytest.raises(ValueError, match="signing key required"):
            submit(flow)
        # 不应创建节点
        mock_node_class.assert_not_called()

    def test_submit_rejects_empty_signing_key(self, mock_node_class, mock_node, monkeypatch):
        """H2: 显式传入空字符串 signing_key 抛 ValueError。"""
        monkeypatch.delenv("ACTANT_SIGNING_KEY", raising=False)
        flow = MagicMock()
        with pytest.raises(ValueError, match="signing_key must not be empty"):
            submit(flow, signing_key="")

    def test_submit_explicit_key_overrides_env(self, mock_node_class, mock_node, monkeypatch):
        """H2: 显式 signing_key 优先于环境变量。"""
        monkeypatch.setenv("ACTANT_SIGNING_KEY", "env-secret")
        flow = MagicMock()
        submit(flow, signing_key="explicit-secret")
        mock_node_class.assert_called_once_with(
            "actant-transient", _executing=False, signing_key="explicit-secret"
        )


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


class TestStart:
    def test_start_default_args(self, mock_node_class, mock_node):
        """start 使用默认参数。"""
        node = start(signing_key="test-key")
        assert node is mock_node
        mock_node_class.assert_called_once()
        # 验证默认参数
        call_kwargs = mock_node_class.call_args
        assert call_kwargs.args[0] == "actant"
        assert call_kwargs.kwargs["_executing"] is True
        assert call_kwargs.kwargs["signing_key"] == "test-key"
        mock_node.start.assert_called_once()
        assert api_mod._active_node is mock_node

    def test_start_with_custom_args(self, mock_node_class, mock_node):
        """start 接受自定义参数。"""
        node = start(
            "worker-1",
            signing_key="secret",
            max_concurrent_tasks=4,
            node_id="node-123",
            data_dir="/tmp/data",
            network={"bootstrap": []},
            router=MagicMock(),
            port=8080,
            listen_ip="127.0.0.1",
            heartbeat_interval=1.0,
            failure_timeout=5.0,
            default_task_timeout=30.0,
            capabilities={"gpu": True},
            log_level="DEBUG",
        )
        assert node is mock_node
        call_kwargs = mock_node_class.call_args
        assert call_kwargs.args[0] == "worker-1"
        assert call_kwargs.kwargs["signing_key"] == "secret"
        assert call_kwargs.kwargs["max_concurrent_tasks"] == 4
        assert call_kwargs.kwargs["node_id"] == "node-123"
        assert call_kwargs.kwargs["data_dir"] == "/tmp/data"
        assert call_kwargs.kwargs["port"] == 8080
        assert call_kwargs.kwargs["listen_ip"] == "127.0.0.1"
        assert call_kwargs.kwargs["log_level"] == "DEBUG"

    def test_start_raises_when_active_node_exists(self, mock_node):
        """已有活跃节点时 start 抛 RuntimeError。"""
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="an active node already exists"):
            start("new-node", signing_key="test-key")

    def test_start_overwrites_stopped_node(self, mock_node_class, mock_node):
        """已存在的节点 runtime 为 None（已停止）时允许 start 新节点。"""
        stopped_node = MagicMock()
        stopped_node._runtime = None
        api_mod._active_node = stopped_node

        node = start("new-worker", signing_key="test-key")
        assert node is mock_node
        assert api_mod._active_node is mock_node

    def test_start_uses_env_var_when_signing_key_none(
        self, mock_node_class, mock_node, monkeypatch
    ):
        """H2: start 也支持 ACTANT_SIGNING_KEY 环境变量默认。"""
        monkeypatch.setenv("ACTANT_SIGNING_KEY", "env-secret")
        node = start("worker-1")
        assert node is mock_node
        call_kwargs = mock_node_class.call_args
        assert call_kwargs.kwargs["signing_key"] == "env-secret"

    def test_start_raises_when_no_key_and_no_env(
        self, mock_node_class, mock_node, monkeypatch
    ):
        """H2: start 既无 signing_key 也无环境变量时抛 ValueError。"""
        monkeypatch.delenv("ACTANT_SIGNING_KEY", raising=False)
        with pytest.raises(ValueError, match="signing key required"):
            start("worker-1")
        mock_node_class.assert_not_called()


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


class TestStop:
    def test_stop_active_node(self, mock_node):
        """stop 调用活跃节点的 shutdown。"""
        api_mod._active_node = mock_node
        stop(timeout=5.0)
        mock_node.shutdown.assert_called_once_with(timeout=5.0)
        assert api_mod._active_node is None

    def test_stop_default_timeout(self, mock_node):
        """stop 默认 timeout=10.0。"""
        api_mod._active_node = mock_node
        stop()
        mock_node.shutdown.assert_called_once_with(timeout=10.0)

    def test_stop_no_active_node_noop(self):
        """无活跃节点时 stop 不抛异常。"""
        # 不应抛异常
        stop()

    def test_stop_clears_active_node_even_if_shutdown_raises(self, mock_node):
        """shutdown 抛异常时仍清空 _active_node。"""
        mock_node.shutdown.side_effect = RuntimeError("shutdown failed")
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="shutdown failed"):
            stop()
        # _active_node 应已清空
        assert api_mod._active_node is None


# ---------------------------------------------------------------------------
# get_active_node
# ---------------------------------------------------------------------------


class TestGetActiveNode:
    def test_returns_none_when_no_active_node(self):
        assert get_active_node() is None

    def test_returns_active_node(self, mock_node):
        api_mod._active_node = mock_node
        assert get_active_node() is mock_node


# ---------------------------------------------------------------------------
# list_workflows
# ---------------------------------------------------------------------------


class TestListWorkflows:
    def test_returns_workflow_list(self, mock_node):
        api_mod._active_node = mock_node
        result = list_workflows()
        mock_node.list_workflows.assert_called_once()
        assert result == [("wf-1", "Running"), ("wf-2", "Completed")]

    def test_raises_when_no_active_node(self):
        with pytest.raises(RuntimeError, match="no active node"):
            list_workflows()

    def test_raises_when_node_runtime_none(self, mock_node):
        mock_node._runtime = None
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="no active node"):
            list_workflows()


# ---------------------------------------------------------------------------
# workflow_state
# ---------------------------------------------------------------------------


class TestWorkflowState:
    def test_returns_state_string(self, mock_node):
        api_mod._active_node = mock_node
        state = workflow_state("wf-1")
        mock_node.workflow_state.assert_called_once_with("wf-1")
        assert state == "Running"

    def test_raises_when_no_active_node(self):
        with pytest.raises(RuntimeError, match="no active node"):
            workflow_state("wf-1")

    def test_raises_when_node_runtime_none(self, mock_node):
        mock_node._runtime = None
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="no active node"):
            workflow_state("wf-1")


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------


class TestCancel:
    def test_cancel_calls_node_cancel(self, mock_node):
        api_mod._active_node = mock_node
        cancel("wf-1")
        mock_node.cancel.assert_called_once_with("wf-1")

    def test_raises_when_no_active_node(self):
        with pytest.raises(RuntimeError, match="no active node"):
            cancel("wf-1")

    def test_raises_when_node_runtime_none(self, mock_node):
        mock_node._runtime = None
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="no active node"):
            cancel("wf-1")


# ---------------------------------------------------------------------------
# cancel_task
# ---------------------------------------------------------------------------


class TestCancelTask:
    def test_cancel_task_returns_bool(self, mock_node):
        api_mod._active_node = mock_node
        result = cancel_task("wf-1", "task-1")
        mock_node.cancel_task.assert_called_once_with("wf-1", "task-1")
        assert result is True

    def test_raises_when_no_active_node(self):
        with pytest.raises(RuntimeError, match="no active node"):
            cancel_task("wf-1", "task-1")

    def test_raises_when_node_runtime_none(self, mock_node):
        mock_node._runtime = None
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="no active node"):
            cancel_task("wf-1", "task-1")


# ---------------------------------------------------------------------------
# workflow_status
# ---------------------------------------------------------------------------


class TestWorkflowStatus:
    def test_returns_status_dict(self, mock_node):
        api_mod._active_node = mock_node
        status = workflow_status("wf-1")
        mock_node.get_workflow_status.assert_called_once_with("wf-1")
        assert status == {"workflow_id": "wf-1", "state": "Running"}

    def test_raises_when_no_active_node(self):
        with pytest.raises(RuntimeError, match="no active node"):
            workflow_status("wf-1")

    def test_raises_when_node_runtime_none(self, mock_node):
        mock_node._runtime = None
        api_mod._active_node = mock_node
        with pytest.raises(RuntimeError, match="no active node"):
            workflow_status("wf-1")


# ---------------------------------------------------------------------------
# 并发安全测试
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_lock_is_thread_safe(self, mock_node_class, mock_node):
        """多线程并发调用 start 不应破坏锁语义。"""
        results: list[bool] = []
        barrier = threading.Barrier(4)

        def worker():
            barrier.wait()
            try:
                start(f"worker-{threading.get_ident()}", signing_key="test-key")
                results.append(True)
            except RuntimeError:
                results.append(False)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 仅一个线程应成功 start，其余应抛 RuntimeError
        assert sum(results) == 1
