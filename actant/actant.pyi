from __future__ import annotations

from collections.abc import Coroutine
from typing import Any

def get_version() -> str: ...
def refresh_logger() -> None: ...

class ActantError(Exception): ...
class NotFoundError(ActantError): ...
class PayloadTooLargeError(ActantError):
    actual: int
    limit: int
    def __init__(self, actual: int, limit: int) -> None: ...

class CancelToken:
    """Cooperative cancellation flag shared between Rust and Python."""
    def is_cancelled(self) -> bool: ...
    @property
    def cancelled(self) -> bool: ...

class _WorkflowState:
    PENDING: _WorkflowState
    RUNNING: _WorkflowState
    COMPLETED: _WorkflowState
    FAILED: _WorkflowState
    CANCELLED: _WorkflowState
    SKIPPED: _WorkflowState

class _RetryPolicy:
    def __init__(
        self,
        max_retries: int = 3,
        delay_ms: int = 1000,
        backoff_multiplier: float = 2.0,
        max_delay_ms: int = 60000,
    ) -> None: ...
    @property
    def max_retries(self) -> int: ...
    @property
    def delay_ms(self) -> int: ...
    @property
    def backoff_multiplier(self) -> float: ...
    @property
    def max_delay_ms(self) -> int: ...
    def to_bytes(self) -> bytes: ...

class _NetworkConfig:
    def __init__(
        self,
        preset: str | None = None,
        bootstrap_nodes: list[str] | None = None,
        hlc_max_drift_ms: int = 500,
        max_pending_direct_requests: int = 1024,
        gossip_bootstrap_peers: list[str] | None = None,
        max_message_size: int = 16777216,
        allowed_peer_ids: list[str] | None = None,
        direct_request_timeout_ms: int = 30000,
        listen_port: int = 0,
        listen_ip: str = "",
    ) -> None: ...
    @property
    def preset(self) -> str: ...
    @property
    def bootstrap_nodes(self) -> list[str]: ...
    @property
    def hlc_max_drift_ms(self) -> int: ...
    @property
    def max_pending_direct_requests(self) -> int: ...
    @property
    def gossip_bootstrap_peers(self) -> list[str]: ...
    @property
    def max_message_size(self) -> int: ...
    @property
    def allowed_peer_ids(self) -> list[str]: ...
    @property
    def direct_request_timeout_ms(self) -> int: ...
    @property
    def listen_port(self) -> int: ...
    @property
    def listen_ip(self) -> str: ...

class _FailoverConfig:
    def __init__(
        self,
        heartbeat_interval_ms: int | None = None,
        failure_timeout_ms: int | None = None,
        lease_expiry_check_interval_secs: int | None = None,
    ) -> None: ...
    @property
    def heartbeat_interval_ms(self) -> int: ...
    @property
    def failure_timeout_ms(self) -> int: ...
    @property
    def lease_expiry_check_interval_secs(self) -> int: ...

class _GossipConfig:
    def __init__(
        self,
        dedup_window_size: int = 1024,
        dedup_ttl_secs: int = 300,
        retry_attempts: int = 3,
        retry_base_delay_ms: int = 100,
    ) -> None: ...
    @property
    def dedup_window_size(self) -> int: ...
    @property
    def dedup_ttl_secs(self) -> int: ...
    @property
    def retry_attempts(self) -> int: ...
    @property
    def retry_base_delay_ms(self) -> int: ...

class _ActantConfig:
    def __init__(
        self,
        payload_signing_key: str,
        network: _NetworkConfig | None = None,
        failover: _FailoverConfig | None = None,
        gossip: _GossipConfig | None = None,
        max_concurrent_tasks: int | None = None,
        default_task_timeout_ms: int | None = None,
        data_dir: str | None = None,
        drain_timeout_secs: int | None = None,
        remote_fallback_delay_ms: int | None = None,
        scheduler: str | None = None,
    ) -> None: ...
    @property
    def payload_signing_key(self) -> str: ...
    @property
    def network(self) -> _NetworkConfig: ...
    @property
    def failover(self) -> _FailoverConfig: ...
    @property
    def gossip(self) -> _GossipConfig: ...
    @property
    def max_concurrent_tasks(self) -> int: ...
    @property
    def default_task_timeout_ms(self) -> int: ...
    @property
    def drain_timeout_secs(self) -> int: ...
    @property
    def remote_fallback_delay_ms(self) -> int: ...
    @property
    def scheduler(self) -> str: ...

# ---------------------------------------------------------------------------
# Event types — delivered via PyEventBridge (call_soon_threadsafe)
# ---------------------------------------------------------------------------

class _Event:
    @property
    def kind(self) -> str: ...  # "completion" | "orchestration" | "supervision"
    @property
    def completion(self) -> _TaskCompletion | None: ...
    @property
    def orchestration(self) -> _OrchestrationEvent | None: ...
    @property
    def supervision(self) -> _SupervisionEventData | None: ...

class _TaskCompletion:
    @property
    def workflow_id(self) -> str: ...
    @property
    def task_id(self) -> str: ...
    @property
    def task_name(self) -> str: ...
    @property
    def state(self) -> str: ...
    @property
    def result(self) -> bytes | None: ...
    @property
    def error(self) -> str | None: ...
    @property
    def target_node(self) -> str | None: ...

class _OrchestrationEvent:
    @property
    def event_type(self) -> str: ...
    @property
    def workflow_id(self) -> str | None: ...
    @property
    def task_id(self) -> str | None: ...
    @property
    def node_id(self) -> str | None: ...
    @property
    def active_workflows(self) -> list[str] | None: ...
    @property
    def data(self) -> bytes | None: ...
    @property
    def state(self) -> str: ...
    @property
    def available_capacity(self) -> int | None: ...
    @property
    def max_capacity(self) -> int | None: ...

class _RetryInfo:
    @property
    def current_retry_count(self) -> int: ...
    @property
    def max_retries(self) -> int: ...
    @property
    def next_delay_ms(self) -> int: ...

class _DagNode:
    """DAG 节点定义，由 Python 层构造后通过 `submit_dag` 提交。

    `priority` 为有符号整数；语义由 Python 层定义。
    `metadata` 为不透明 key-value 映射，Rust 透传不解释。
    """
    name: str
    payload: bytes
    retry: _RetryPolicy | None
    timeout_ms: int | None
    priority: int | None
    metadata: dict[str, str] | None

    def __init__(
        self,
        name: str,
        payload: bytes,
        retry: _RetryPolicy | None = None,
        timeout_ms: int | None = None,
        priority: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None: ...

class _TaskDef:
    """任务运行时定义：Rust 编排器输出，Python 路由后入队执行。

    `target_node`/`target_endpoint_addr` 由 Python 路由器填写；
    为 `None` 表示本地执行或尚未路由。

    `timeout_ms`/`retry_policy` 必须在路由过程中保留，否则重试任务会
    错误地使用 worker 全局默认值（如默认 30s 超时、无重试），
    导致超时任务被误判为成功完成。
    """
    task_id: str
    name: str
    payload: bytes
    workflow_id: str | None
    target_node: str | None
    target_endpoint_addr: str | None
    timeout_ms: int | None
    retry_policy: _RetryPolicy | None

    def __init__(
        self,
        task_id: str,
        name: str,
        payload: bytes,
        workflow_id: str | None = None,
        target_node: str | None = None,
        target_endpoint_addr: str | None = None,
        timeout_ms: int | None = None,
        retry_policy: _RetryPolicy | None = None,
    ) -> None: ...

class _PeerCapacity:
    """Peer 节点容量快照：可用槽位、最大槽位、endpoint 地址。"""
    available: int
    max: int
    endpoint_addr: str | None

    def __init__(
        self,
        available: int,
        max: int,
        endpoint_addr: str | None = None,
    ) -> None: ...

class _SupervisionEventData:
    @property
    def event_type(self) -> str: ...
    @property
    def actor_id(self) -> str: ...
    @property
    def error(self) -> str | None: ...

class _RuntimeCore:
    """Rust 核心运行时入口，由 Python 编排循环驱动。

    API 分层：
      - 公共方法（无 `_` 前缀）：稳定高层接口，面向典型使用场景。
      - 内部原语（`_` 前缀）：暴露 Rust 编排器/故障转移/gossip 的底层操作，
        供 Python 编排循环（`actant._orchestration`）或自定义编排实现直接驱动状态机。
        这些方法语义可能随 Rust 核心演进而调整，自定义实现需自行承担兼容成本。
    """

    @staticmethod
    def start(
        name: str,
        config: _ActantConfig | None = None,
        node_id: str | None = None,
        tasks: dict[str, Any] | None = None,
    ) -> _RuntimeCore: ...

    # ------------------------------------------------------------------
    # 身份与状态
    # ------------------------------------------------------------------
    def node_id(self) -> str: ...
    def peer_id(self) -> str: ...
    def running_task_count(self) -> int: ...
    def max_concurrent_tasks(self) -> int: ...
    def available_capacity(self) -> int: ...
    def max_capacity(self) -> int: ...
    def get_health_info(self) -> tuple[str, int]: ...
    def get_metrics_snapshot(self) -> dict[str, int]: ...

    # ------------------------------------------------------------------
    # Peer 容量
    # ------------------------------------------------------------------
    def get_peer_capacities(self) -> dict[str, _PeerCapacity]: ...
    def _update_peer_capacity(self, peer_id: str, available: int, max: int) -> None: ...

    # ------------------------------------------------------------------
    # 指标
    # ------------------------------------------------------------------
    def prometheus_text(self) -> str: ...

    # ------------------------------------------------------------------
    # 事件桥（Python 注册回调，Rust 通过 call_soon_threadsafe 上调）
    # ------------------------------------------------------------------
    def set_event_callback(self, callback: Any) -> None: ...

    # ------------------------------------------------------------------
    # DAG 提交与任务入队
    # ------------------------------------------------------------------
    def submit_dag(
        self,
        nodes: list[_DagNode],
        edges: list[tuple[int, int, str | None]],
        #                     ^from  ^to    ^condition_tag
        workflow_timeout_ms: int | None = None,
        default_retry_policy: _RetryPolicy | None = None,
        target_nodes: dict[int, str] | None = None,
        target_endpoint_addrs: dict[int, str] | None = None,
        failure_strategy: str | None = None,
    ) -> _AsyncResultCore: ...
    def enqueue_tasks(self, tasks: list[_TaskDef]) -> None: ...
    def _drain_unrouted_tasks(self) -> list[_TaskDef]: ...
    def scheduler_stats(self) -> int: ...

    # ------------------------------------------------------------------
    # 编排器原语：供 Python 编排循环驱动 DAG 状态机
    # ------------------------------------------------------------------
    def _mark_failed_and_get_retry_info(
        self,
        workflow_id: str,
        task_id: str,
        error: str,
    ) -> _RetryInfo | None: ...
    def _complete_task_and_broadcast(
        self,
        workflow_id: str,
        task_id: str,
        result: bytes,
        task_name: str = "",
    ) -> tuple[list[_TaskDef], list[tuple[str, str]]]: ...
    def _activate_conditional_successor(
        self,
        workflow_id: str,
        task_id: str,
    ) -> _TaskDef | None: ...
    def _skip_conditional_branch(
        self,
        workflow_id: str,
        task_id: str,
    ) -> list[_TaskDef]: ...
    def _broadcast_failure(
        self,
        workflow_id: str,
        task_id: str,
        error: str,
        task_name: str = "",
    ) -> None: ...
    def cancel_workflow(self, workflow_id: str) -> None:
        """Raises NotFoundError if workflow_id does not exist."""
        ...
    def cancel_task(self, workflow_id: str, task_id: str) -> bool:
        """Raises NotFoundError if workflow_id does not exist."""
        ...
    def _mark_workflow_failed(
        self,
        workflow_id: str,
        error: str,
    ) -> None: ...
    def _build_ready_tasks(
        self,
        workflow_id: str,
        task_ids: list[str],
    ) -> list[_TaskDef]: ...
    def _get_retry_info(
        self,
        workflow_id: str,
        task_id: str,
    ) -> _RetryInfo | None: ...
    def _prepare_retry(
        self,
        workflow_id: str,
        task_id: str,
    ) -> _TaskDef | None: ...
    def _mark_task_running(
        self,
        workflow_id: str,
        task_id: str,
    ) -> None: ...
    def _apply_dag_state_update(
        self,
        workflow_id: str,
        task_id: str,
        state: str,
        data: bytes,
    ) -> None: ...
    def _handle_heads_exchange(self, data: bytes) -> None: ...
    def get_stored_results(self, workflow_id: str) -> list[list[bytes]] | None: ...
    def gossip_stats(self) -> tuple[int, int, int, int]: ...
    def _recoverable_workflows_with_pending(self) -> list[tuple[str, list[str]]]: ...

    # ------------------------------------------------------------------
    # 故障转移原语：用于自定义 lease/claim 策略
    # ------------------------------------------------------------------
    def get_peer_infos(self) -> list[tuple[str, int, list[str]]]: ...
    def _detect_failed_nodes(self) -> Coroutine[Any, Any, list[tuple[str, list[str]]]]: ...
    def _should_claim_workflow(self, workflow_ids: list[str]) -> Coroutine[Any, Any, bool]: ...
    def _active_leases(self) -> list[tuple[str, str, int, int]]: ...

    # ------------------------------------------------------------------
    # 网络操作
    # ------------------------------------------------------------------
    def listen_addresses(self) -> Coroutine[Any, Any, dict[str, Any]]: ...
    def dial(self, addr: str) -> Coroutine[Any, Any, None]: ...
    def discover_peers(self) -> Coroutine[Any, Any, None]: ...
    def _add_gossip_peer(self, peer_id: str) -> Coroutine[Any, Any, None]: ...

    # ------------------------------------------------------------------
    # Actor 操作
    # ------------------------------------------------------------------
    def create_actor(self, name: str, dispatcher: Any) -> str: ...
    def create_actor_with_id(self, name: str, actor_id: str, dispatcher: Any) -> str: ...
    def actor_core(self) -> _ActorCore: ...

    # ------------------------------------------------------------------
    # 状态枚举辅助
    # ------------------------------------------------------------------
    def workflow_state_completed(self) -> _WorkflowState: ...
    def workflow_state_failed(self) -> _WorkflowState: ...

    # ------------------------------------------------------------------
    # 工作流与任务状态查询
    # ------------------------------------------------------------------
    def list_workflows(self) -> list[tuple[str, str]]: ...
    def workflow_state(self, workflow_id: str) -> str | None: ...
    def task_states(self, workflow_id: str) -> list[tuple[str, str]] | None: ...

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def shutdown(self, timeout_ms: int | None = None) -> None: ...
    def drain(self) -> None: ...

class _AsyncResultCore:
    @property
    def workflow_id(self) -> str: ...
    def ready(self) -> bool: ...
    def state(self) -> str: ...
    async def get(self, timeout_ms: int | None = None) -> dict[str, Any]: ...
    async def wait_for_completion(self, timeout_ms: int | None = None) -> dict[str, Any]: ...

class _ActorCore:
    async def call_method(
        self,
        actor_id: str,
        method: str,
        payload: bytes,
    ) -> bytes: ...
    def stop_actor(self, actor_id: str) -> None: ...
    def kill_actor(self, actor_id: str) -> None: ...
    def restart_actor(self, actor_id: str, actor_type: str) -> None: ...
    def actor_status(self, actor_id: str) -> str: ...
    def list_actors(self) -> list[str]: ...
