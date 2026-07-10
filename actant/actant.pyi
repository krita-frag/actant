from __future__ import annotations

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
        capability_gossip_interval_ms: int = 5000,
        event_channel_capacity: int = 256,
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
    @property
    def capability_gossip_interval_ms(self) -> int: ...
    @property
    def event_channel_capacity(self) -> int: ...

class _FailoverConfig:
    def __init__(
        self,
        heartbeat_interval_ms: int | None = None,
        failure_timeout_ms: int | None = None,
        lease_expiry_check_interval_secs: int | None = None,
        lease_duration_ms: int | None = None,
    ) -> None: ...
    @property
    def heartbeat_interval_ms(self) -> int: ...
    @property
    def failure_timeout_ms(self) -> int: ...
    @property
    def lease_expiry_check_interval_secs(self) -> int: ...
    @property
    def lease_duration_ms(self) -> int: ...

class _GossipConfig:
    def __init__(
        self,
        dedup_window_size: int = 1024,
        dedup_ttl_secs: int = 300,
        retry_attempts: int = 3,
        retry_base_delay_ms: int = 100,
        heads_broadcast_interval_ms: int = 30000,
    ) -> None: ...
    @property
    def dedup_window_size(self) -> int: ...
    @property
    def dedup_ttl_secs(self) -> int: ...
    @property
    def retry_attempts(self) -> int: ...
    @property
    def retry_base_delay_ms(self) -> int: ...
    @property
    def heads_broadcast_interval_ms(self) -> int: ...

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

class _CapabilityRuntime:
    """Rust `capability::Runtime` 的 PyO3 包装。

    Python `Runtime` 在内部延迟创建此对象，用于执行 Rust 内置 capability
    handler。普通用户直接使用 `actant.Runtime()` 即可。
    """

    def __init__(self) -> None: ...
    def builtin_capabilities(self) -> list[tuple[str, str]]: ...
    @property
    def capability_count(self) -> int: ...
    def registered_capabilities(self) -> list[str]: ...
    def handler_count(self, name: str) -> int: ...
    def ask(self, name: str, request: Any) -> Any | None: ...
    def perform(self, name: str, request: Any) -> Any: ...
    def emit(self, name: str, request: Any) -> None: ...

class _SupervisionEventData:
    @property
    def event_type(self) -> str: ...
    @property
    def actor_id(self) -> str: ...
    @property
    def error(self) -> str | None: ...

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

class _RuntimeCore:
    """Rust 统一运行时核心的 PyO3 包装。

    由 `Runtime.start()` 创建，聚合网络、存储、Actor 系统、Worker 等子系统。
    `serve()` 在 tokio 后台 spawn worker 守护循环（非阻塞），`shutdown()`
    优雅关闭所有子系统并关闭 iroh endpoint。
    """
    def __init__(
        self,
        name: str | None = None,
        data_dir: str | None = None,
        config: _ActantConfig | None = None,
    ) -> None: ...
    def capability_runtime(self) -> _CapabilityRuntime: ...
    def serve(self) -> None: ...
    def shutdown(self, timeout_ms: int = 5000) -> None: ...
    def node_id(self) -> str: ...
