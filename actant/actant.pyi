from __future__ import annotations

from collections.abc import Callable
from typing import Any, final

def get_version() -> str: ...
def refresh_logger() -> None: ...
def prometheus_text() -> str: ...

# Rust-exposed exceptions (register_exceptions in src/py/error.rs)
# 从 actant.exceptions 导入的 Python 端异常类，与 Rust ActantError 一一对应。
class ActantError(RuntimeError):
    kind: str
class StorageError(ActantError): ...
class NetworkError(ActantError): ...
class SerializationError(ActantError): ...
class ActorError(ActantError): ...
class WorkflowError(ActantError): ...
class TaskError(ActantError): ...
class WorkerError(ActantError): ...
class ConfigError(ActantError): ...
class MetricsError(ActantError): ...
class NotFoundError(ActantError): ...
class AlreadyExistsError(ActantError): ...
class ActantTimeoutError(ActantError): ...
class TaskCancelledError(ActantError): ...
class InvalidStateError(ActantError): ...
class InternalError(ActantError): ...
class PayloadTooLargeError(ActantError):
    actual: int
    limit: int
class WorkflowFailedError(ActantError):
    task_name: str | None
    task_error: str | None
class WorkflowCancelledError(ActantError): ...

@final
class CancelToken:
    """Cooperative cancellation flag shared between Rust and Python."""
    def is_cancelled(self) -> bool: ...
    @property
    def cancelled(self) -> bool: ...

@final
class _WorkflowState:
    PENDING: _WorkflowState
    RUNNING: _WorkflowState
    COMPLETED: _WorkflowState
    FAILED: _WorkflowState
    CANCELLED: _WorkflowState
    SKIPPED: _WorkflowState

@final
class _RetryPolicy:
    def __new__(
        cls,
        max_retries: int = 3,
        delay_ms: int = 1000,
        backoff_multiplier: float = 2.0,
        max_delay_ms: int = 60000,
    ) -> _RetryPolicy: ...
    @property
    def max_retries(self) -> int: ...
    @property
    def delay_ms(self) -> int: ...
    @property
    def backoff_multiplier(self) -> float: ...
    @property
    def max_delay_ms(self) -> int: ...
    def to_bytes(self) -> bytes: ...

@final
class _NetworkConfig:
    def __new__(
        cls,
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
        dns_origin_domain: str = "",
    ) -> _NetworkConfig: ...
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
    @property
    def dns_origin_domain(self) -> str:
        """自定义 DNS 起源域，仅当 ``preset = "dns"`` 时生效；空 = n0 默认 ``iroh.link``。"""

@final
class _FailoverConfig:
    def __new__(
        cls,
        heartbeat_interval_ms: int | None = None,
        failure_timeout_ms: int | None = None,
        lease_expiry_check_interval_secs: int | None = None,
        lease_duration_ms: int | None = None,
    ) -> _FailoverConfig: ...
    @property
    def heartbeat_interval_ms(self) -> int: ...
    @property
    def failure_timeout_ms(self) -> int: ...
    @property
    def lease_expiry_check_interval_secs(self) -> int: ...
    @property
    def lease_duration_ms(self) -> int: ...

@final
class _GossipConfig:
    """Gossip 同步配置。所有字段的默认值取自 Rust ``GossipConfig::default()``（单一来源）。"""

    def __new__(
        cls,
        dedup_window_size: int | None = None,
        dedup_ttl_secs: int | None = None,
        retry_attempts: int | None = None,
        retry_base_delay_ms: int | None = None,
        heads_broadcast_interval_ms: int | None = None,
    ) -> _GossipConfig: ...
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

@final
class _ActantConfig:
    """Rust ``ActantConfig`` 的 PyO3 桥。

    Worker 相关默认值（均取自 Rust 侧默认值）：
    - ``max_concurrent_tasks`` 默认 ``num_cpus``；
    - ``num_worker_processes`` 默认跟随 ``max_concurrent_tasks``（保持并发
      信号量与进程池容量一致）；
    - ``crash_failover_max_attempts`` 默认 3；
    - ``workflow_default_timeout_ms`` 默认 3_600_000（``WorkflowConfig``）。
    """

    def __new__(
        cls,
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
        require_payload_signing: bool = False,
        num_worker_processes: int | None = None,
        crash_failover_max_attempts: int | None = None,
        workflow_default_timeout_ms: int | None = None,
    ) -> _ActantConfig: ...
    @property
    def payload_signing_key(self) -> str: ...
    @property
    def require_payload_signing(self) -> bool: ...
    @property
    def network(self) -> _NetworkConfig: ...
    @property
    def failover(self) -> _FailoverConfig: ...
    @property
    def gossip(self) -> _GossipConfig: ...
    @property
    def max_concurrent_tasks(self) -> int: ...
    @property
    def num_worker_processes(self) -> int:
        """worker 子进程数（进程池大小）；``None`` 构造时跟随 ``max_concurrent_tasks``。"""
    @property
    def crash_failover_max_attempts(self) -> int:
        """worker 崩溃后任务重路由的最大执行次数（含首次）。"""
    @property
    def workflow_default_timeout_ms(self) -> int:
        """工作流默认超时（毫秒）。"""
    @property
    def default_task_timeout_ms(self) -> int: ...
    @property
    def drain_timeout_secs(self) -> int: ...
    @property
    def remote_fallback_delay_ms(self) -> int: ...
    @property
    def scheduler(self) -> str: ...


@final
class PyAsyncAwaitable:
    """Rust 内部 awaitable，由 ``future_into_py_iter`` 在无非 asyncio running loop 时返回。

    Python 代码不应直接构造此对象——它仅作为 ``actant`` 模块中异步操作的
    返回值出现，用户通过 ``await`` 消费其结果。在 asyncio 事件循环中运行时，
    实际返回标准 ``asyncio.Future``；本类声明保留以便 mypy 类型推断。
    """

    def __await__(self) -> PyAsyncAwaitable: ...
    def __iter__(self) -> PyAsyncAwaitable: ...
    def __next__(self) -> Any | None: ...


@final
class _Event:
    @property
    def kind(self) -> str: ...  # "completion"
    @property
    def completion(self) -> _TaskCompletion | None: ...

@final
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

@final
class _DagNode:
    """DAG 节点定义，由 Python 层构造后通过 `submit_dag` 提交。

    `task_id` 是节点在 DAG 中的唯一标识（对应 Orchestrator 侧 TaskId），
    由 FlowDAG 记录器设为被提交任务的 task_id；`name` 为人类可读名称。
    `priority` 为有符号整数；语义由 Python 层定义。
    `metadata` 为不透明 key-value 映射，Rust 透传不解释。
    """
    task_id: str
    name: str
    payload: bytes
    retry: _RetryPolicy | None
    timeout_ms: int | None
    priority: int | None
    metadata: dict[str, str] | None

    def __new__(
        cls,
        task_id: str,
        name: str,
        payload: bytes,
        retry: _RetryPolicy | None = None,
        timeout_ms: int | None = None,
        priority: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> _DagNode: ...

@final
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

    def __new__(
        cls,
        task_id: str,
        name: str,
        payload: bytes,
        workflow_id: str | None = None,
        target_node: str | None = None,
        target_endpoint_addr: str | None = None,
        timeout_ms: int | None = None,
        retry_policy: _RetryPolicy | None = None,
    ) -> _TaskDef: ...

@final
class _CapabilityRuntime:
    """Rust `capability::Runtime` 的 PyO3 包装。

    Python `Runtime` 在内部延迟创建此对象，用于执行 Rust 内置 capability
    handler。普通用户直接使用 `actant.Runtime()` 即可。
    """

    def __new__(cls) -> _CapabilityRuntime: ...
    def builtin_capabilities(self) -> list[tuple[str, str]]: ...
    @property
    def capability_count(self) -> int: ...
    def registered_capabilities(self) -> list[str]: ...
    def handler_count(self, name: str) -> int: ...
    def chain_python_handler(self, name: str, handler: Callable[..., Any]) -> None: ...
    def ask(self, name: str, request: Any) -> Any | None: ...
    def perform(self, name: str, request: Any) -> Any: ...
    def emit(self, name: str, request: Any) -> None: ...
    def ask_async(self, name: str, request: Any) -> Any: ...
    def perform_async(self, name: str, request: Any) -> Any: ...
    def perform_batch_async(self, items: list[tuple[str, Any]]) -> Any: ...

@final
class _ListenAddresses:
    """本节点监听地址信息，由 ``_RuntimeCore.listen_addresses()`` 返回。"""

    @property
    def endpoint_id(self) -> str: ...
    @property
    def relay_url(self) -> str | None: ...
    @property
    def direct_addrs(self) -> list[str]: ...
    @property
    def endpoint_addr(self) -> str: ...

@final
class _RuntimeCore:
    """Rust 统一运行时核心的 PyO3 包装。

    由 `Runtime.start()` 创建，聚合网络、存储、Actor 系统、Worker 等子系统。
    `serve()` 在 tokio 后台 spawn worker 守护循环（非阻塞），`shutdown()`
    优雅关闭所有子系统并关闭 iroh endpoint。
    """
    def __new__(
        cls,
        name: str | None = None,
        data_dir: str | None = None,
        config: _ActantConfig | None = None,
    ) -> _RuntimeCore: ...
    def capability_runtime(self) -> _CapabilityRuntime: ...
    def serve(self) -> None: ...
    def shutdown(self, timeout_ms: int = 5000) -> None: ...
    def node_id(self) -> str: ...
    def peer_id(self) -> str: ...
    def listen_addresses(self) -> _ListenAddresses: ...
    def dial(self, addr: str) -> None: ...
    def dial_async(self, addr: str) -> Any: ...
    def add_gossip_peer(self, peer_id: str) -> None: ...
    def add_gossip_peer_async(self, peer_id: str) -> Any: ...
    def discover_peers(self) -> list[str]: ...
    def discover_peers_async(self) -> Any: ...
    def cancel_task(self, task_id: str) -> bool: ...
    def set_max_concurrent_tasks(self, new_max: int) -> None: ...
    def max_concurrent_tasks(self) -> int: ...
    def broadcast_cancel(self, task_id: str, workflow_id: str) -> None: ...
    def submit_task(self, task: _TaskDef) -> None: ...
    def submit_tasks_batch(self, tasks: list[_TaskDef]) -> None:
        """批量提交任务（性能优化路径）：比循环调用 ``submit_task`` 快 10-50x。

        仅在批量场景使用（如 ``gather``）；要求已调用 ``serve()``。
        """
    def submit_dag(
        self,
        workflow_id: str,
        nodes: list[_DagNode],
        edges: list[tuple[str, str]],
        failure_strategy: str | None = None,
        default_retry_policy: _RetryPolicy | None = None,
    ) -> None: ...
    def complete_workflow(self, workflow_id: str, outcomes: list[tuple[str, bool, bytes]]) -> None: ...
    def get_workflow_state(self, workflow_id: str) -> dict[str, Any] | None: ...
    def list_workflows(self) -> list[str]: ...
    def register_task_result_callback(self, callback: Callable[[_TaskCompletion], None]) -> None: ...
    def value_store(self, data: bytes) -> bytes:
        """将字节存入本节点内容寻址 blob 存储，返回 BlobRef wire 编码（0.3.2 R2）。"""
    def value_fetch(self, ref_bytes: bytes) -> bytes:
        """按 BlobRef wire 编码取回值字节：本地命中优先，未命中跨节点流式拉取。"""
    def value_ref_parts(self, ref_bytes: bytes) -> tuple[str, str]:
        """解码 BlobRef wire 编码为 (hash_hex, node)。"""
