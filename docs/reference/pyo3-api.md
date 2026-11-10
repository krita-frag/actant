# PyO3 绑定 API

> 本页由 `scripts/gen_api_reference.py` 从源码生成，请勿手工编辑。

本页覆盖 Python 可直接导入的 `actant.actant` 接口。纯 Python 公共 API 见 [Python API](python-api.md)，配置摘要见 [配置项](configuration.md)，命令行见 [CLI](cli.md)。

## 函数

```python
def get_version() -> str
```

```python
def refresh_logger() -> None
```

```python
def metrics_text() -> str
```

## 类

### `ActantError` (RuntimeError)

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `kind` | `str` |  |

### `PayloadTooLargeError` (ActantError)

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `actual` | `int` |  |
| `limit` | `int` |  |

### `WorkflowFailedError` (ActantError)

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `task_name` | `str \| None` |  |
| `task_error` | `str \| None` |  |

### `_WorkflowState`

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `PENDING` | `_WorkflowState` |  |
| `RUNNING` | `_WorkflowState` |  |
| `COMPLETED` | `_WorkflowState` |  |
| `FAILED` | `_WorkflowState` |  |
| `CANCELLED` | `_WorkflowState` |  |

### `_RetryPolicy`

**构造**

```python
_RetryPolicy(max_retries: int = 3, delay_ms: int = 1000, backoff_multiplier: float = 2.0, max_delay_ms: int = 60000)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `max_retries` | `int` |  |
| `delay_ms` | `int` |  |
| `backoff_multiplier` | `float` |  |
| `max_delay_ms` | `int` |  |

**方法**

```python
def default(cls) -> _RetryPolicy
```

```python
def to_bytes(self) -> bytes
```

### `_NetworkConfig`

**构造**

```python
_NetworkConfig(preset: str | None = None, bootstrap_nodes: list[str] | None = None, hlc_max_drift_ms: int = 500, max_pending_direct_requests: int = 1024, gossip_bootstrap_peers: list[str] | None = None, max_message_size: int = 16777216, allowed_peer_ids: list[str] | None = None, direct_request_timeout_ms: int = 30000, listen_port: int = 0, listen_ip: str = '', event_channel_capacity: int = 1024, dns_origin_domain: str = '', relay_endpoints: list[str] | None = None, require_signed_records: bool = False)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `preset` | `str` |  |
| `bootstrap_nodes` | `list[str]` |  |
| `hlc_max_drift_ms` | `int` |  |
| `max_pending_direct_requests` | `int` |  |
| `gossip_bootstrap_peers` | `list[str]` |  |
| `max_message_size` | `int` |  |
| `allowed_peer_ids` | `list[str]` |  |
| `direct_request_timeout_ms` | `int` |  |
| `listen_port` | `int` |  |
| `listen_ip` | `str` |  |
| `event_channel_capacity` | `int` |  |
| `dns_origin_domain` | `str` | 自定义 DNS 起源域，仅当 ``preset = "dns"`` 时生效；空 = n0 默认 ``iroh.link``。 |
| `relay_endpoints` | `list[str]` | 自定义 relay 集群 URL 列表；非空时覆盖 preset 自带的 relay 配置。 |
| `require_signed_records` | `bool` | 强制校验心跳节点记录签名。 |

### `_FailoverConfig`

**构造**

```python
_FailoverConfig(heartbeat_interval_ms: int | None = None, failure_timeout_ms: int | None = None, lease_expiry_check_interval_secs: int | None = None, lease_duration_ms: int | None = None)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `heartbeat_interval_ms` | `int` |  |
| `failure_timeout_ms` | `int` |  |
| `lease_expiry_check_interval_secs` | `int` |  |
| `lease_duration_ms` | `int` |  |

### `_GossipConfig`

Gossip 同步配置。所有字段的默认值取自 Rust ``GossipConfig::default()``（单一来源）。

**构造**

```python
_GossipConfig(dedup_window_size: int | None = None, dedup_ttl_secs: int | None = None, retry_attempts: int | None = None, retry_base_delay_ms: int | None = None, heads_broadcast_interval_ms: int | None = None)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `dedup_window_size` | `int` |  |
| `dedup_ttl_secs` | `int` |  |
| `retry_attempts` | `int` |  |
| `retry_base_delay_ms` | `int` |  |
| `heads_broadcast_interval_ms` | `int` |  |

### `_StoreConfig`

本地持久化配置（LMDB）。

``map_size`` 是 mmap 虚拟地址上限，不预分配磁盘；``sync_mode`` 取
``"sync"`` / ``"group_commit"`` / ``"no_sync"``，``flush_interval_ms``
仅在 ``"group_commit"`` 下生效。

**构造**

```python
_StoreConfig(data_dir: str | None = None, map_size: int | None = None, max_dbs: int | None = None, sync_mode: str | None = None, flush_interval_ms: int | None = None)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `data_dir` | `str \| None` |  |
| `map_size` | `int` |  |
| `max_dbs` | `int` |  |
| `sync_mode` | `str` |  |
| `flush_interval_ms` | `int` |  |

### `_ActantConfig`

Rust ``ActantConfig`` 的 PyO3 桥。

Worker 相关默认值（均取自 Rust 侧默认值）：
- ``max_concurrent_tasks`` 默认 ``num_cpus``；
- ``num_worker_processes`` 默认跟随 ``max_concurrent_tasks``（保持并发
  信号量与进程池容量一致）；
- ``crash_failover_max_attempts`` 默认 3。

**构造**

```python
_ActantConfig(payload_signing_key: str, network: _NetworkConfig | None = None, failover: _FailoverConfig | None = None, gossip: _GossipConfig | None = None, max_concurrent_tasks: int | None = None, default_task_timeout_ms: int | None = None, drain_timeout_secs: int | None = None, remote_fallback_delay_ms: int | None = None, scheduler: str | None = None, require_payload_signing: bool = False, num_worker_processes: int | None = None, crash_failover_max_attempts: int | None = None, node_labels: dict[str, str] | None = None, *, store: _StoreConfig | None = None, prefetch_min: int | None = None, prefetch_max: int | None = None, worker_cancel_grace_ms: int | None = None, pending_result_channel_capacity: int | None = None, completed_retention_count: int | None = None, max_history_events: int | None = None, persist_flush_interval_ms: int | None = None, state_poll_interval_ms: int | None = None, starvation_timeout_ms: int | None = None, node_quotas: dict[str, int] | None = None, role: str | None = None)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `payload_signing_key` | `str` |  |
| `require_payload_signing` | `bool` |  |
| `network` | `_NetworkConfig` |  |
| `failover` | `_FailoverConfig` |  |
| `gossip` | `_GossipConfig` |  |
| `max_concurrent_tasks` | `int` |  |
| `num_worker_processes` | `int` | worker 子进程数（进程池大小）；``None`` 构造时跟随 ``max_concurrent_tasks``。 |
| `crash_failover_max_attempts` | `int` | worker 崩溃后任务重路由的最大执行次数（含首次）。 |
| `default_task_timeout_ms` | `int` |  |
| `drain_timeout_secs` | `int` |  |
| `remote_fallback_delay_ms` | `int` |  |
| `starvation_timeout_ms` | `int` | 任务无匹配节点（Defer）时最长等待毫秒数，超时转 Failed；0 表示不设上限。 |
| `scheduler` | `str` |  |
| `node_labels` | `dict[str, str]` | 用户自定义节点标签，随心跳广播给集群。 |
| `role` | `str` | 节点拓扑角色：``"both"``（默认）/ ``"orchestrator"`` / ``"worker"``。 |
| `node_quotas` | `dict[str, int]` | 本节点命名配额容量（如 ``{"gpu": 2}``），随心跳广播剩余容量。 |
| `store` | `_StoreConfig` | 本地持久化配置（目录 / mmap 上限 / 同步策略）。 |
| `prefetch_min` | `int` |  |
| `prefetch_max` | `int` |  |
| `worker_cancel_grace_ms` | `int` |  |
| `pending_result_channel_capacity` | `int` |  |
| `completed_retention_count` | `int` |  |
| `max_history_events` | `int` |  |
| `persist_flush_interval_ms` | `int` |  |
| `state_poll_interval_ms` | `int` |  |

### `PyAsyncAwaitable`

Rust 内部 awaitable，由 ``future_into_py_iter`` 在无非 asyncio running loop 时返回。

Python 代码不应直接构造此对象——它仅作为 ``actant`` 模块中异步操作的
返回值出现，用户通过 ``await`` 消费其结果。在 asyncio 事件循环中运行时，
实际返回标准 ``asyncio.Future``；本类声明保留以便 mypy 类型推断。

### `_TaskCompletion`

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `workflow_id` | `str` |  |
| `task_id` | `str` |  |
| `task_name` | `str` |  |
| `state` | `str` |  |
| `result` | `bytes \| None` |  |
| `error` | `str \| None` |  |
| `target_node` | `str \| None` |  |

### `_DagNode`

DAG 节点定义，由 Python 层构造后通过 `add_workflow_node` 提交。

`task_id` 是节点在 DAG 中的唯一标识（对应 Orchestrator 侧 TaskId），
flow 路径下由提交序号确定性生成（重放身份）；`name` 为人类可读名称。
`priority` 为有符号整数；语义由 Python 层定义。
`metadata` 为不透明 key-value 映射，Rust 透传不解释。
`tags`/`quota` 为放置诉求，随节点派发为 `TaskDefinition` 的对应字段。

**构造**

```python
_DagNode(task_id: str, name: str, payload: bytes, timeout_ms: int | None = None, priority: int | None = None, tags: list[str] | None = None, quota: str | None = None)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `task_id` | `str` |  |
| `name` | `str` |  |
| `payload` | `bytes` |  |
| `retry` | `_RetryPolicy \| None` |  |
| `timeout_ms` | `int \| None` |  |
| `priority` | `int \| None` |  |
| `metadata` | `dict[str, str] \| None` |  |
| `tags` | `list[str]` |  |
| `quota` | `str \| None` |  |

### `_TaskDef`

任务运行时定义：Rust 编排器输出，Python 路由后入队执行。

`target_node`/`target_endpoint_addr` 由 Python 路由器填写；
为 `None` 表示本地执行或尚未路由。

`timeout_ms`/`retry_policy` 必须在路由过程中保留，否则重试任务会
错误地使用 worker 全局默认值（如默认 30s 超时、无重试），
导致超时任务被误判为成功完成。

**构造**

```python
_TaskDef(task_id: str, name: str, payload: bytes, workflow_id: str | None = None, target_node: str | None = None, target_endpoint_addr: str | None = None, timeout_ms: int | None = None, tags: list[str] | None = None, quota: str | None = None, priority: int | None = None, retry_policy: _RetryPolicy | None = None)
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `task_id` | `str` |  |
| `name` | `str` |  |
| `payload` | `bytes` |  |
| `workflow_id` | `str \| None` |  |
| `target_node` | `str \| None` |  |
| `target_endpoint_addr` | `str \| None` |  |
| `timeout_ms` | `int \| None` |  |
| `tags` | `list[str]` |  |
| `quota` | `str \| None` |  |
| `priority` | `int` |  |
| `pinned_target` | `bool` |  |

### `_CapabilityRuntime`

Rust `capability::Runtime` 的 PyO3 包装。

Python `Runtime` 在内部延迟创建此对象，用于执行 Rust 内置 capability
handler。普通用户直接使用 `actant.Runtime()` 即可。

**构造**

```python
_CapabilityRuntime()
```

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `capability_count` | `int` |  |

**方法**

```python
def builtin_capabilities(self) -> list[tuple[str, str]]
```

```python
def registered_capabilities(self) -> list[str]
```

```python
def ask(self, name: str, request: Any) -> Any | None
```

```python
def perform(self, name: str, request: Any) -> Any
```

```python
def emit(self, name: str, request: Any) -> None
```

```python
def ask_async(self, name: str, request: Any) -> Any
```

```python
def perform_async(self, name: str, request: Any) -> Any
```

```python
def perform_batch_async(self, items: list[tuple[str, Any]]) -> Any
```

### `_ListenAddresses`

本节点监听地址信息，由 ``_RuntimeCore.listen_addresses()`` 返回。

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `endpoint_id` | `str` |  |
| `relay_url` | `str \| None` |  |
| `direct_addrs` | `list[str]` |  |
| `endpoint_addr` | `str` |  |

### `_StoreStats`

本地存储容量快照（``_RuntimeCore.storage_stats()`` 的返回值）。

``file_bytes / map_size`` 是 LMDB 写满判断口径；``used_bytes`` 是不含
空闲页的实际数据占用。

**属性**

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `map_size` | `int` |  |
| `used_bytes` | `int` |  |
| `file_bytes` | `int` |  |
| `entries` | `int` |  |
| `readers` | `int` |  |
| `max_readers` | `int` |  |
| `utilization_ratio` | `float` | 数据文件高水位占 mmap 上限的比例（``file_bytes / map_size``）。 |

### `_RuntimeCore`

Rust 统一运行时核心的 PyO3 包装。

由 `Runtime.start()` 创建，聚合网络、存储、Actor 系统、Worker 等子系统。
`serve()` 在 tokio 后台 spawn worker 守护循环（非阻塞），`shutdown()`
优雅关闭所有子系统并关闭 iroh endpoint。

**构造**

```python
_RuntimeCore(name: str | None = None, data_dir: str | None = None, config: _ActantConfig | None = None, route_policy: str | Callable[[dict[str, Any], list[dict[str, Any]]], Any] | None = None)
```

**方法**

```python
def capability_runtime(self) -> _CapabilityRuntime
```

```python
def serve(self) -> None
```

```python
def shutdown(self, timeout_ms: int = 5000) -> None
```

```python
def node_id(self) -> str
```

```python
def peer_id(self) -> str
```

```python
def listen_addresses(self) -> _ListenAddresses
```

```python
def storage_stats(self) -> _StoreStats
```

返回本地存储容量与使用情况快照；启动时已同步上报一次 ``actant.store.*`` 指标。

```python
def backup_to(self, path: str) -> int
```

在线热备份本地存储到 ``path``，返回备份文件字节数。

```python
def dial(self, addr: str) -> None
```

```python
def add_gossip_peer(self, peer_id: str) -> None
```

```python
def discover_peers(self) -> list[str]
```

```python
def close_peer(self, target: str) -> None
```

```python
def reopen_peer(self, target: str) -> None
```

```python
def delay_heartbeat(self, ms: int) -> None
```

```python
def inject_hlc_drift(self, ms: int) -> None
```

```python
def cancel_task(self, task_id: str) -> bool
```

```python
def update_node_state(self, labels: dict[str, str] | None = None, quotas: dict[str, int] | None = None, draining: bool | None = None) -> None
```

运行时更新节点状态（标签 / 配额容量 / draining）。

```python
def set_max_concurrent_tasks(self, new_max: int) -> None
```

```python
def max_concurrent_tasks(self) -> int
```

```python
def broadcast_cancel(self, task_id: str, workflow_id: str) -> None
```

```python
def submit_task(self, task: _TaskDef) -> None
```

```python
def submit_tasks_batch(self, tasks: list[_TaskDef]) -> None
```

批量提交任务（性能优化路径）：比循环调用 ``submit_task`` 快 10-50x。

仅在批量场景使用（如 ``gather``）；要求已调用 ``serve()``。

```python
def register_batch(self, batch_id: str, task_ids: list[str]) -> dict[str, Any]
```

注册 batch/array job，返回初始进度快照。

```python
def batch_progress(self, batch_id: str) -> dict[str, Any] | None
```

查询 batch 进度；未注册返回 ``None``。

```python
def record_batch_terminal(self, task_id: str, state: str) -> dict[str, Any] | None
```

记录 batch 成员终态（completed/failed/cancelled）。

```python
def remove_batch(self, batch_id: str) -> bool
```

移除 batch 注册与成员索引。

```python
def submit_workflow(self, workflow_id: str, failure_strategy: str | None = None, timeout_ms: int = 0, name: str | None = None, version: int = 0, args: bytes | None = None) -> None
```

创建空持久化工作流外壳（flow 提交路径第一步）。

``name`` 非空时把提交描述（flow 名、版本、入参字节）
作为 ``WorkflowInvocation`` 写入 journal 的 ``Submitted`` 事件；其余提交
路径不伪造调用描述。

```python
def add_workflow_node(self, workflow_id: str, node: _DagNode, deps: list[str]) -> dict[str, Any]
```

增量加入单个 DAG 节点（先持久化再派发；重放命中时返回历史状态）。

返回 ``{"created": bool, "state": str | None, "result": bytes | None,
"error": str | None}``。

```python
def cancel_workflow(self, workflow_id: str) -> None
```

```python
def seal_workflow(self, workflow_id: str) -> None
```

封口工作流节点集（flow 函数体返回信号）。

封口后 orchestrator 才允许工作流终态判定；全部任务已终态时立即收尾。

```python
def register_wait_point(self, workflow_id: str, wait_key: str, kind: str, name: str | None = None, deadline_ms: int = 0) -> None
```

注册持久化等待点（先注册、后 `wait_wait_point`）。

`kind` 为 ``"signal"`` / ``"timer"`` / ``"suspend"``：signal 等具名业务
事件，timer 须给绝对 epoch 毫秒 `deadline_ms`（> 0），suspend 等
`resume_suspended`。同 `wait_key` 重复注册为 no-op（不改写条件、不重复
追加事件），这是重放体幂等的前提。

抛 ``ValueError``（kind 非法或 timer 的 deadline_ms 为 0）与
``NotFoundError``（工作流不存在）。

```python
def signal_wait_point(self, workflow_id: str, wait_key: str, payload: bytes = b'') -> bytes | None
```

递交信号唤醒等待点（信号缓冲：注册前抵达的信号进入缓冲，不丢弃）。

``payload`` 原样随信号进入工作流历史，``wait_wait_point`` 返回同一份字节；
返回唤醒的 payload；``None`` 表示**此刻没有等待点可被唤醒**，信号已入
缓冲，将来注册同一 `wait_key` 时立即命中。**未知工作流**抛
``NotFoundError``；**工作流已终态不报错**（递交方重试时会撞上刚跑完的
终态，报错就等于"明明送到了却报错"）。

```python
def resume_suspended(self, workflow_id: str) -> int
```

恢复挂起：唤醒该工作流所有等待中的 `suspend` 等待点。
返回唤醒数量；``0`` 表示当前没有挂起中的挂起点（幂等）。只唤醒
`suspend` 条件，不触碰 `signal` / `timer` 等待点。

```python
def resolve_context_value(self, workflow_id: str, key: str, kind: str, candidate: bytes) -> bytes
```

解析（首次记录 / 重放命中）确定性上下文值。

首次执行返回 ``candidate`` 并写入历史；重放返回历史记录值。同 ``key``
的历史记录是另一种 ``kind`` 时抛 ``FlowReplayError``。

```python
def get_context_value(self, workflow_id: str, key: str) -> bytes | None
```

只读查询已记录的上下文值；未记录返回 ``None``（不写历史）。

```python
def wait_wait_point(self, workflow_id: str, wait_key: str, timeout_ms: int = 0) -> bytes | None
```

阻塞等待等待点条件满足（flow 体 park 原语）。

调用方必须先 `register_wait_point`；本方法只负责 park，且不在 actor
消息循环内阻塞（不会拖停其他工作流）。``timeout_ms=0`` 表示无限等待。

返回条件满足时的 payload；``None`` 表示超时，或等待期间工作流被移除 /
编排器未注入。

```python
def wait_wait_point_async(self, workflow_id: str, wait_key: str, timeout_ms: int = 0) -> Awaitable[bytes | None]
```

非阻塞等待等待点条件满足（asyncio 版本）。

语义与 ``wait_wait_point`` 一致，但返回 awaitable，等待期间不占用线程、
不阻塞 event loop。

```python
def report_task_result(self, workflow_id: str, task_id: str, state: str, result: bytes | None = None, error: str | None = None) -> dict[str, Any]
```

上报本地 flow 任务终态结果（Python 事件泵调用）。

`workflow_id` 为空串表示独立 ``@task``（非编排任务），直接返回空裁决。
`state` 为 ``"Completed"`` / ``"Failed"`` / ``"Cancelled"``；返回
``{"retry": bool, "delay_ms": int}``——``retry`` 为真表示 orchestrator
已排定重试并按延迟重新入队，提交方句柄应继续等待；为假表示终局。

```python
def get_workflow_state(self, workflow_id: str) -> dict[str, Any] | None
```

```python
def get_dag(self, workflow_id: str) -> dict[str, Any] | None
```

查询工作流 DAG **结构**：节点 / 依赖边 / 重试策略。

与 `get_workflow_state`（**执行**状态）分工：本方法是**结构**。
返回 ``None``（工作流不存在）或含 ``nodes`` / ``edges`` /
``default_retry_policy`` / ``failure_strategy`` 的 dict。**不含任务
payload**（对 Rust 不透明且可能很大）——需要结果查 `get_workflow_state`。

```python
def get_workflow_history(self, workflow_id: str, *, after: tuple[int, int] | None = None) -> list[dict[str, Any]]
```

读取工作流事件历史（审计出口）。

每项 ``{sequence, timestamp_ms, kind, task_id, error, payload}``；``kind``
/ ``task_id`` / ``error`` 供筛选，``payload`` 为不透明字节。`after` 是
``(sequence, timestamp_ms)`` 游标，``None`` 从头读取。

```python
def get_workflow_journal(self, workflow_id: str) -> list[dict[str, Any]]
```

读取工作流的稳定 journal（跨语言消费面）。

每项固定含 ``seq`` / ``timestamp_ms`` / ``kind`` / ``workflow_id`` /
``task_id`` / ``wait_key`` / ``error`` / ``payload``，并按事件类型附带
``condition``（等待点标签）与 ``invocation``（提交描述 dict / ``None``）。
``kind`` 为小写稳定事件名；``payload`` 是该事件的业务字节。

```python
def get_workflow_invocation(self, workflow_id: str) -> dict[str, Any] | None
```

读取 ``Submitted`` 事件中的提交描述。

返回 ``{workflow_id, name, version, args}``；
旧记录或非 flow 提交路径返回 ``None``。``args`` 是上层序列化字节。

```python
def list_workflows(self) -> list[str]
```

```python
def list_all_workflows(self) -> list[str]
```

```python
def peers(self) -> list[dict[str, Any]]
```

```python
def delete_workflow(self, workflow_id: str) -> None
```

删除工作流及其持久化状态；不存在时为 no-op。

与自动淘汰互补，供手动清理已完成 / 孤儿工作流。运行中的工作流删除后
其任务结果将不可恢复。

```python
def register_task_log_callback(self, callback: Callable[[dict[str, Any]], None], task_id: str | None = None) -> None
```

```python
def register_worker_state_callback(self, callback: Callable[[dict[str, Any]], None]) -> None
```

```python
def register_task_result_callback(self, callback: Callable[[_TaskCompletion], None]) -> None
```

```python
def value_store(self, data: bytes) -> bytes
```

将字节存入本节点内容寻址 blob 存储，返回 BlobRef wire 编码。

```python
def value_fetch(self, ref_bytes: bytes) -> bytes
```

按 BlobRef wire 编码取回值字节：本地命中优先，未命中跨节点流式拉取。

```python
def value_ref_parts(self, ref_bytes: bytes) -> tuple[str, str]
```

解码 BlobRef wire 编码为 (hash_hex, node)。

## 其他类型

| 类型 | 基类 |
| --- | --- |
| `StorageError` | ActantError |
| `NetworkError` | ActantError |
| `SerializationError` | ActantError |
| `ActorError` | ActantError |
| `WorkflowError` | ActantError |
| `TaskError` | ActantError |
| `WorkerError` | ActantError |
| `ConfigError` | ActantError |
| `MetricsError` | ActantError |
| `NotFoundError` | ActantError |
| `AlreadyExistsError` | ActantError |
| `ActantTimeoutError` | ActantError |
| `TaskCancelledError` | ActantError |
| `InvalidStateError` | ActantError |
| `InternalError` | ActantError |
| `WorkflowCancelledError` | ActantError |
| `FlowReplayError` | ActantError |
