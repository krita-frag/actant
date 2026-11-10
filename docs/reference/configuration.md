# 配置项

> 本页由 `scripts/gen_api_reference.py` 从源码生成，请勿手工编辑。

字段名与默认值来自 `actant/actant.pyi` 的构造签名。运行时行为说明见 [Worker 运行模型](../architecture/worker-model.md) 与 [部署与发现](../guide/deployment.md)。

## `_ActantConfig`

Rust ``ActantConfig`` 的 PyO3 桥。

Worker 相关默认值（均取自 Rust 侧默认值）：
- ``max_concurrent_tasks`` 默认 ``num_cpus``；
- ``num_worker_processes`` 默认跟随 ``max_concurrent_tasks``（保持并发
  信号量与进程池容量一致）；
- ``crash_failover_max_attempts`` 默认 3。

| 参数 | 类型 | 默认值 |
| --- | --- | --- |
| `payload_signing_key` | `str` | `必填` |
| `network` | `_NetworkConfig \| None` | `None` |
| `failover` | `_FailoverConfig \| None` | `None` |
| `gossip` | `_GossipConfig \| None` | `None` |
| `max_concurrent_tasks` | `int \| None` | `None` |
| `default_task_timeout_ms` | `int \| None` | `None` |
| `drain_timeout_secs` | `int \| None` | `None` |
| `remote_fallback_delay_ms` | `int \| None` | `None` |
| `scheduler` | `str \| None` | `None` |
| `require_payload_signing` | `bool` | `False` |
| `num_worker_processes` | `int \| None` | `None` |
| `crash_failover_max_attempts` | `int \| None` | `None` |
| `node_labels` | `dict[str, str] \| None` | `None` |
| `store` | `_StoreConfig \| None` | `None` |
| `prefetch_min` | `int \| None` | `None` |
| `prefetch_max` | `int \| None` | `None` |
| `worker_cancel_grace_ms` | `int \| None` | `None` |
| `pending_result_channel_capacity` | `int \| None` | `None` |
| `completed_retention_count` | `int \| None` | `None` |
| `max_history_events` | `int \| None` | `None` |
| `persist_flush_interval_ms` | `int \| None` | `None` |
| `state_poll_interval_ms` | `int \| None` | `None` |
| `starvation_timeout_ms` | `int \| None` | `None` |
| `node_quotas` | `dict[str, int] \| None` | `None` |
| `role` | `str \| None` | `None` |

## `_NetworkConfig`

| 参数 | 类型 | 默认值 |
| --- | --- | --- |
| `preset` | `str \| None` | `None` |
| `bootstrap_nodes` | `list[str] \| None` | `None` |
| `hlc_max_drift_ms` | `int` | `500` |
| `max_pending_direct_requests` | `int` | `1024` |
| `gossip_bootstrap_peers` | `list[str] \| None` | `None` |
| `max_message_size` | `int` | `16777216` |
| `allowed_peer_ids` | `list[str] \| None` | `None` |
| `direct_request_timeout_ms` | `int` | `30000` |
| `listen_port` | `int` | `0` |
| `listen_ip` | `str` | `''` |
| `event_channel_capacity` | `int` | `1024` |
| `dns_origin_domain` | `str` | `''` |
| `relay_endpoints` | `list[str] \| None` | `None` |
| `require_signed_records` | `bool` | `False` |

## `_FailoverConfig`

| 参数 | 类型 | 默认值 |
| --- | --- | --- |
| `heartbeat_interval_ms` | `int \| None` | `None` |
| `failure_timeout_ms` | `int \| None` | `None` |
| `lease_expiry_check_interval_secs` | `int \| None` | `None` |
| `lease_duration_ms` | `int \| None` | `None` |

## `_GossipConfig`

Gossip 同步配置。所有字段的默认值取自 Rust ``GossipConfig::default()``（单一来源）。

| 参数 | 类型 | 默认值 |
| --- | --- | --- |
| `dedup_window_size` | `int \| None` | `None` |
| `dedup_ttl_secs` | `int \| None` | `None` |
| `retry_attempts` | `int \| None` | `None` |
| `retry_base_delay_ms` | `int \| None` | `None` |
| `heads_broadcast_interval_ms` | `int \| None` | `None` |

## `_RetryPolicy`

| 参数 | 类型 | 默认值 |
| --- | --- | --- |
| `max_retries` | `int` | `3` |
| `delay_ms` | `int` | `1000` |
| `backoff_multiplier` | `float` | `2.0` |
| `max_delay_ms` | `int` | `60000` |

## `_StoreConfig`

本地持久化配置（LMDB）。

``map_size`` 是 mmap 虚拟地址上限，不预分配磁盘；``sync_mode`` 取
``"sync"`` / ``"group_commit"`` / ``"no_sync"``，``flush_interval_ms``
仅在 ``"group_commit"`` 下生效。

| 参数 | 类型 | 默认值 |
| --- | --- | --- |
| `data_dir` | `str \| None` | `None` |
| `map_size` | `int \| None` | `None` |
| `max_dbs` | `int \| None` | `None` |
| `sync_mode` | `str \| None` | `None` |
| `flush_interval_ms` | `int \| None` | `None` |

## 环境变量

| 变量 | 读取位置 | 作用 |
| --- | --- | --- |
| `ACTANT_DISCOVERY` | `src/py/config.rs` | 覆盖 `network.preset`；CI 用 `ACTANT_DISCOVERY=none` 强制离线 |
| `ACTANT_PAYLOAD_KEY` | `actant/cli.py` | `actant worker` 的 payload 签名密钥 |
| `ACTANT_TRACING` | `crates/actant-core/src/observability.rs` | 设为 `1` 启用 Rust `tracing` 结构化日志 |
| `ACTANT_TOKIO_CONSOLE` | `crates/actant-core/src/observability.rs` | 设为 `1` 启用 tokio-console（需 `tokio-console` feature） |

测试专用：`ACTANT_SKIP_BUILD_CHECK=1` 跳过原生扩展新鲜度检查，见 [测试](../contributing/testing.md)。
