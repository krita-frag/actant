# 部署与发现

## 节点身份

节点身份 = iroh endpoint keypair，随 `data_dir/identity.key` 持久化（权限 0600）。endpoint id 跨重启稳定。

不显式给 `data_dir` 时使用临时目录，节点身份不持久；生产部署应显式指定。

## 存储

`data_dir` 下除 `identity.key` 与 blob 数据外，还有两套独立的 LMDB 环境：
`store/`（业务状态、事件日志、检查点）与 `orchestrator/`（编排器状态）。
两者都是进程内嵌存储，不跨节点共享，节点间状态通过 gossip / lease 协同。

- **仅支持本地块设备**（本地 SSD 或云盘挂载）。不要放在 NFS / SMB / EFS 或
  Kubernetes RWX PVC 上：LMDB 依赖 mmap 与 POSIX 文件锁，网络文件系统会导致
  锁语义失效甚至静默损坏。K8s 请使用 local PV 或 hostPath。
- 同一 `data_dir` 只允许一个进程打开；`lock.mdb` 会拒绝第二个进程。
- `store.map_size`（Python 侧 `_StoreConfig(map_size=...)`）是 mmap 虚拟地址上限，
  默认 32 GiB，不预分配磁盘。实际占用随数据增长；使用率超过 75% 时启动与统计
  采样会打印 warn，并上报 `actant.store.utilization_ratio`。写满时返回带处置建议
  的语义化错误，处理方式是调大 `store.map_size` 后重启节点。
- 存储指标：`actant.store.map_size_bytes`、`actant.store.used_bytes`、
  `actant.store.file_bytes`、`actant.store.utilization_ratio`、
  `actant.store.readers`、`actant.store.max_readers`、
  `actant.store.capacity_errors`。运维侧可周期性调用 `Store::storage_stats()`
  刷新快照。
- Capability 指标：`actant.capability.invocations`（Prometheus 名为
  `actant_capability_invocations_total`）按 `capability` / `effect`
  （`ask` / `perform` / `emit`）统计公共 effect 入口的调用次数，用于定位
  高频业务 capability 与异常重试。Rust 内部分派、以及框架内部经 Store /
  ValueStore 的持久化与 payload offload 读写都不计入，避免内部流量淹没业务
  计数。
- 备份：调用 `Store::backup_to(path)` 做在线热备份；备份带 compaction，会回收
  长期运行积累的空闲页。恢复时停止节点，用备份文件替换目标环境的 `data.mdb`
  后重新启动。

## 发现 preset

通过 `_NetworkConfig(preset=...)` 配置。内置 3 种：

| preset | 用途 | 行为 |
| --- | --- | --- |
| `none` | 测试 / CI / 单节点 | 仅 loopback，无 DNS Pkarr，无 relay；须通过 `bootstrap_nodes` 显式拨号 |
| `local` | 互联网节点通用模式（默认） | iroh N0 preset：DNS Pkarr 发现 + relay 兜底 |
| `dns` | 仅 DNS endpoint 发现 | 配合 `dns_origin_domain` 使用自定义 DNS 起源域 |

`Runtime()` 的默认 preset 取决于 `data_dir`：**未显式传 `data_dir` 时为 `none`**，**显式给了 `data_dir` 时为 `local`**。

!!! note "已删除的 preset"
    `mdns` 与 `relay` preset 已在 0.3.5 删除。需要自定义 relay 时使用 `relay_endpoints` 配置项。

```python
from actant import Runtime
from actant.actant import _ActantConfig, _NetworkConfig

def _cfg(preset: str, **kw):
    return _ActantConfig(
        payload_signing_key="shared-secret",
        network=_NetworkConfig(preset=preset, **kw),
    )

rt = Runtime(config=_cfg("none", bootstrap_nodes=["<endpoint-id>"]))
```

环境变量 `ACTANT_DISCOVERY=<preset>` 优先于配置，用于在不动代码的前提下切换发现策略（例如 CI 中强制 `ACTANT_DISCOVERY=none`）。

未知 preset 在启动时通过 `DiscoveryMode::validate` 校验并返回 `Config` 错误，无静默回退。

## 构造预设

优先使用预设构造入口，不要逐个拼装配置：

| 入口 | 适用场景 | 固定行为 |
| --- | --- | --- |
| `Runtime.test()` | 单元/集成测试、CI | 临时 `data_dir`、网络 `none`、drain 5s |
| `Runtime()` | 单节点与通用开发 | 按 `data_dir` 选择 `none` / `local` preset |
| `Runtime.production()` | 生产部署 | 强制签名密钥、非空 allowlist、签名节点记录 |

**基础参数**（日常只需这些）：`name`、`data_dir`、`max_concurrent_tasks`、`default_task_timeout_ms`、`drain_timeout_secs`、`scheduler`、`route_policy`。

### 自定义路由策略

`route_policy` 接受内置名称或 Python callable：

| 取值 | 行为 |
| --- | --- |
| `None` / `"default"` | 心跳新鲜度 + 槽位比较 + 标签适配（`DefaultRoutePolicy`） |
| `"label_affinity"` | 本地满足标签即本地执行，否则转给标签匹配且槽位最多的 peer |
| callable | `fn(ctx: dict, candidates: list[dict]) -> 决策` |

callable 的返回值：

| 返回 | 含义 |
| --- | --- |
| `"local"` | 本地执行；本地不满足标签或配额时框架仍会改写为 `defer` |
| `"defer"` | 本轮延后，按 `remote_fallback_delay_ms` 重新入队 |
| `("remote", node_id)` | 转发到指定 peer；节点不在候选或缺 endpoint 地址时降级为 `defer` |
| `("defer", reason)` | 同上，`reason` 进入日志与 starvation 失败信息 |
| `None` | 弃权，交回 `DefaultRoutePolicy` |

`ctx` 的键：`local_role` / `local_draining` / `local_available_slots` /
`heartbeat_ttl_ms` / `local_labels` / `local_available_quotas` / `required_tags` /
`required_quota`。`candidates` 每项的键：`node_id` / `endpoint_addr` /
`available_slots` / `max_slots` / `role` / `draining` / `last_heartbeat_ms` /
`labels` / `available_quotas`。

回调抛异常、返回非法值、或拿不到 GIL 时，本次决策记 warning 并回退到
`DefaultRoutePolicy`，不会中断任务。回调在 routing 线程上同步执行，**不要**在其中
等待任务结果或做阻塞调用（例如 `AsyncResult.result()`），否则会与该线程负责的
结果回泵互相等待。

```python
def pick(ctx, candidates):
    if ctx["local_available_slots"] > 0 and not ctx["local_draining"]:
        return "local"
    free = [c for c in candidates if c["available_slots"] > 0 and not c["draining"]]
    if not free:
        return "defer"
    best = max(free, key=lambda c: c["available_slots"])
    return ("remote", best["node_id"])


rt = Runtime(route_policy=pick)
```


**高级参数**通过 `config=_ActantConfig(...)` 传入，仅在明确需要调优时使用：`store`（`_StoreConfig`）、`prefetch_*`、`state_poll_interval_ms`、`pending_result_channel_capacity`、`completed_retention_count`、`max_history_events`、`num_worker_processes`、`node_quotas` 等。字段与默认值见 [配置项](../reference/configuration.md)。

## 信任与生产模式

- `NetworkConfig.allowed_peer_ids` 非空时校验 gossip 成员资格；直连侧另有 ALPN 白名单与 wire MAC。
- `require_signed_records=True` 时拒绝缺签/坏签心跳。
- `Runtime.production()` 强制：非空 `payload_signing_key`、非空 `allowed_peer_ids`（空则拒启）、`require_signed_records=True`。

## 启动 Worker 节点

```bash
# 前台常驻，SIGINT/SIGTERM 触发优雅 drain
uv run actant worker --log-level info

# 自定义参数
uv run actant worker --max-concurrent-tasks 8 --scheduler priority

# 启动时恢复未完成工作流
uv run actant worker --resume
```

显式指定 `--data-dir` 后节点才启用 iroh P2P 自动发现，无需中心服务器；不指定时默认 `preset="none"`，仅 loopback（见上文「网络发现」）。

## 节点标签与配额

```python
cfg = _ActantConfig(
    payload_signing_key="secret",
    node_labels={"region": "cn", "gpu": "a100"},
    node_quotas={"gpu": 2},
)
```

任务通过 `tags` 与 `node_labels` 匹配后才允许放置；`quota` 由执行节点本地原子扣减，剩余容量随心跳广播。

## 可观测性

运行时环境变量（`ACTANT_TRACING` / `ACTANT_TOKIO_CONSOLE` / `ACTANT_DISCOVERY` / `ACTANT_PAYLOAD_KEY`）及其源码读取位置统一见[配置项 · 环境变量](../reference/configuration.md)，本页不重复维护。

## 下一步

- [Worker 运行模型](../architecture/worker-model.md)：进程池、超时、崩溃故障转移与可调参数。
- [排障](../operations/troubleshooting.md)。
