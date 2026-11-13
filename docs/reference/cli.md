# CLI

> 本页由 `scripts/gen_api_reference.py` 从源码生成，请勿手工编辑。

命令的完整参数以 `actant --help` 为准；本页由 CLI 的 `--help` 输出生成。

## `actant`

```text
usage: actant [-h] {worker,status,version,metrics} ...

Actant CLI — P2P 分布式任务编排引擎

positional arguments:
  {worker,status,version,metrics}
    worker              启动一个 Worker 节点（后台常驻，P2P 自动发现对端）
    status              查询本地 Runtime 状态（版本、capability、持久化节点信息）
    version             显示版本号
    metrics             启动独立的 Prometheus HTTP exporter（/metrics 端点）

options:
  -h, --help            show this help message and exit
```

## `actant worker`

```text
usage: actant worker [-h] [--name NAME] [--data-dir DATA_DIR]
                     [--log-level {debug,info,warning,error}]
                     [--max-concurrent-tasks MAX_CONCURRENT_TASKS]
                     [--default-task-timeout-ms DEFAULT_TASK_TIMEOUT_MS]
                     [--drain-timeout-secs DRAIN_TIMEOUT_SECS]
                     [--remote-fallback-delay-ms REMOTE_FALLBACK_DELAY_MS]
                     [--scheduler {priority,fifo}] [--role {both,orchestrator,worker}]
                     [--payload-signing-key PAYLOAD_SIGNING_KEY]
                     [--bootstrap-nodes BOOTSTRAP_NODES]
                     [--gossip-bootstrap-peers GOSSIP_BOOTSTRAP_PEERS]
                     [--discovery {none,local,dns}] [--listen-ip LISTEN_IP]
                     [--listen-port LISTEN_PORT] [--relay-endpoints RELAY_ENDPOINTS]
                     [--heartbeat-interval-ms HEARTBEAT_INTERVAL_MS]
                     [--failure-timeout-ms FAILURE_TIMEOUT_MS] [--metrics-port METRICS_PORT]
                     [--resume] [--host-flows]

options:
  -h, --help            show this help message and exit
  --name NAME           节点名称（默认自动生成）
  --data-dir DATA_DIR   持久化数据目录（默认使用系统临时目录）
  --log-level {debug,info,warning,error}
                        Python 侧日志级别（默认 info；Rust 侧用 RUST_LOG 环境变量）
  --max-concurrent-tasks MAX_CONCURRENT_TASKS
                        单节点最大并发任务数（默认 CPU 核数）
  --default-task-timeout-ms DEFAULT_TASK_TIMEOUT_MS
                        任务默认超时毫秒（默认 30000）
  --drain-timeout-secs DRAIN_TIMEOUT_SECS
                        退出时等待在途任务的最长秒数（默认 30）
  --remote-fallback-delay-ms REMOTE_FALLBACK_DELAY_MS
                        本地无法执行时任务重新入队前的延迟毫秒（默认 500）
  --scheduler {priority,fifo}
                        调度器类型（默认 priority）
  --role {both,orchestrator,worker}
                        节点拓扑角色（默认 both）。orchestrator 只编排不执行本地任务；worker 只执行任务、不 claim workflow、不驱动
                        flow host
  --payload-signing-key PAYLOAD_SIGNING_KEY
                        P2P payload 签名密钥（也可用 ACTANT_PAYLOAD_KEY 环境变量）
  --bootstrap-nodes BOOTSTRAP_NODES
                        逗号分隔的 bootstrap 节点地址（完整 endpoint_addr，而非仅 endpoint id）
  --gossip-bootstrap-peers GOSSIP_BOOTSTRAP_PEERS
                        逗号分隔的 gossip 引导对端 endpoint ID；discovery=none 时必填，否则只建立直连、不会加入 gossip
  --discovery {none,local,dns}
                        节点发现 preset（默认 local）。纯内网/离线用 none；也可用 ACTANT_DISCOVERY 环境变量覆盖
  --listen-ip LISTEN_IP
                        P2P 监听 IPv4 地址（默认所有接口；内网部署建议显式指定）
  --listen-port LISTEN_PORT
                        P2P 监听端口（默认随机端口）
  --relay-endpoints RELAY_ENDPOINTS
                        逗号分隔的自建 relay URL 列表（覆盖 preset 自带 relay）
  --heartbeat-interval-ms HEARTBEAT_INTERVAL_MS
                        故障转移心跳间隔毫秒
  --failure-timeout-ms FAILURE_TIMEOUT_MS
                        故障转移超时判定毫秒
  --metrics-port METRICS_PORT
                        若指定，在该端口启动 Prometheus HTTP exporter（/metrics 端点）
  --resume              启动后在后台重放本节点未终态工作流（崩溃恢复闭环）。会执行已注册的 flow 代码并重新派发任务，默认关闭。
  --host-flows          常驻 flow host：持续扫描 data_dir 中未完成工作流并从 journal 重放，包括其他进程 start_flow 提交的工作流。与
                        --resume 互斥。
```

## `actant status`

```text
usage: actant status [-h] [--data-dir DATA_DIR]

options:
  -h, --help           show this help message and exit
  --data-dir DATA_DIR  持久化数据目录（读取 node_meta.json 获取节点信息）
```

## `actant version`

```text
usage: actant version [-h]

options:
  -h, --help  show this help message and exit
```

## `actant metrics`

```text
usage: actant metrics [-h] [--port PORT] [--log-level {debug,info,warning,error}]

options:
  -h, --help            show this help message and exit
  --port PORT           HTTP 监听端口（默认 9100）
  --log-level {debug,info,warning,error}
                        Python 侧日志级别
```
