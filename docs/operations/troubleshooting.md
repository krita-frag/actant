# 排障

## 任务超时

**现象**：任务在 `timeout_ms` 后被标记失败，worker 进程消失。

**原因**：任务级超时是硬超时，Rust 直接终止对应 worker 子进程。业务代码没有机会收尾。

**处理**：

- 需要优雅收尾时，在任务内使用 `get_task_context().is_cancelled()` 做协作式检查点（见[任务](../guide/tasks.md)）。
- 区分业务失败与超时：超时不会消耗 `retries` 之外的崩溃重路由额度。
- 任务级超时设为 `0` 会落到 `default_task_timeout_ms`（默认 30s），不是无限等待。

## 工作流 deadline 不生效

**现象**：`@flow(timeout_ms=...)` 到期，但函数体还在跑，或根本没有超时。

**原因**：

- 函数体本身不可抢占，deadline 只能防止提交新的工作。
- 不含任何 `Task.submit` 的 flow 不创建编排外壳，deadline 无宿主。

**处理**：把长耗时段落拆成 `Task.submit`；不要依赖 flow deadline 中断纯 Python 长循环。

## 重启后任务没有继续

**现象**：进程重启后，之前提交的任务不再执行。

**原因**：独立任务默认不持久化任务定义；只有工作流节点有执行表与步骤记忆。

**处理**：

- 工作流：调用 `Runtime.host_flows()`，或 `Runtime.start(auto_resume=True)`。跨进程提交用 `start_flow`，由配置了 `--host-flows` 的 worker 执行。
- 独立任务：用 `idempotency_key` 重新提交；重启后同键会抛 `AlreadyExistsError` 并带上首次 `task_id`，不会二次执行。
- 检查 `data_dir` 是否显式指定且可写；未指定时使用临时目录，重启即丢。

## 信号等待不返回

**现象**：`wait_signal("x")` 一直挂起，或重复递交没有变化。

**原因**：信号是闭锁。同一 `name` 重复 await 返回首次递交的 payload；重复递交不覆盖。

**处理**：

- 确认 `name` 与 `signal_wait_point(workflow_id, name, payload=...)` 完全一致。
- 信号可先于等待点抵达，注册时会命中缓冲；若仍挂起，检查工作流是否已创建（惰性创建）。
- payload 是原始 `bytes`，业务序列化由调用方负责。

## 大结果取不到

**现象**：`AsyncResult.result()` 卡住或报 blob 相关错误。

**原因**：超过 `REF_INLINE_THRESHOLD`（1MB）的结果落到本节点内容寻址 blob store，句柄内部用 `Ref` 表示。

**处理**：

- 确认 `data_dir` 可写，且节点未被清理。
- 跨节点拉取依赖网络可达；`Ref` 的来源节点必须在位。
- 需要长期保留时不要把 `data_dir` 当临时目录。

## 任务被反复重路由

**现象**：任务多次重新入队，最终失败。

**原因**：worker 崩溃触发故障转移，受 `crash_failover_max_attempts`（默认 3）约束。

**处理**：

- 看 worker stderr 是否转发到父进程 tracing。
- 区分崩溃（基础设施）与业务异常；业务失败不参与该上限。
- 减少并发或降低 `num_worker_processes` 排除资源耗尽。

## 节点之间发现不到

**现象**：多节点启动后互相不可见。

**处理**：

- 未显式传 `data_dir` 时默认 `none`，仅 loopback；显式给 `data_dir` 才是 `local`。
- 检查 `ACTANT_DISCOVERY` 是否覆盖了配置。
- `none` 模式必须显式 `bootstrap_nodes` 拨号。
- 生产模式检查 `allowed_peer_ids`、`require_signed_records` 与 `payload_signing_key`。

## 常用诊断开关

```bash
ACTANT_TRACING=1 uv run actant worker --log-level debug
ACTANT_TOKIO_CONSOLE=1 uv run actant worker
```

完整环境变量清单与源码读取位置见[配置项 · 环境变量](../reference/configuration.md)。
