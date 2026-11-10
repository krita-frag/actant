# 恢复语义

## 唯一不变量

```text
recover = 快照 + 其后事件重放
```

没有第三条恢复路径。快照是 dirty-flush 写入的**重放加速缓存**，事实源是 event log。

## 快照加载

扫描 `dag` / `exec` / `pending` 前缀重建内存状态，Running 任务重置为 Pending；同时加载等待点快照（`orch:wait:`）与事件水位（`orch:eventseq:`）。

## 事件重放

对每个带水位的工作流，从 `workflow:{id}` topic 读取水位之后的事件，按事件语义幂等推进。重放复用 `mark_task_completed` / `fail_task` 的既有守卫：已终态的工作流或任务拒绝改写。

快照与事件由同一 `EventId` 水位对齐——水位之后的事件正是崩溃前尚未落盘的增量。

## 恢复事件

每个成功恢复的 workflow 会向 `event_log` 追加一条 `JournalEvent::Recovered` 事件（`corrupt=false`）；损坏被移除的 workflow 追加 `corrupt=true` 事件。外部订阅者可通过 `workflow:{id}` topic 观测恢复进度。

## 损坏处理

`dag` / `exec` / `pending` 三类条目对同一 workflow 是绑定的，三者共同构成 `WorkflowSlot`。若任一条目反序列化失败，整个 workflow 视为损坏：已创建的 slot 被移除，对应 store 条目被清理，避免“exec 数据悬空而 dag 缺失”导致后续 `on_task_completed` 因 `workflow not found` 失败。损坏事件通过 `tracing::warn` 记录。

## 已完成工作流的淘汰

超过 `completed_retention_count` 的已完成工作流会从内存与存储中移除，避免状态无限增长。淘汰只作用于已终态工作流：它们不再需要重放，因此不影响恢复语义。

## 相关页面

- [工作流](../guide/flows.md)：等待点、信号、`start_flow` / `host_flows`。
- [故障转移](failover.md)：节点失效后的工作流接管。
