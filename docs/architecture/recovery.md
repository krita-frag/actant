# 恢复语义

## 唯一不变量

```text
recover = 快照 + 其后事件重放
```

没有第三条恢复路径。快照是 dirty-flush 写入的**重放加速缓存**，事实源是 event log。

## 快照加载

扫描 `dag` / `exec` / `pending` 前缀重建内存状态，Running 任务重置为 Pending；同时加载等待点快照（`orch:wait:`）与事件水位（`orch:eventseq:`）。

`submit` 采用 journal-first：`Submitted` 与全部 `NodeAdded` 作为一个原子批次先写 event log，再写快照。若在写快照前崩溃，恢复期会从 event log 的 topic 列表发现这些“只有 journal、没有快照”的工作流，从零重放事件重建 DAG、任务与 pending。它们是同一条恢复路径的起点，不是第二条恢复路径。

## 事件重放

对每个带水位的工作流，从 `workflow:{id}` topic 读取水位之后的事件，按事件语义幂等推进。重放复用 `mark_task_completed` / `fail_task` 的既有守卫：已终态的工作流或任务拒绝改写。

快照与事件由同一 `EventId` 水位对齐——水位之后的事件正是崩溃前尚未落盘的增量。

## 恢复事件

每个成功恢复的 workflow 会向 `event_log` 追加一条 `JournalEvent::Recovered` 事件（`corrupt=false`）；损坏被移除的 workflow 追加 `corrupt=true` 事件。外部订阅者可通过 `workflow:{id}` topic 观测恢复进度。

## 损坏处理

`dag` / `exec` / `pending` 三类条目对同一 workflow 是绑定的，三者共同构成 `WorkflowSlot`。若任一条目反序列化失败，整个 workflow 视为损坏：已创建的 slot 被移除，对应 store 条目被清理，避免“exec 数据悬空而 dag 缺失”导致后续 `on_task_completed` 因 `workflow not found` 失败。损坏事件通过 `tracing::warn` 记录。

## 已完成工作流的淘汰

`completed_retention_count` 为 `0` 时工作流在终态后立即淘汰；为 `N > 0` 时最多保留 N 个终态工作流，超出时淘汰最旧的，避免状态无限增长。淘汰只作用于已终态工作流。

淘汰会先写入 `Evicted` 墓碑，写入成功后才删除快照；写失败时放弃淘汰，避免「快照已删、墓碑缺失」导致重启后补建空 slot 并重新派发已完成任务。`workflow:{id}` topic 与其余历史事件保留，故淘汰后 `get_workflow_journal` 仍可查；`evicted` 可能出现在终态事件之后，journal 末条不再是稳定的终态事件。

淘汰成功后还会把该 topic 从 `EventLog::list_topics` 中隐藏（LMDB 实现删除 topic 的序号元数据、内存实现记入隐藏集合），但**保留全部事件条目**：恢复期不再逐个扫描历史工作流，`get_workflow_journal` 仍可按 topic 直读完整历史。同一 id 被重用时，下一次追加会重新写出序号元数据，topic 自动恢复可见。隐藏失败时墓碑仍在 topic 中，由下述代际判定兜底。

恢复期按事件序号判定墓碑所属的**代**：只有最后一个 `Submitted` 之后写下的 `Evicted` 才代表当前工作流已淘汰。同一 `workflow_id` 被重用时，新代的 `Submitted` 排在旧墓碑之后，恢复会补建新 slot 并只重放新代事件，避免旧代事件污染新 DAG。对「有快照但事件水位缺失」的工作流同样按最后一个 `Submitted` 钉住重放起点，覆盖 submit 后尚未 flush 就崩溃、以及旧数据的情形。

淘汰前会留存该工作流的终态摘要（内存表，上限 1024 条、不落盘），使 `get_workflow_state_summary` 在 slot 消失后仍返回 Completed / Failed / Cancelled 与错误，轮询方不会把「已淘汰」误判成「不存在」。flow 返回值由 Python 侧独立的 `TerminalRecordStore` 承载，不受 Rust 侧淘汰影响。

## 版本兼容

没有运行时版本协商，也没有迁移路径。事件集合即契约：`JournalEvent` 的变体与 `kind()` 字符串必须与当前构建对齐；尾部新增变体只需同步 `kind()`。

不兼容的字段或语义变更要求重建 `data_dir`。store 打开时校验 `meta:fmt_version`：版本不符直接拒绝启动；**库非空但没有版本号**（版本化之前写入的布局）同样拒绝——把未知布局当作当前版本读，只会在恢复期以「工作流被当作 corrupt 整体清除」的形式丢数据，且现场已被删除。journal 侧同口径：重放遇到无法解码的事件时，整个工作流判为不可信并被丢弃（写墓碑 → 删快照 → 隐藏 topic），不会带着「快照已把 Running 重置为 Pending、历史却未重放」的状态继续运行。

## 相关页面

- [工作流](../guide/flows.md)：等待点、信号、`start_flow` / `host_flows`。
- [故障转移](failover.md)：节点失效后的工作流接管。
