# 故障转移

## 角色

`FailoverManager` 维护远端节点心跳（含其活跃 workflow 清单）、可用槽位、容量视图和本地租约。节点被判定失效时，所有健康节点通过一致性哈希 `should_claim_workflow` 对 workflow 归属做确定性选择，只有获选节点会 claim 并重新调度该 workflow 的运行中任务。

## 时间参数

`FailoverConfig` 要求：

```text
heartbeat_interval_ms < failure_timeout_ms < lease_duration_ms
```

该关系保证：

- 节点有足够心跳机会，不会因单次延迟被误判；
- 故障判定完成前旧租约不会先过期；
- claim 后的租约有明确过期时间，避免永久双主。

## 两条失联处置腿

判失效后按“谁的账谁认”分两腿，覆盖执行器与编排器两种死亡形态：

1. **孤儿编排**：peer 最后一次心跳携带的 `active_workflows` 即它失联时仍在驱动的 workflow，按一致性哈希 `claim` 接管并 `reschedule_incomplete_tasks`。清单必须取自心跳：租约虽然也记录 owner，但远端副本不会随 owner 的续租刷新，`lease_duration` 之后即过期并被清理，按它反查只能看到最近一次 claim 落在窗口内的那部分 workflow。
2. **在途转发**：peer 没有在跑任何 workflow，但它身上跑着本节点转发过去的任务。这些任务登记在 `FailoverManager::outbound`，按任务类型分流：

   - **编排任务**（`workflow_id` 非空）：终结为 `TaskCompletion::Failed`，经 event_bus 发布（与远端结果回灌同一条路），由工作流自身的失败策略决定是否重试。
   - **直提任务**（`workflow_id` 为空）：不就地失败，登记条目经 `relinquish_lease()` 放弃租约（`lease_deadline_ms` 归零 = 无有效租约，判定上等同立即到期）；Worker 的租约检查点取出后递增 `attempt`、清空路由并重新入队。旧执行节点的迟到结果因 `attempt` 不一致被拒绝（见下节）。

## 状态副本：接管的前提

接管方必须先持有工作流的状态副本（DAG + 执行状态），否则手里只有空壳，
没有任何任务可重调度。副本经 heads 交换（`actant:heads`）扩散：接收方本地
没有该工作流、或节点数/成功数落后于对端时，向对端请求全量状态。

两个实现约束：

- **同步话题是全局的**（`actant:wf-state-req` / `actant:wf-state-resp`），
  目标节点写在载荷（`target_node`）里，非目标节点收到即丢弃。原因是 iroh
  gossip 只在话题创建时接收集群引导集，**不会**把「已建连接、但当时未加入该
  话题」的邻居补进来；运行期按节点新建话题会订阅成功、`broadcast` 返回 `Ok`，
  却没有一个接收方。
- **接管方要重派发的是「未终结且已就绪」的任务**，不只是 Running：
  `restore_workflow_from_bytes` 会把非终态任务重置为 `Pending` 并推进
  `attempt`，只看 Running 会让接管方一个可派发任务都没有，工作流停在
  Running 永不推进。

副本落地后任务由接管方执行，而提交句柄留在已死亡的编排节点上。执行者因此
在**没有本地句柄**的情况下也要把终态回灌 orchestrator（`_on_task_result` 的
无句柄分支）：结果帧正文对 Rust 不透明，业务成败必须在 Python 侧解码后显式
上报，否则任务跑完了工作流也不封口。

## 退让不是终点

`claim_workflow` 返回 `ClaimOutcome`（`Adopted` / `AlreadyOwned` / `Deferred` /
`NotOrchestrator`）而不是 `Ok(())`：退让与接管都是成功，但只有 `Adopted` /
`AlreadyOwned` 才允许重调度任务。

获选节点遇到尚未到期的租约时会退让。此时触发退让的 peer 已在本轮被
`expire_stale_peers` 清出视图，后续轮次没有扫描入口——若就此作罢，租约到期被
`expire_leases` 删除后工作流永久无人驱动。因此退让项记入 `pending_claims`，
由每轮故障检测的第三条腿重试：接管成功或原 owner 心跳复活即移除该项。

## 直提任务租约与 attempt fencing

直提任务转发前由 origin 写入 `TaskDefinition::lease_deadline_ms = now + task_timeout + failure_timeout`。远端不解释该字段，到期判定只发生在 origin：

- Worker 主循环每秒（或空闲唤醒时）调用 `take_expired_standalone`（判定统一为 `TaskDefinition::lease_expired_at`），取出的条目按 `crash_failover_max_attempts` 决定重投或终结为 `TaskCompletion::Failed`（`worker` kind）。
- 结果回传携带 `attempt`；`take_outbound_if_not_stale` 只拒绝**严格更旧**的 attempt（执行节点自身崩溃转移也会推进 attempt），旧执行的迟到结果既不发布完成事件，也不移除新 attempt 的登记。

编排任务不使用该字段：工作流已有自己的超时、重试与失败语义，租约重投只针对没有编排器兜底的直提任务。

## 租约持久化

新 claim 的租约同时记录墙钟时间和单调 deadline；从 store 恢复的租约没有单调基线，只能回退到墙钟过期判断。

## 相关页面

- [Worker 运行模型](worker-model.md)：worker 崩溃后的任务重路由。
- [恢复语义](recovery.md)：编排器自身的快照 + 重放恢复。
