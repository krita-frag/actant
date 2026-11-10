# 故障转移

## 角色

`FailoverManager` 维护远端节点心跳、可用槽位、活跃 workflow 列表和本地租约。节点被判定失效时，所有健康节点通过一致性哈希 `should_claim_workflow` 对 workflow 归属做确定性选择，只有获选节点会 claim 并重新调度该 workflow 的运行中任务。

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

1. **孤儿编排**：peer 的 `active_workflows` 非空 ⇒ 它是这些 workflow 的编排者，按一致性哈希 `claim` 接管并 `reschedule_running_tasks`。
2. **在途转发**：peer 是执行器时 `active_workflows` 为空，但它身上跑着本节点转发过去的任务。这些任务登记在 `FailoverManager::outbound`，按任务类型分流：

   - **编排任务**（`workflow_id` 非空）：终结为 `TaskCompletion::Failed`，经 event_bus 发布（与远端结果回灌同一条路），由工作流自身的失败策略决定是否重试。
   - **直提任务**（`workflow_id` 为空）：不就地失败，登记条目经 `relinquish_lease()` 放弃租约（`lease_deadline_ms` 归零 = 无有效租约，判定上等同立即到期）；Worker 的租约检查点取出后递增 `attempt`、清空路由并重新入队。旧执行节点的迟到结果因 `attempt` 不一致被拒绝（见下节）。

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
