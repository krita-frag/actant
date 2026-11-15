# 故障转移

## 角色

`FailoverManager` 维护远端节点心跳（含其活跃 workflow 清单）、可用槽位、容量视图和本地租约。节点被判定失效时，所有健康节点通过一致性哈希 `should_claim_workflow` 对 workflow 归属做确定性选择，只有获选节点会 claim 并重新调度该 workflow 的运行中任务。

## 时间参数

`FailoverConfig` 有**四个**时间参数，其中一个是秒、其余是毫秒：

| 字段 | 默认值 | 单位 | 作用 |
|---|---|---|---|
| `heartbeat_interval_ms` | 2000 | 毫秒 | 心跳发送间隔 |
| `failure_timeout_ms` | 8000 | 毫秒 | 失联判定阈值（超过即判故障） |
| `lease_duration_ms` | 16000 | 毫秒 | 工作流租约时长（固定值，不由前两项推导） |
| `lease_expiry_check_interval_secs` | 4 | **秒** | 失效租约扫描周期 |

`FailoverConfig::validate()`（`ActantConfig::validate()` 在启动时调用）强制五条约束，
任一不满足即启动失败，不留「跑起来才发现」的窗口：

1. `heartbeat_interval_ms > 0`；
2. `failure_timeout_ms > heartbeat_interval_ms`——至少容忍一次心跳丢失，不会因单次延迟被误判；
3. `lease_duration_ms > failure_timeout_ms`——故障判定完成前旧租约不会先过期；
4. `lease_expiry_check_interval_secs > 0`；
5. `lease_expiry_check_interval_secs * 1000 <= lease_duration_ms / 2`——扫描周期超过半个租期时，
   失效租约滞留期与本节点「无有效租约仍在驱动」的双主窗口都被放大，故从字段文档升级为强制校验。

主动探测的单次上界不是配置项，而是常量 `OUTBOUND_PROBE_TIMEOUT = 2s`（见第 2 腿之二）。

## 五条处置腿

`detect_and_claim_failed_nodes()` 每轮推进五条腿，对应「编排器死」「执行器死」「心跳视图盲区」
「分区恢复」四类形态。**编号是语义分类，不是执行顺序**（顺序见本节末尾）。

- **第 1 腿 · 孤儿编排接管**：peer 最后一次心跳携带的 `active_workflows` 即它失联时仍在驱动的 workflow，按一致性哈希 `should_claim_workflow` 在**存活且具备编排角色**的节点间选出接管方，获选节点 `claim` 并 `reschedule_workflow_tasks`。清单必须取自心跳：租约虽然也记录 owner，但远端副本不会随 owner 的续租刷新，`lease_duration` 之后即过期并被清理，按它反查只能看到最近一次 claim 落在窗口内的那部分 workflow。仅有 Worker 角色的节点不进候选集——否则哈希可能把孤儿派给一个永不 claim 的节点。
- **第 2 腿 · 在途转发处置**：peer 没有在跑任何 workflow，但它身上跑着本节点转发过去的任务。这些任务登记在 `FailoverManager::outbound`，按任务类型分流：

  - **编排任务**（`workflow_id` 非空）：终结为 `TaskCompletion::Failed`，经任务结果 correctness 通道发布（与远端结果回灌同一条路），由工作流自身的失败策略决定是否重试。
  - **直提任务**（`workflow_id` 为空）：不就地失败，登记条目经 `relinquish_lease()` 放弃租约（`lease_deadline_ms` 归零 = 无有效租约，判定上等同立即到期）；Worker 的租约检查点取出后递增 `attempt`、清空路由并重新入队。旧执行节点的迟到结果因 `attempt` 不一致被拒绝（见下节）。

- **第 2 腿之二 · 主动探测**：心跳视图有天然盲区——目标在发出第一个可被观测的心跳前就失联，或只按 `endpoint_addr` 直投、从未建立心跳关系。此时 `peers` 里既无条目也无超时基线，第 1／2 腿都无从触发，在途任务会永久挂起。对**已经成功转发过任务**的目标，本节点持有直连通道，用 `DirectRequest::Ping` 探测：

  - 探测成功即认为对端仍在服务，**不**终结其任务（避免误杀健康任务）；失败或超时才终结；
  - 仅对不在新鲜心跳窗口内的目标探测，正常拓扑下每轮通常零探测；`probe_exempt` 条目（已归还 origin 的直提任务）与本轮已判死的节点直接跳过；
  - 多目标**并发**探测，单次上界 `OUTBOUND_PROBE_TIMEOUT = 2s`；串行时 K 个不可达目标会让一轮检测线性膨胀到 K×2s，并推迟同轮其余腿。

- **第 3 腿 · 退让接管重试**：入口是 `pending_claims` 表而不是 peer 视图——触发退让的 peer 当轮已被 `expire_stale_peers` 清出视图，没有扫描入口。每轮重新参加一致性哈希选举（失效 peer 已移出候选集，职责可能已转给本节点），三种结局：接管成功则重调度并移除、仍退让则保留、原 owner 心跳复活则移除（不再是孤儿）。

- **第 4 腿 · 重调度任务重试**：接管时 `enqueue` 失败的任务（调度器未注入或临时拒绝）留在 `pending_reschedules`，每轮重投。没有这条腿，接管方会「租约拿到了、任务却一个也没入队」，工作流停在 Running。

- **第 5 腿 · 分区恢复仲裁**：分区愈合后两侧可能同时在驱动同一工作流（对称分区超过 `lease_duration` 后恢复，claim 广播在分区内丢失、`expire_leases` 又对本节点活跃租约续租）。处理对象是"本节点持有租约、且**存活** peer 的 `active_workflows` 也含该工作流"：

  - 租约仍有效 → 重播 `OrchestratorClaim`，时间戳取 `claimed_at_ms` 而**不刷新**（续租式刷新会让两侧比较失去稳定基准）；对端按 `(timestamp_ms, node_id)` 全序比较，恰好一方胜出，输家移除 active 集合、停止续租与派发。对端的对称扫描保证双向仲裁。
  - 租约已失效但工作流仍在驱动（`expire_leases` 续租窗口内）→ 走完整 `claim_workflow` 重置租约并重调度。

### 一轮里的执行顺序

```text
逐 peer 扫描： 第 2 腿（在途处置） → 第 1 腿（孤儿接管）
           → 第 2 腿之二（主动探测，跳过本轮已判死的节点）
           → 第 3 腿（退让接管重试）
           → 第 5 腿（分区恢复仲裁）
           → 第 4 腿（重调度任务重投）
```

理由：第 2 腿排在第 1 腿之前，因为死亡执行器的 `active_workflows` 为空，只处理孤儿会漏掉在途任务；
探测放在逐 peer 扫描之后，因为已被判死的 peer 条目在上文已清空，此处只剩「心跳都没来得及观测到」
的残余目标；第 4 腿放最后，它重投的是前面几条腿新接管时入队失败的任务。

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

获选节点遇到尚未到期的租约时退让，退让项落到 `pending_claims`，由第 3 腿逐轮重试——
若就此作罢，租约到期被 `expire_leases` 删除后工作流永久无人驱动。

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
