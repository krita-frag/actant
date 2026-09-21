//! 基于心跳与租约的工作流故障转移。
//!
//! [`FailoverManager`] 维护远端节点心跳、可用槽位、活跃 workflow 列表和本地租约。
//! 当节点被判定失效时，所有健康节点使用一致性哈希
//! [`should_claim_workflow`] 对 workflow 归属做
//! 确定性选择，只有获选节点会 claim 并重新调度该 workflow 的运行中任务。
//!
//! ## 时间参数
//!
//! [`FailoverConfig`] 要求
//! `heartbeat_interval_ms < failure_timeout_ms < lease_duration_ms`。这个关系保证：
//! - 节点有足够心跳机会，不会因为单次延迟就被误判；
//! - 故障判定完成前旧租约不会先过期；
//! - claim 后的租约有明确过期时间，避免永久双主。
//!
//! ## 持久化
//!
//! 新 claim 的租约同时记录墙钟时间和单调 deadline；从 store 恢复的租约没有
//! 单调基线，只能回退到墙钟过期判断。
//!
//! ## 两条失联处置腿
//!
//! 判失效后按"谁的账谁认"分两腿，覆盖执行器与编排器两种死亡形态：
//!
//! 1. **孤儿编排**（既有）：peer 的 `active_workflows` 非空 ⇒ 它是那些 workflow
//!    的编排者，按一致性哈希 `claim` 接管并 `reschedule_running_tasks`。
//! 2. **在途转发**（新增）：peer 是**执行器**时 `active_workflows` 为空，
//!    但它身上跑着**本节点转发过去**的任务。这些任务登记在
//!    [`FailoverManager::outbound`]，失联时终结为 `TaskCompletion::Failed`
//!    并经 event_bus 发布（与远端结果回灌同一条路），使提交方 `AsyncResult`
//!    不再永久挂起。
//!
//! 第 2 腿刻意**不重派发**：源节点没有"该任务未执行完"的持久凭据，盲目重跑
//! 会静默重复副作用；重跑交由显式重试策略（重试裁决）或提交方重提。
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::{
    should_claim_workflow, ActorId, FailoverConfig, NodeHeartbeat, NodeId, OrchestratorClaim,
    PlatformInfo, Result, TaskCompletion, TaskId, WireEnvelope, WireMessage, WorkflowId,
    STORE_KEY_LEASE, TOPIC_FAILOVER, TOPIC_HEADS, TOPIC_HEARTBEAT,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::state::{HybridLogicalClock, LmdbStore};
use crate::runtime::workflow::actor::workflow_methods;
use crate::runtime::workflow::messaging;

/// 在途目标主动存活探测的单次超时上界。
///
/// 取值远小于 `failure_timeout_ms`（默认 8s）且远小于检测间隔（4s）量级：
/// 对端已死时直连握手可能拖到 QUIC 超时（数十秒），必须由本上界截断，
/// 否则探测会拖住整轮失联扫描。2s 对健康对端是极宽裕的往返预算。
const OUTBOUND_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
struct PersistedLease {
    node_id: NodeId,
    claimed_at_ms: u64,
    expires_at_ms: u64,
}

struct PeerState {
    node_id: NodeId,
    last_heartbeat_ms: u64,
    active_workflows: HashSet<WorkflowId>,
    available_slots: u32,
    max_slots: u32,
    endpoint_addr: Option<String>,
    labels: BTreeMap<String, String>,
    platform: Option<PlatformInfo>,
}

/// 已转发到远端 peer、结果尚未回来的在途任务条目。
///
/// 只保存**构造失败完成事件所需的最小字段**（不含 payload）——登记表是"路由
/// 记账"，不是任务副本，避免为每个在途任务额外持有一份可能达 MiB 级的 payload。
#[derive(Debug, Clone)]
pub struct OutboundTask {
    /// 任务所属 workflow（直提任务为空串 id）。
    pub workflow_id: WorkflowId,
    /// 任务名（失败完成事件的 `task_name` 字段）。
    pub task_name: String,
    /// 转发目标节点。
    pub target_node: NodeId,
    /// 转发目标的可达地址（iroh 公钥）。用于心跳视图不可用时的主动存活探测；
    /// `None` 时退化为以 `target_node` 作为地址（与转发路径的取值一致）。
    pub target_endpoint_addr: Option<String>,
    /// 登记时刻，用于诊断"在途过久"。
    pub forwarded_at: Instant,
}

/// 对外暴露的 peer 视图（节点可见性 N2）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub last_heartbeat_ms: u64,
    pub active_workflows: HashSet<WorkflowId>,
    pub available_slots: u32,
    pub max_slots: u32,
    pub endpoint_addr: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub platform: Option<PlatformInfo>,
}

impl From<&PeerState> for PeerInfo {
    fn from(state: &PeerState) -> Self {
        Self {
            node_id: state.node_id.clone(),
            last_heartbeat_ms: state.last_heartbeat_ms,
            active_workflows: state.active_workflows.clone(),
            available_slots: state.available_slots,
            max_slots: state.max_slots,
            endpoint_addr: state.endpoint_addr.clone(),
            labels: state.labels.clone(),
            platform: state.platform.clone(),
        }
    }
}

struct LeaseEntry {
    node_id: NodeId,
    claimed_at_ms: u64,
    expires_at_ms: u64,
    /// 单调时钟租约到期时刻。``Some`` 时优先用于过期判定，避免 NTP 时钟跳变
    /// 导致误判（M5-2 改进）。``None`` 表示租约从持久化恢复（无单调基线），
    /// 回退到墙钟 ``expires_at_ms`` 比较。
    deadline: Option<std::time::Instant>,
}

impl LeaseEntry {
    /// 租约是否仍有效。
    ///
    /// 优先使用单调时钟 ``deadline``（若存在），否则回退到墙钟比较。
    /// ``now_ms`` 为当前墙钟毫秒，``now_monotonic`` 为当前单调时刻。
    fn is_valid(&self, now_ms: u64, now_monotonic: std::time::Instant) -> bool {
        match self.deadline {
            Some(deadline) => now_monotonic < deadline,
            None => now_ms < self.expires_at_ms,
        }
    }

    /// 构造一个带单调 deadline 的新租约（用于本进程新 claim 的租约）。
    fn new_with_monotonic(
        node_id: NodeId,
        claimed_at_ms: u64,
        expires_at_ms: u64,
        lease_duration_ms: u64,
    ) -> Self {
        Self {
            node_id,
            claimed_at_ms,
            expires_at_ms,
            deadline: std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(lease_duration_ms)),
        }
    }

    /// 构造一个从持久化恢复的租约（无单调基线，回退墙钟比较）。
    fn restored(node_id: NodeId, claimed_at_ms: u64, expires_at_ms: u64) -> Self {
        Self {
            node_id,
            claimed_at_ms,
            expires_at_ms,
            deadline: None,
        }
    }
}

impl Clone for LeaseEntry {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            claimed_at_ms: self.claimed_at_ms,
            expires_at_ms: self.expires_at_ms,
            deadline: self.deadline,
        }
    }
}

/// 维护节点健康状态、workflow 租约和故障接管决策。
///
/// `FailoverManager` 通过 heartbeat topic 收集 peer 的活跃 workflow 与容量视图；
/// 当 peer 超过 `failure_timeout_ms` 未更新时，本节点会按一致性哈希判断自己是否
/// 应接管该 peer 的每个 workflow。接管流程先通知本地 `WorkflowActor` adopt 状态，
/// 再持久化租约并广播 claim。
pub struct FailoverManager {
    node_id: NodeId,
    network: Arc<dyn crate::runtime::network::Transport>,
    actor_system: Arc<ActorSystem>,
    workflow_actor_id: ActorId,
    scheduler: parking_lot::Mutex<Option<Arc<dyn crate::runtime::workflow::Scheduler>>>,
    peers: Arc<DashMap<NodeId, PeerState>>,
    heartbeat_interval_ms: u64,
    failure_timeout_ms: u64,
    lease_duration_ms: u64,
    lease_expiry_check_interval_secs: u64,
    leases: Arc<DashMap<WorkflowId, LeaseEntry>>,
    store: Option<LmdbStore>,
    clock: Arc<HybridLogicalClock>,
    /// Local node's available task capacity (updated by the worker).
    local_available_capacity: Arc<AtomicU32>,
    /// Local node's maximum task capacity.
    local_max_capacity: Arc<AtomicU32>,
    /// 已转发到远端 peer 的在途任务登记（`task_id` → 目标/来源信息）。
    ///
    /// 仅内存、不落盘：本表是恢复加速器而非正确性凭据——丢失只退化为"该任务
    /// 不因节点失联而终止"（回到失联处置之前的行为），不会产生错误恢复。
    outbound: Arc<DashMap<String, OutboundTask>>,
    /// 本地事件总线：失联时把在途任务终结为 `TaskFailed` 发布出去，让提交方
    /// `AsyncResult` 得以终止。`None` 时（极简测试桩）该腿只清理登记表并告警。
    event_bus: Option<EventBus>,
    /// 本节点平台信息（N1），随心跳广播。
    platform: Option<PlatformInfo>,
    /// 本节点用户自定义标签（N2），随心跳广播。
    labels: BTreeMap<String, String>,
    /// 节点身份密钥（endpoint keypair）。`Some` 时心跳以私钥签名。
    signing_key: Option<iroh::SecretKey>,
    /// `true` 时拒绝缺签/坏签的入站心跳（身份与信任）。
    require_signed_records: bool,
    /// 允许加入集群的 iroh endpoint id（z32 字符串）。空 = 不校验成员资格。
    allowed_peer_ids: Vec<String>,
}

impl Drop for FailoverManager {
    fn drop(&mut self) {
        tracing::debug!("FailoverManager::drop");
    }
}

impl FailoverManager {
    /// 使用默认 failover 配置创建 manager。
    pub fn new(
        node_id: NodeId,
        network: Arc<dyn crate::runtime::network::Transport>,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
    ) -> Self {
        let config = FailoverConfig::default();
        Self::with_config(
            node_id,
            network,
            actor_system,
            workflow_actor_id,
            config,
            None,
        )
    }

    pub(crate) fn with_config(
        node_id: NodeId,
        network: Arc<dyn crate::runtime::network::Transport>,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
        config: FailoverConfig,
        store: Option<LmdbStore>,
    ) -> Self {
        let fm = Self {
            node_id,
            network,
            actor_system,
            workflow_actor_id,
            scheduler: parking_lot::Mutex::new(None),
            peers: Arc::new(DashMap::new()),
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            failure_timeout_ms: config.failure_timeout_ms,
            lease_duration_ms: config.lease_duration_ms,
            lease_expiry_check_interval_secs: config.lease_expiry_check_interval_secs,
            leases: Arc::new(DashMap::new()),
            store,
            clock: Arc::new(HybridLogicalClock::new()),
            local_available_capacity: Arc::new(AtomicU32::new(0)),
            local_max_capacity: Arc::new(AtomicU32::new(0)),
            outbound: Arc::new(DashMap::new()),
            event_bus: None,
            platform: None,
            labels: BTreeMap::new(),
            signing_key: None,
            require_signed_records: false,
            allowed_peer_ids: Vec::new(),
        };
        fm.recover_leases_from_store();
        fm
    }

    /// 注入节点身份与信任配置（身份与信任批）。
    ///
    /// - `signing_key`：本节点 endpoint 私钥，`Some` 时出站心跳签名；
    /// - `require_signed_records`：拒绝缺签/坏签的入站心跳；
    /// - `allowed_peer_ids`：非空时入站心跳的 `endpoint_addr` 必须在列表内
    ///   （gossip 侧成员校验；直连侧另有 ALPN 白名单）。
    pub fn with_identity(
        mut self,
        signing_key: Option<iroh::SecretKey>,
        require_signed_records: bool,
        allowed_peer_ids: Vec<String>,
    ) -> Self {
        self.signing_key = signing_key;
        self.require_signed_records = require_signed_records;
        self.allowed_peer_ids = allowed_peer_ids;
        self
    }

    /// 心跳签名域：`signature = None` 的心跳序列化字节。
    fn signing_payload(hb: &NodeHeartbeat) -> Vec<u8> {
        let unsigned = NodeHeartbeat {
            signature: None,
            ..hb.clone()
        };
        postcard::to_allocvec(&unsigned).unwrap_or_default()
    }

    /// 注入本节点元数据（平台信息 + 标签），随心跳广播（N1/N2）。
    pub fn with_node_metadata(
        mut self,
        platform: Option<PlatformInfo>,
        labels: BTreeMap<String, String>,
    ) -> Self {
        self.platform = platform;
        self.labels = if crate::common::model::node_labels_within_limit(&labels) {
            labels
        } else {
            tracing::warn!(
                "node_labels exceed {} bytes; labels will not be advertised",
                crate::common::model::NODE_LABELS_MAX_BYTES
            );
            BTreeMap::new()
        };
        self
    }

    /// 向 WorkflowActor 发起调用。
    async fn call_workflow<T: serde::Serialize>(
        &self,
        method: &str,
        payload: T,
    ) -> crate::common::Result<crate::common::ActorMessageResult> {
        let bytes = messaging::encode(&payload)?;
        self.actor_system
            .call(&self.workflow_actor_id, method, bytes)
            .await
            .map_err(|e| crate::common::ActantError::Actor(e.to_string()))
    }

    /// 调用无返回值的 WorkflowActor 方法，仅检查错误。
    async fn call_workflow_void<T: serde::Serialize>(
        &self,
        method: &str,
        payload: T,
    ) -> crate::common::Result<()> {
        let result = self.call_workflow(method, payload).await?;
        if let Some(err) = result.error {
            Err(crate::common::ActantError::from(err))
        } else {
            Ok(())
        }
    }

    /// 获取活跃 workflow ID 列表。
    async fn active_workflow_ids(&self) -> crate::common::Result<Vec<WorkflowId>> {
        let result = self
            .call_workflow(workflow_methods::ACTIVE_WORKFLOW_IDS, ())
            .await?;
        messaging::decode(&result.payload)
    }

    /// 从本地编排器接管指定 workflow。
    async fn adopt_workflow(&self, workflow_id: &WorkflowId) -> crate::common::Result<()> {
        self.call_workflow_void(workflow_methods::ADOPT_WORKFLOW, workflow_id)
            .await
    }

    /// 从本地编排器中移除指定 workflow（仅内存状态）。
    async fn remove_active_workflow(&self, workflow_id: &WorkflowId) -> crate::common::Result<()> {
        self.call_workflow_void(workflow_methods::REMOVE_ACTIVE_WORKFLOW, workflow_id)
            .await
    }

    /// 覆盖心跳发送间隔。
    ///
    /// 主要供测试或特殊部署调参使用；生产配置通常来自
    /// [`FailoverConfig`]。
    pub fn with_heartbeat_interval(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms;
        self
    }

    /// 设置本节点当前可用容量快照。
    ///
    /// Worker 会通过容量回调持续更新该值，心跳广播会携带它供远端路由决策使用。
    pub fn with_capacity(self, available: u32, max: u32) -> Self {
        self.local_available_capacity
            .store(available, Ordering::Relaxed);
        self.local_max_capacity.store(max, Ordering::Relaxed);
        self
    }

    /// 注入用于 failover 重调度的 scheduler。
    pub fn set_scheduler(&self, scheduler: Arc<dyn crate::runtime::workflow::Scheduler>) {
        *self.scheduler.lock() = Some(scheduler);
    }

    /// 注入本地事件总线，用于失联时终结在途任务（见 `fail_outbound_tasks_to`）。
    ///
    /// 采用 builder 方法而非构造参数，是为了让既有 11 处
    /// `FailoverManager::new` 调用点（多为不需要该腿的单元测试桩）保持不变。
    pub fn with_event_bus(mut self, event_bus: EventBus) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// 登记一条"已转发到远端、结果未回"的在途任务。
    ///
    /// 由 Worker 在 `forward_remote_task` 成功后调用；同一 `task_id` 重复登记
    /// 覆盖旧值（重路由场景下目标是最后一次转发目标）。
    pub fn record_outbound(
        &self,
        task_id: &TaskId,
        target_node: &NodeId,
        target_endpoint_addr: Option<&str>,
        workflow_id: WorkflowId,
        task_name: &str,
    ) {
        self.outbound.insert(
            task_id.as_str().to_string(),
            OutboundTask {
                workflow_id,
                task_name: task_name.to_string(),
                target_node: target_node.clone(),
                target_endpoint_addr: target_endpoint_addr.map(str::to_string),
                forwarded_at: Instant::now(),
            },
        );
    }

    /// 清除一条在途登记（该任务的命运已在本地确定）。
    ///
    /// 由 Worker 网络事件路由在收到该任务的远端结果时调用。
    pub fn clear_outbound(&self, task_id: &str) {
        self.outbound.remove(task_id);
    }

    /// 当前在途登记条数（诊断 / 测试用）。
    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }

    /// 取出并移除所有目标为 `dead` 的在途登记。
    fn drain_outbound_to(&self, dead: &NodeId) -> Vec<(String, OutboundTask)> {
        let keys: Vec<String> = self
            .outbound
            .iter()
            .filter(|e| &e.value().target_node == dead)
            .map(|e| e.key().clone())
            .collect();
        let mut drained = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((_, entry)) = self.outbound.remove(&key) {
                drained.push((key, entry));
            }
        }
        drained
    }

    /// 该 peer 是否处于"新鲜"心跳窗口内（可用于跳过主动探测）。
    fn is_peer_fresh(&self, node_id: &NodeId) -> bool {
        let now_ms = crate::common::epoch_millis();
        self.peers.get(node_id).is_some_and(|p| {
            p.last_heartbeat_ms > 0
                && now_ms.saturating_sub(p.last_heartbeat_ms) <= self.failure_timeout_ms
        })
    }

    /// 失联处置第 2 腿之二：对**心跳视图不可判**的在途目标做主动存活探测。
    ///
    /// 心跳视图有一个天然盲区：节点在发出第一个可被观测的心跳之前就失联
    /// （或从未与本节点建立心跳关系，例如仅按 `endpoint_addr` 直投）。此时
    /// `peers` 里既没有它的条目、也没有可用于超时判定的时间基线，第 1 腿与
    /// 第 2 腿的前半段都无从触发，在途任务会永久挂起。对一个**已经成功转发过
    /// 任务**的目标，本节点持有直连通道，可以直接探测——这是该情形下唯一
    /// 可靠的存活信号。
    ///
    /// 探测成功即认为对端仍在服务：**不**终结其任务（避免误杀健康任务）；
    /// 探测失败/超时才终结。仅对"不在新鲜心跳窗口内"的目标探测，正常拓扑下
    /// 每次扫描通常零探测。
    async fn probe_outbound_targets(&self) {
        // 去重：同一目标可能承载多个在途任务，只探测一次。
        let mut targets: HashMap<NodeId, Option<String>> = HashMap::new();
        for entry in self.outbound.iter() {
            let target = entry.value().target_node.clone();
            if self.is_peer_fresh(&target) {
                continue;
            }
            targets
                .entry(target)
                .or_insert_with(|| entry.value().target_endpoint_addr.clone());
        }
        for (target, addr) in targets {
            let addr = addr.unwrap_or_else(|| target.as_str().to_string());
            let probe = self
                .network
                .send_direct_request(&addr, crate::runtime::network::DirectRequest::Ping);
            let alive = matches!(
                tokio::time::timeout(OUTBOUND_PROBE_TIMEOUT, probe).await,
                Ok(Ok(crate::runtime::network::DirectResponse::Pong))
            );
            if alive {
                tracing::debug!(
                    node = %target.as_str(),
                    "in-flight target answered liveness probe; keeping its tasks"
                );
                continue;
            }
            tracing::warn!(
                node = %target.as_str(),
                addr = %addr,
                probe_timeout_ms = OUTBOUND_PROBE_TIMEOUT.as_millis() as u64,
                "in-flight target did not answer liveness probe; settling its tasks"
            );
            self.fail_outbound_tasks_to(&target).await;
        }
    }

    /// 失联处置第 2 腿：把目标为 `dead` 的在途任务终结为 `TaskFailed`。
    ///
    /// 发布走 event_bus（与 `network_router::publish_remote_completion` 同一条路）：
    /// - 直提任务：Python 事件泵解析 `AsyncResult` 为异常，句柄不再永久挂起；
    /// - 编排任务：事件泵照常回灌 orchestrator，由重试裁决决定是否重派发。
    ///
    /// 返回终结的任务数，供调用方决定是否需要打日志。
    async fn fail_outbound_tasks_to(&self, dead: &NodeId) -> usize {
        let drained = self.drain_outbound_to(dead);
        if drained.is_empty() {
            return 0;
        }
        let count = drained.len();
        let Some(event_bus) = self.event_bus.as_ref() else {
            tracing::warn!(
                node = %dead.as_str(),
                count,
                "no event_bus on FailoverManager; in-flight tasks cannot be settled"
            );
            return count;
        };
        for (task_id, entry) in drained {
            crate::metrics::inc_failover_node_lost_tasks();
            tracing::warn!(
                task_id = %task_id,
                node = %dead.as_str(),
                workflow_id = %entry.workflow_id.as_str(),
                in_flight_ms = entry.forwarded_at.elapsed().as_millis() as u64,
                "executor node lost; settling in-flight task as failed"
            );
            // 错误 kind 复用既有的 `worker`（执行侧基础设施失败），不新增 kind：
            // 新增 kind 需同时改 `ActorErrorKind` 与 Python `_KIND_TO_EXCEPTION`
            // 两处，而语义上"执行节点消失"本属执行侧失败。
            // 前缀经 `format_error_kind` 生成，避免手写格式漂移。
            let completion = TaskCompletion::Failed {
                workflow_id: entry.workflow_id,
                task_id: TaskId::from(task_id),
                task_name: entry.task_name,
                error: crate::common::format_error_kind(
                    "worker",
                    &format!(
                        "executor node {} was lost before the task produced a result; \
                         the task is settled as failed and is NOT re-run automatically",
                        dead.as_str()
                    ),
                ),
                target_node: Some(dead.clone()),
            };
            event_bus.publish(BusEvent::TaskFailed(completion));
        }
        count
    }

    /// 订阅 failover 相关 gossip topic。
    ///
    /// # Errors
    ///
    /// 如果底层网络订阅任一 topic 失败，返回错误。
    pub async fn subscribe_topics(&self) -> Result<()> {
        self.network.subscribe(TOPIC_HEARTBEAT).await?;
        self.network.subscribe(TOPIC_FAILOVER).await?;
        self.network.subscribe(TOPIC_HEADS).await?;
        self.network
            .subscribe(crate::common::TOPIC_DAG_STATE)
            .await?;
        Ok(())
    }

    /// 广播本节点心跳、活跃 workflow 与可用容量。
    ///
    /// # Errors
    ///
    /// 如果查询本地 workflow 状态、序列化 heartbeat 或网络广播失败，返回错误。
    pub async fn send_heartbeat(&self) -> Result<()> {
        let active_workflows = self.active_workflow_ids().await?;
        let now_ms = crate::common::epoch_millis();
        let endpoint_addr = self
            .network
            .listen_addresses()
            .ok()
            .map(|a| a.endpoint_addr);
        let hb = NodeHeartbeat {
            node_id: self.node_id.clone(),
            active_workflows,
            timestamp_ms: now_ms,
            available_slots: self.local_available_capacity.load(Ordering::Relaxed),
            max_slots: self.local_max_capacity.load(Ordering::Relaxed),
            endpoint_addr,
            platform: self.platform.clone(),
            labels: self.labels.clone(),
            signature: None,
        };
        // 节点记录签名：持有身份密钥时对签名域 ed25519 签名。签名失败不阻塞
        // 心跳（require_signed_records 的接收方将拒绝，问题显式暴露）。
        let mut hb = hb;
        if let Some(ref key) = self.signing_key {
            hb.signature = Some(key.sign(&Self::signing_payload(&hb)).to_bytes().to_vec());
        }
        let msg = WireMessage::NodeHeartbeat(hb);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        tracing::debug!(
            "sending heartbeat from {} to topic {}",
            self.node_id.0,
            TOPIC_HEARTBEAT
        );
        let result = self.network.broadcast(TOPIC_HEARTBEAT, data).await;
        if let Err(ref e) = result {
            tracing::warn!("heartbeat broadcast failed: {}", e);
        }
        if result.is_ok() {
            crate::metrics::inc_heartbeats_sent();
        }
        result
    }

    pub fn get_peer_infos(&self) -> HashMap<NodeId, PeerInfo> {
        self.peers
            .iter()
            .map(|ref_multi| (ref_multi.key().clone(), PeerInfo::from(ref_multi.value())))
            .collect()
    }

    /// 返回当前在线的 peer 视图（节点可见性 N2）。
    ///
    /// 在线判定复用心跳新鲜度语义：距上次心跳超过 `failure_timeout_ms` 或
    /// 从未收到心跳的节点不出现在结果中。返回值含节点元数据（slots/labels/
    /// platform），是面板与资源核算类第 3 层应用的数据源。
    pub fn peers(&self) -> Vec<PeerInfo> {
        let now_ms = crate::common::epoch_millis();
        self.peers
            .iter()
            .filter(|entry| {
                let last = entry.value().last_heartbeat_ms;
                last > 0 && now_ms.saturating_sub(last) <= self.failure_timeout_ms
            })
            .map(|entry| PeerInfo::from(entry.value()))
            .collect()
    }

    pub fn remove_peer(&self, node_id: &NodeId) {
        self.peers.remove(node_id);
    }

    /// Remove peers whose last heartbeat exceeds the failure timeout and return their info.
    /// Also decrements the connected_peers gauge for each removed peer.
    pub fn expire_stale_peers(&self) -> Vec<(NodeId, PeerInfo)> {
        let now_ms = crate::common::epoch_millis();
        let timeout_ms = self.failure_timeout_ms;
        let stale: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|ref_multi| {
                let state = ref_multi.value();
                state.last_heartbeat_ms > 0
                    && now_ms.saturating_sub(state.last_heartbeat_ms) > timeout_ms
            })
            .map(|ref_multi| ref_multi.key().clone())
            .collect();
        let mut removed = Vec::new();
        for node_id in &stale {
            if let Some((_, state)) = self.peers.remove(node_id) {
                crate::metrics::dec_connected_peers();
                removed.push((node_id.clone(), PeerInfo::from(&state)));
            }
        }
        removed
    }

    /// 声明本节点接管指定 workflow。
    ///
    /// # Errors
    ///
    /// 如果本地 `WorkflowActor` adopt 失败、租约持久化失败、claim 序列化失败
    /// 或网络广播失败，返回错误。
    pub async fn claim_workflow(&self, workflow_id: &WorkflowId) -> Result<()> {
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();
        let lease_duration_ms = self.lease_duration_ms;
        if let Some(existing) = self.leases.get(workflow_id) {
            if existing.is_valid(now_ms, now_monotonic) {
                if existing.node_id == self.node_id {
                    return Ok(());
                }
                // 租约仍有效且不属于本节点：直接退让，不通过字典序抢占。
                // 故障转移的仲裁统一由调用方 `should_claim_workflow`（一致性哈希）决定，
                // 避免两种策略互相矛盾导致脑裂。
                tracing::info!(
                    "workflow {} already claimed by {} with valid lease, deferring",
                    workflow_id.0,
                    existing.node_id.0
                );
                return Ok(());
            }
        }
        let lease = LeaseEntry::new_with_monotonic(
            self.node_id.clone(),
            now_ms,
            now_ms + lease_duration_ms,
            lease_duration_ms,
        );

        // 先 adopt workflow，成功后再持久化 lease，避免 adopt 失败但 lease 已写入
        self.adopt_workflow(workflow_id).await?;

        self.persist_lease(workflow_id, &lease)?;
        self.leases.insert(workflow_id.clone(), lease);

        crate::metrics::inc_failover_claims();

        let claim = OrchestratorClaim {
            node_id: self.node_id.clone(),
            workflow_id: workflow_id.clone(),
            timestamp_ms: now_ms,
        };
        let msg = WireMessage::OrchestratorClaim(claim);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg))
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        self.network.broadcast(TOPIC_FAILOVER, data).await
    }

    /// 处理远端节点广播的 workflow claim。
    ///
    /// 远端 claim 会更新本地租约表；如果 claim 不属于本节点，本地会移除该 workflow
    /// 的 active 状态以避免双主推进。
    pub async fn handle_claim(&self, claim: &OrchestratorClaim) {
        let lease_duration_ms = self.lease_duration_ms;

        // 使用 claimant 的时间戳作为基准，使租期时长
        // 在所有节点上一致，不受时钟偏差 / 网络
        // latency 影响。若使用接收方本地时间，
        // 会因网络传输时间而缩短租期。
        // 单调 deadline 以本节点接收时刻为起点，避免 NTP 跳变误判（M5-2）。
        let lease = LeaseEntry::new_with_monotonic(
            claim.node_id.clone(),
            claim.timestamp_ms,
            claim.timestamp_ms + lease_duration_ms,
            lease_duration_ms,
        );
        if let Err(e) = self.persist_lease(&claim.workflow_id, &lease) {
            tracing::error!("failed to persist lease for {}: {}", claim.workflow_id.0, e);
        }
        self.leases.insert(claim.workflow_id.clone(), lease);

        if claim.node_id != self.node_id {
            if let Err(e) = self.remove_active_workflow(&claim.workflow_id).await {
                tracing::warn!(error = %e, workflow_id = %claim.workflow_id.as_str(), "failover: failed to remove active workflow");
            }
            tracing::info!(
                "node {} claimed workflow {}, removed from local active set",
                claim.node_id.0,
                claim.workflow_id.0
            );
        }
    }

    pub async fn reschedule_workflow_tasks(&self, workflow_id: &WorkflowId) -> Result<()> {
        let result = self
            .call_workflow(workflow_methods::RESCHEDULE_RUNNING_TASKS, workflow_id)
            .await?;
        let tasks: Vec<crate::common::TaskDefinition> = messaging::decode(&result.payload)?;

        for task_def in &tasks {
            crate::metrics::inc_failover_reschedules();

            let hlc_ts = self.clock.tick();
            let update = crate::common::WireDagStateUpdate {
                workflow_id: workflow_id.clone(),
                task_id: task_def.id.clone(),
                task_state: crate::common::WireTaskState::Failed {
                    error: "original orchestrator failed, rescheduling".into(),
                },
                hlc_timestamp: hlc_ts,
                origin_node: self.node_id.clone(),
            };
            let msg = WireMessage::DagStateUpdate(update);
            if let Ok(data) = postcard::to_allocvec(&WireEnvelope::wrap(msg)) {
                if self
                    .network
                    .broadcast(crate::common::TOPIC_DAG_STATE, data)
                    .await
                    .is_err()
                {
                    tracing::warn!("failed to broadcast dag state update");
                }
            }

            // 通过 scheduler 入队；不可用时记录警告
            let sched_opt = self.scheduler.lock().clone();
            if let Some(sched) = sched_opt {
                if let Err(e) = sched.enqueue(task_def.clone()).await {
                    tracing::warn!(
                        "scheduler rejected rescheduled task {}/{}: {}",
                        workflow_id.0,
                        task_def.id.0,
                        e
                    );
                }
            } else {
                tracing::warn!(
                    "no scheduler set on FailoverManager, cannot enqueue rescheduled task {}/{}",
                    workflow_id.0,
                    task_def.id.0
                );
            }
        }

        Ok(())
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval_ms
    }

    pub fn failure_timeout_ms(&self) -> u64 {
        self.failure_timeout_ms
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// 返回所有活跃租约（workflow_id, node_id, claimed_at_ms, expires_at_ms）。
    pub fn active_leases(&self) -> Vec<(String, String, u64, u64)> {
        self.leases
            .iter()
            .map(|ref_multi| {
                let wf_id = ref_multi.key();
                let lease = ref_multi.value();
                (
                    wf_id.0.clone(),
                    lease.node_id.0.clone(),
                    lease.claimed_at_ms,
                    lease.expires_at_ms,
                )
            })
            .collect()
    }

    /// 更新本地节点的容量信息（由工作线程调用，当运行任务数量变化时）。
    pub fn update_local_capacity(&self, available: u32, max: u32) {
        self.local_available_capacity
            .store(available, Ordering::Relaxed);
        self.local_max_capacity.store(max, Ordering::Relaxed);
    }

    /// Update a peer's capacity snapshot.
    pub fn update_peer_capacity(&self, node_id: NodeId, available: u32, max: u32) {
        if let Some(mut peer) = self.peers.get_mut(&node_id) {
            peer.available_slots = available;
            peer.max_slots = max;
        }
    }

    /// Returns a snapshot of all peer capacities for task routing.
    pub fn get_peer_capacities(
        &self,
    ) -> std::collections::HashMap<NodeId, (u32, u32, Option<String>)> {
        self.peers
            .iter()
            .map(|ref_multi| {
                (
                    ref_multi.key().clone(),
                    (
                        ref_multi.value().available_slots,
                        ref_multi.value().max_slots,
                        ref_multi.value().endpoint_addr.clone(),
                    ),
                )
            })
            .collect()
    }

    /// 启动心跳发送和故障检测循环。
    /// 返回一个取消发送器；发送一个 `true` 会停止两个循环。
    ///
    /// 接收 `Arc<Self>` 以便后台任务持有共享引用。
    pub fn start_background_loops(self: Arc<Self>) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        // Heartbeat 循环
        let failover_hb = self.clone();
        let mut hb_cancel = cancel_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(
                failover_hb.heartbeat_interval_ms,
            ));
            let mut hb_count: u64 = 0;
            loop {
                tokio::select! {
                    _ = hb_cancel.changed() => break,
                    _ = interval.tick() => {
                        hb_count += 1;
                        if let Err(e) = failover_hb.send_heartbeat().await {
                            tracing::warn!("background heartbeat #{} failed: {}", hb_count, e);
                        }
                    }
                }
            }
        });

        // Failover 检测循环
        let failover_fd = self.clone();
        let mut fd_cancel = cancel_rx.clone();
        let lease_check_interval_secs = failover_fd.lease_expiry_check_interval_secs;
        tokio::spawn(async move {
            let check_interval = failover_fd.failure_timeout_ms / 2;
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(check_interval));
            let mut lease_interval =
                tokio::time::interval(std::time::Duration::from_secs(lease_check_interval_secs));
            loop {
                tokio::select! {
                    _ = fd_cancel.changed() => break,
                    _ = interval.tick() => {
                        failover_fd.detect_and_claim_failed_nodes().await;
                    }
                    _ = lease_interval.tick() => {
                        failover_fd.expire_leases().await;
                    }
                }
            }
        });

        cancel_tx
    }

    /// 检测失效 peer 并按一致性哈希接管 orphan workflow。
    ///
    /// 先把失联 peer 清出视图并取得其最后快照：孤儿 workflow 列表来自快照，
    /// 而接管选举的候选集合只包含存活节点（活跃 peer + 本节点）——否则孤儿
    /// workflow 可能被哈希给已失联的节点，永远无人接管。
    ///
    /// 单个 workflow 接管或重调度失败只记录错误并继续处理其他 workflow，避免一个
    /// 损坏状态阻塞整批故障恢复。
    pub async fn detect_and_claim_failed_nodes(&self) {
        let stale = self.expire_stale_peers();
        if !stale.is_empty() {
            tracing::info!(
                removed = stale.len(),
                "expired stale peers before failure detection"
            );
        }
        let now_ms = crate::common::epoch_millis();
        let timeout_ms = self.failure_timeout_ms();
        let my_id = &self.node_id;
        // 选举候选集合：仅存活节点（expire 后视图中的 peer + 本节点）。
        let candidate_ids: Vec<String> = {
            let mut ids: Vec<_> = self.peers.iter().map(|e| e.key().0.clone()).collect();
            ids.push(my_id.0.clone());
            ids
        };

        // 待检测集合：失联快照 + 当前视图（覆盖边界时序下仍超时的 peer）。
        let mut to_check: Vec<(NodeId, PeerInfo)> = stale;
        for (node_id, info) in self.get_peer_infos() {
            to_check.push((node_id, info));
        }

        for (node_id, info) in &to_check {
            let is_failed = info.last_heartbeat_ms > 0
                && now_ms.saturating_sub(info.last_heartbeat_ms) > timeout_ms;
            if !is_failed {
                continue;
            }

            // 第 2 腿：无论该 peer 是否编排 workflow，都要处置转发到它
            // 身上的在途任务——死亡执行器的 active_workflows 为空，正是旧守卫
            // 让在途任务永久挂起的地方。
            let settled = self.fail_outbound_tasks_to(node_id).await;

            if info.active_workflows.is_empty() {
                // 纯执行器失联：没有孤儿 workflow，第 2 腿已处置完毕。
                if settled > 0 {
                    tracing::warn!(
                        node = %node_id.0,
                        settled,
                        "failed peer was an executor only; settled its in-flight tasks"
                    );
                }
                continue;
            }
            tracing::warn!(
                "detected failed node: {}, orphaned workflows: {:?}",
                node_id.0,
                info.active_workflows
                    .iter()
                    .map(|w| w.0.clone())
                    .collect::<Vec<_>>()
            );

            // 使用 per-workflow 一致性哈希将 claim 均匀分布到
            // 存活节点，而非将所有 workflow 发往
            // ID 最低的单个节点。
            for wf_id in &info.active_workflows {
                if should_claim_workflow(&wf_id.0, &my_id.0, candidate_ids.clone()) {
                    if let Err(e) = self.claim_workflow(wf_id).await {
                        tracing::error!("failed to claim workflow {}: {}", wf_id.0, e);
                        continue;
                    }
                    if let Err(e) = self.reschedule_workflow_tasks(wf_id).await {
                        tracing::error!(
                            "failed to reschedule tasks for workflow {}: {}",
                            wf_id.0,
                            e
                        );
                    }
                }
            }
        }

        // 第 2 腿之二：心跳视图不可判的在途目标走主动探测。
        // 放在逐 peer 扫描之后——已被判失联的 peer 其条目已在上文被清空，
        // 此处只处理"连心跳都没来得及被观测到"的残余目标。
        self.probe_outbound_targets().await;
    }

    /// 入站心跳的身份与信任校验。
    ///
    /// 依次应用（任一失败拒绝并 warn）：
    /// 1. 成员校验：`allowed_peer_ids` 非空时，心跳的 `endpoint_addr` 必须在
    ///    列表内（gossip 侧旁路的封闭；直连侧另有 ALPN 白名单）；
    /// 2. 签名校验：`require_signed_records` 开启时，心跳必须携带有效签名，
    ///    且签名公钥（`endpoint_addr` 解析出的 endpoint id）须与声称的
    ///    来源一致——节点无法伪造他人身份的节点记录。
    fn verify_heartbeat(&self, hb: &NodeHeartbeat) -> bool {
        if !self.allowed_peer_ids.is_empty() {
            let Some(ref addr) = hb.endpoint_addr else {
                tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: no endpoint_addr while allowlist is active");
                return false;
            };
            if !self.allowed_peer_ids.iter().any(|a| a == addr) {
                tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: peer not in allowlist");
                return false;
            }
        }
        if !self.require_signed_records {
            return true;
        }
        // 重放防御：签名覆盖 timestamp_ms，但接收方新鲜度用本地时钟——不校验
        // 发送方时间戳时，捕获的旧签名心跳可被无限重放，让死亡节点永久占据
        // peer 视图。容差 = failure_timeout_ms（与失联判定同窗；跨节点时钟
        // 偏差侵蚀该窗口的既有语义不变）。
        let now_ms = crate::common::epoch_millis();
        if now_ms.saturating_sub(hb.timestamp_ms) > self.failure_timeout_ms {
            tracing::warn!(
                node = %hb.node_id.0,
                ts = hb.timestamp_ms,
                "heartbeat rejected: sender timestamp older than failure_timeout (replay?)"
            );
            return false;
        }
        let (Some(sig_bytes), Some(ref addr)) = (&hb.signature, &hb.endpoint_addr) else {
            tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: unsigned while require_signed_records is on");
            return false;
        };
        let Ok(pk) = addr.parse::<iroh::EndpointId>() else {
            tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: endpoint_addr is not a valid endpoint id");
            return false;
        };
        let Ok(sig_arr) = <[u8; iroh::Signature::LENGTH]>::try_from(sig_bytes.as_slice()) else {
            tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: malformed signature");
            return false;
        };
        let sig = iroh::Signature::from_bytes(&sig_arr);
        match pk.verify(&Self::signing_payload(hb), &sig) {
            Ok(()) => true,
            Err(_) => {
                tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: signature verification failed");
                false
            }
        }
    }

    /// 处理远端心跳并更新 peer 视图。
    ///
    /// `last_heartbeat_ms` 记录**接收方本地时钟**的接收时刻而非发送方
    /// `timestamp_ms`：故障检测窗口由接收方度量，若使用发送方时钟，
    /// 跨节点时钟偏差会直接侵蚀/放大检测窗口（偏差大时误判失联或漏判）。
    pub fn handle_heartbeat(&self, hb: &NodeHeartbeat) {
        if hb.node_id != self.node_id && !self.verify_heartbeat(hb) {
            return;
        }
        if hb.node_id != self.node_id {
            tracing::debug!(
                "received heartbeat from {} with {} active workflows",
                hb.node_id.0,
                hb.active_workflows.len()
            );
            let is_new = !self.peers.contains_key(&hb.node_id);
            let received_at_ms = crate::common::epoch_millis();
            let mut peer = self.peers.entry(hb.node_id.clone()).or_insert(PeerState {
                node_id: hb.node_id.clone(),
                last_heartbeat_ms: 0,
                active_workflows: HashSet::new(),
                available_slots: 0,
                max_slots: 0,
                endpoint_addr: None,
                labels: BTreeMap::new(),
                platform: None,
            });
            peer.last_heartbeat_ms = received_at_ms;
            peer.active_workflows = hb.active_workflows.iter().cloned().collect();
            peer.available_slots = hb.available_slots;
            peer.max_slots = hb.max_slots;
            peer.endpoint_addr = hb.endpoint_addr.clone();
            // 入站标签与发送侧同限：超限整体置空，防止恶意 peer 用超大标签集
            // 经 PeerState 常驻与 peers() 输出放大带宽。
            peer.labels = if crate::common::model::node_labels_within_limit(&hb.labels) {
                hb.labels.clone()
            } else {
                tracing::warn!(
                    node = %hb.node_id.0,
                    "heartbeat labels exceed {} bytes; ignoring them",
                    crate::common::model::NODE_LABELS_MAX_BYTES
                );
                BTreeMap::new()
            };
            peer.platform = hb.platform.clone();
            if is_new {
                crate::metrics::inc_connected_peers();
            }
        }
    }

    pub async fn expire_leases(&self) {
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();
        let active = match self.active_workflow_ids().await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "failover: failed to get active workflow ids");
                return;
            }
        };
        let active_set: HashSet<WorkflowId> = active.into_iter().collect();

        // 租约仲裁语义：本节点活跃 workflow 即续租。
        //
        // - 本节点持有、workflow 仍在本节点 active_set：**无条件续租**——本地
        //   延长到期时间并持久化，不走 claim→adopt→广播→全 peer persist 的
        //   重选路径，消除每个 lease_duration 周期的写放大；也避免过期后参与
        //   接管选举、输给新加入 peer 时出现租约无人持有的窗口（分区自愈）。
        //
        //   反双主依赖 handle_claim 的时序契约：远端节点 claim 成功后广播
        //   claim，本节点收到即 remove_active_workflow，workflow 退出
        //   active_set，下一轮 expire_leases 不再为其续租，旧主让位。claim
        //   与心跳走同一传输通道；若 claim 通知丢失，claimer 会在后续
        //   detect_and_claim_failed_nodes 循环中重新 claim，窗口由
        //   `lease_duration_ms > failure_timeout_ms` 的配置约束兜底。
        //
        // - 其余过期租约（非本节点持有，或 workflow 已不活跃）：移除。
        //   过期失效路径仅对非活跃 workflow 生效。
        let mut lapsed_own_active: Vec<WorkflowId> = Vec::new();
        let mut expired: Vec<WorkflowId> = Vec::new();
        for entry in self.leases.iter() {
            let (wf_id, lease) = (entry.key(), entry.value());
            if lease.is_valid(now_ms, now_monotonic) {
                continue;
            }
            if lease.node_id == self.node_id && active_set.contains(wf_id) {
                lapsed_own_active.push(wf_id.clone());
            } else {
                expired.push(wf_id.clone());
            }
        }

        for wf_id in &expired {
            self.remove_lease(wf_id);
        }

        for wf_id in &lapsed_own_active {
            let renewed = LeaseEntry::new_with_monotonic(
                self.node_id.clone(),
                now_ms,
                now_ms + self.lease_duration_ms,
                self.lease_duration_ms,
            );
            // 持久化失败保留旧租约记录，下一轮 expire_leases 重试续租。
            if let Err(e) = self.persist_lease(wf_id, &renewed) {
                tracing::warn!(
                    workflow = %wf_id.0,
                    error = %e,
                    "failed to persist renewed lease for active workflow"
                );
                continue;
            }
            self.leases.insert(wf_id.clone(), renewed);
            tracing::debug!(
                workflow = %wf_id.0,
                "renewed lapsed lease for active workflow owned by this node"
            );
        }
    }

    /// 从内存与持久化存储中移除租约。
    fn remove_lease(&self, wf_id: &WorkflowId) {
        self.leases.remove(wf_id);
        if let Some(ref store) = self.store {
            let key = format!("{}{}", STORE_KEY_LEASE, wf_id.0);
            if let Err(e) = store.delete(&key) {
                tracing::warn!(
                    "failed to delete expired lease for workflow {}: {}",
                    wf_id.0,
                    e
                );
            }
        }
    }

    fn persist_lease(&self, workflow_id: &WorkflowId, lease: &LeaseEntry) -> Result<()> {
        if let Some(ref store) = self.store {
            let persisted = PersistedLease {
                node_id: lease.node_id.clone(),
                claimed_at_ms: lease.claimed_at_ms,
                expires_at_ms: lease.expires_at_ms,
            };
            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&persisted)
                .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
            let key = format!("{}{}", STORE_KEY_LEASE, workflow_id.0);
            store.put(&key, &data)?;
        }
        Ok(())
    }

    fn recover_leases_from_store(&self) {
        if let Some(ref store) = self.store {
            let entries = match store.scan_prefix(STORE_KEY_LEASE) {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!("failed to scan leases from store: {}", e);
                    return;
                }
            };
            let now_ms = crate::common::epoch_millis();
            for (key, data) in entries {
                let wf_id_str = key.strip_prefix(STORE_KEY_LEASE).unwrap_or(&key);
                let wf_id = WorkflowId(wf_id_str.to_string());
                match rkyv::from_bytes::<PersistedLease, rkyv::rancor::Error>(&data) {
                    Ok(persisted) => {
                        if now_ms < persisted.expires_at_ms {
                            // 从持久化恢复的租约无单调基线，回退墙钟比较。
                            self.leases.insert(
                                wf_id,
                                LeaseEntry::restored(
                                    persisted.node_id,
                                    persisted.claimed_at_ms,
                                    persisted.expires_at_ms,
                                ),
                            );
                        } else if let Err(e) = store.delete(&key) {
                            tracing::warn!("failed to delete expired lease {}: {}", wf_id_str, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("corrupt lease entry {}: {:?}", wf_id_str, e);
                        if let Err(e) = store.delete(&key) {
                            tracing::warn!("failed to delete corrupt lease {}: {}", wf_id_str, e);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../../../tests/rust/unit/runtime/workflow/failover.rs"]
mod tests;
