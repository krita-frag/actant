//! 基于心跳与租约的工作流故障转移。
//!
//! [`FailoverManager`] 维护心跳、可用槽位、活跃 workflow 与本地租约；节点失效后
//! 由健康节点通过一致性哈希选主并 claim 接管。时间参数约束、两条失联处置腿
//! 与租约持久化语义见 `docs/architecture/failover.md`。
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::{
    should_claim_workflow, ActorId, FailoverConfig, NodeHeartbeat, NodeId, OrchestratorClaim,
    PlatformInfo, Result, SignedOrchestratorClaim, TaskCompletion, TaskDefinition, TaskId,
    WireEnvelope, WireMessage, WorkflowId, STORE_KEY_LEASE, TOPIC_FAILOVER, TOPIC_HEADS,
    TOPIC_HEARTBEAT,
};
use crate::runtime::actor::ActorSystem;
use crate::runtime::events::{TaskLifecycleEvent, TaskResultFeed};
use crate::runtime::state::{HybridLogicalClock, LmdbStore};
use crate::runtime::workflow::actor::workflow_methods;
use crate::runtime::workflow::messaging;
use crate::runtime::workflow::route::RouteCandidate;

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
    /// 对端编排器最近一次心跳声明的活跃工作流。
    ///
    /// 失联判定发生在此字段最后一次被刷新的时刻之后，因此它等价于"对端失联
    /// 时仍在驱动的工作流"，是接管的对象清单。
    active_workflows: HashSet<WorkflowId>,
    available_slots: u32,
    max_slots: u32,
    endpoint_addr: Option<String>,
    labels: BTreeMap<String, String>,
    platform: Option<PlatformInfo>,
    /// 对端各命名配额的剩余容量（最近一次心跳快照）。
    available_quotas: BTreeMap<String, u32>,
    /// 对端拓扑角色（用于路由排除 orchestrator）。
    role: crate::common::NodeRole,
    /// 对端是否正在 draining。
    draining: bool,
}

/// 已转发到远端 peer、结果尚未回来的在途任务条目。
///
/// 保存任务快照是为了租约到期后由 origin 重新入队——重投必须有完整载荷。
/// 登记表条目随结果回传或重投移除，不会长期驻留。
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
    /// 探测豁免：失联处置时被归还给 origin 的直提条目置位。其存活性由
    /// origin 的租约检查点兜底，探测无信息增量——豁免使后续每轮检测腿
    /// 跳过对已知死目标的重复 2s 直连超时。
    pub probe_exempt: bool,
    /// 转发时的任务快照。直提任务租约到期后用它重新入队；编排任务仅用于
    /// 失败完成事件字段。
    pub task: TaskDefinition,
}

/// 对外暴露的 peer 视图（节点可见性）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub last_heartbeat_ms: u64,
    /// 对端最近一次心跳声明的活跃工作流（接管时的孤儿清单）。
    #[serde(default)]
    pub active_workflows: HashSet<WorkflowId>,
    pub available_slots: u32,
    pub max_slots: u32,
    pub endpoint_addr: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub platform: Option<PlatformInfo>,
    /// 对端各命名配额的剩余容量（心跳快照）。
    #[serde(default)]
    pub available_quotas: BTreeMap<String, u32>,
    /// 对端拓扑角色。
    #[serde(default)]
    pub role: crate::common::NodeRole,
    /// 对端是否正在 draining。
    #[serde(default)]
    pub draining: bool,
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
            available_quotas: state.available_quotas.clone(),
            role: state.role,
            draining: state.draining,
        }
    }
}

/// `claim_workflow` 的裁决结果。
///
/// 调用方必须区分「已接管」与「退让」：两者都不是错误，但只有前者允许重调度
/// 该工作流的在途任务。合并成 `Ok(())` 会让退让方也去重调度，而它并不持有
/// 该工作流。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// 本节点新建租约并广播 claim，已接管。
    Adopted,
    /// 本节点原本就持有有效租约，无需重复 claim。
    AlreadyOwned,
    /// 租约仍被其它节点有效持有，本节点退让。
    Deferred,
    /// 本节点角色不具备编排能力，未参与 claim。
    NotOrchestrator,
}

struct LeaseEntry {
    node_id: NodeId,
    claimed_at_ms: u64,
    expires_at_ms: u64,
    /// 单调时钟租约到期时刻。``Some`` 时优先用于过期判定，避免 NTP 时钟跳变
    /// 导致误判。``None`` 表示租约从持久化恢复（无单调基线），
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
    result_feed: Option<TaskResultFeed>,
    /// 本节点平台信息，随心跳广播。
    platform: Option<PlatformInfo>,
    /// 本节点用户自定义标签，随心跳广播。运行时可整体替换。
    labels: parking_lot::RwLock<BTreeMap<String, String>>,
    /// 本节点拓扑角色。`Worker` 不参与 claim，也不会接管失联节点的 workflow。
    role: crate::common::NodeRole,
    /// 本节点是否正在 draining，随心跳广播。
    draining: AtomicBool,
    /// 节点身份密钥（endpoint keypair）。`Some` 时心跳以私钥签名。
    signing_key: Option<iroh::SecretKey>,
    /// `true` 时拒绝缺签/坏签的入站心跳（身份与信任）。
    require_signed_records: bool,
    /// 允许加入集群的 iroh endpoint id（z32 字符串）。空 = 不校验成员资格。
    allowed_peer_ids: Vec<String>,
    /// 故障注入：本节点忽略其心跳的 peer（模拟单向网络分区）。
    blocked_peers: Arc<DashMap<NodeId, ()>>,
    /// 故障注入：心跳静默截止时刻（epoch ms）。之前不广播心跳。
    heartbeat_paused_until_ms: Arc<AtomicU64>,
    /// 本节点命名配额计数器（用于心跳广播剩余容量）。由 builder 在 Worker
    /// 创建后注入；`None` 时广播空表（等价"不提供任何配额"）。
    local_quota_tracker: Arc<parking_lot::Mutex<Option<Arc<crate::runtime::quota::QuotaTracker>>>>,
    /// 路由候选快照版本：成员或容量变更时递增，驱动缓存重建。
    peers_version: AtomicU64,
    /// 路由候选缓存：`(版本, 候选列表)`。
    ///
    /// 构建在锁外完成，写锁只替换指针；读取命中时仅做 `Arc::clone`。
    /// 缓存放这里是因为候选完全由 `peers` 派生，与成员状态同生命周期，
    /// 避免跨层失效通知。
    route_snapshot: parking_lot::RwLock<Option<(u64, Arc<Vec<RouteCandidate>>)>>,
    /// 最近一次成功查询到的活跃 workflow 清单。
    ///
    /// 心跳查询失败时沿用该清单，避免发送空清单让 peers 误以为本节点没有
    /// 孤儿 workflow（随后失联将无人接管）。
    last_active_workflows: Arc<parking_lot::RwLock<Vec<WorkflowId>>>,
    /// 重调度入队失败的任务，按 task_id 保留以便逐轮重试。
    pending_reschedules: Arc<DashMap<TaskId, crate::common::TaskDefinition>>,
    /// 待接管工作流：本轮被选举接管、但因租约仍被原 owner 有效持有而退让。
    ///
    /// 退让不能是终点。触发退让的 peer 当轮已被 `expire_stale_peers` 移出视图，
    /// 后续轮次不再扫描它；而租约到期后 `expire_leases` 会把条目删掉。若退让即
    /// 作罢，工作流此后永久无人驱动。键是工作流，值是原 owner（用于判断它是否
    /// 复活）。条目在接管成功或原 owner 复活时移除，因此不会无限累积。
    pending_claims: DashMap<WorkflowId, NodeId>,
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
            result_feed: None,
            platform: None,
            labels: parking_lot::RwLock::new(BTreeMap::new()),
            role: crate::common::NodeRole::default(),
            draining: AtomicBool::new(false),
            signing_key: None,
            require_signed_records: false,
            allowed_peer_ids: Vec::new(),
            blocked_peers: Arc::new(DashMap::new()),
            heartbeat_paused_until_ms: Arc::new(AtomicU64::new(0)),
            local_quota_tracker: Arc::new(parking_lot::Mutex::new(None)),
            peers_version: AtomicU64::new(0),
            route_snapshot: parking_lot::RwLock::new(None),
            last_active_workflows: Arc::new(parking_lot::RwLock::new(Vec::new())),
            pending_reschedules: Arc::new(DashMap::new()),
            pending_claims: DashMap::new(),
        };
        fm.recover_leases_from_store();
        fm
    }

    /// 注入节点身份与信任配置（身份与信任批）。
    ///
    /// - `signing_key`：本节点 endpoint 私钥，`Some` 时出站心跳签名；
    /// - `require_signed_records`：拒绝缺签/坏签的入站心跳；
    /// - `allowed_peer_ids`：非空时入站心跳的 endpoint id（取自完整
    ///   `endpoint_addr`）必须在列表内（gossip 侧成员校验；直连侧另有
    ///   ALPN 白名单）。
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

    /// claim 签名域：带长度前缀的 `(node_id, workflow_id, timestamp_ms)`。
    ///
    /// 使用长度前缀避免 `a|b` 与 `ab|` 之类拼接歧义；域标签隔离心跳签名。
    fn claim_signing_payload(claim: &OrchestratorClaim) -> Vec<u8> {
        let mut payload = b"actant/orchestrator-claim/v1".to_vec();
        let node = claim.node_id.as_str().as_bytes();
        payload.extend_from_slice(&(node.len() as u32).to_le_bytes());
        payload.extend_from_slice(node);
        let workflow = claim.workflow_id.as_str().as_bytes();
        payload.extend_from_slice(&(workflow.len() as u32).to_le_bytes());
        payload.extend_from_slice(workflow);
        payload.extend_from_slice(&claim.timestamp_ms.to_le_bytes());
        payload
    }

    /// 设置本节点拓扑角色。
    ///
    /// `Worker` 角色的节点永不参与 workflow claim / 故障接管；角色同时随
    /// 心跳广播，供其他节点的路由策略排除不执行任务的 orchestrator。
    pub fn with_role(mut self, role: crate::common::NodeRole) -> Self {
        self.role = role;
        self
    }

    /// 返回本节点拓扑角色。
    pub fn role(&self) -> crate::common::NodeRole {
        self.role
    }

    /// 应用运行时节点状态更新（标签 / draining）。
    ///
    /// 配额由共享的 `QuotaTracker` 承载，调用方在 [`crate::runtime::workflow::Worker`]
    /// 侧更新即可；此处只维护随心跳广播的标签与 draining 标志。
    /// 标签超限由调用方先行校验，此处直接整表替换。
    pub fn apply_node_state(&self, update: &crate::common::NodeStateUpdate) {
        if let Some(labels) = &update.labels {
            *self.labels.write() = labels.clone();
        }
        if let Some(draining) = update.draining {
            self.draining.store(draining, Ordering::Relaxed);
        }
    }

    /// 注入本节点元数据（平台信息 + 标签），随心跳广播。
    pub fn with_node_metadata(
        mut self,
        platform: Option<PlatformInfo>,
        labels: BTreeMap<String, String>,
    ) -> Self {
        self.platform = platform;
        self.labels =
            parking_lot::RwLock::new(if crate::common::model::node_labels_within_limit(&labels) {
                labels
            } else {
                tracing::warn!(
                    "node_labels exceed {} bytes; labels will not be advertised",
                    crate::common::model::NODE_LABELS_MAX_BYTES
                );
                BTreeMap::new()
            });
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

    /// 获取本地编排器当前的活跃 workflow ID 列表（用于租约续期）。
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

    /// 本节点是否有权执行该工作流的任务（派发前唯一闸口）。
    ///
    /// 判据只有一条：租约表里该工作流归谁。三种情形：
    /// - 无租约条目：本节点未参与租约协调（单机、或工作流从未被 claim），放行；
    /// - 租约归本节点：放行（含已过期但尚未被对端夺走——被夺走会立刻改写条目）；
    /// - 租约归对端：拒绝。这是「收到对端 claim 后只摘编排状态、执行层仍继续跑」
    ///   的第二道闸：只摘 slot 的话，已经出队（`inflight`/执行中）的任务仍会被
    ///   派发，副作用在新旧两个主上各发生一次；而完成回灌时 slot 已删、结果被
    ///   静默丢弃。排队中的任务由 `remove_workflow_tasks` 撤销，本条兜住已出队部分。
    pub fn dispatch_permitted(&self, workflow_id: &WorkflowId) -> bool {
        match self.leases.get(workflow_id) {
            Some(entry) => entry.node_id == self.node_id,
            None => true,
        }
    }

    /// 注入用于 failover 重调度的 scheduler。
    pub fn set_scheduler(&self, scheduler: Arc<dyn crate::runtime::workflow::Scheduler>) {
        *self.scheduler.lock() = Some(scheduler);
    }

    /// 注入本节点配额计数器：心跳随之广播各配额剩余容量。
    ///
    /// 采用 setter 而非构造参数，保持既有 `FailoverManager::new` 调用点不变；
    /// builder 在 Worker 创建后调用（Worker 持有同一 Arc）。
    pub fn set_quota_tracker(&self, tracker: Arc<crate::runtime::quota::QuotaTracker>) {
        *self.local_quota_tracker.lock() = Some(tracker);
    }

    /// 注入本地事件总线，用于失联时终结在途任务（见 `fail_outbound_tasks_to`）。
    ///
    /// 采用 builder 方法而非构造参数，是为了让既有 11 处
    /// `FailoverManager::new` 调用点（多为不需要该腿的单元测试桩）保持不变。
    pub fn with_result_feed(mut self, result_feed: TaskResultFeed) -> Self {
        self.result_feed = Some(result_feed);
        self
    }

    /// 登记一条"已转发到远端、结果未回"的在途任务。
    ///
    /// 由 Worker 在 `forward_remote_task` 成功后调用；同一 `task_id` 重复登记
    /// 覆盖旧值（重路由场景下目标是最后一次转发目标）。
    pub fn record_outbound(
        &self,
        task: &TaskDefinition,
        target_node: &NodeId,
        target_endpoint_addr: Option<&str>,
    ) {
        self.outbound.insert(
            task.id.as_str().to_string(),
            OutboundTask {
                workflow_id: task
                    .workflow_id
                    .clone()
                    .unwrap_or_else(|| WorkflowId::from(String::new())),
                task_name: task.name.clone(),
                target_node: target_node.clone(),
                target_endpoint_addr: target_endpoint_addr.map(str::to_string),
                forwarded_at: Instant::now(),
                probe_exempt: false,
                task: task.clone(),
            },
        );
    }

    /// 当前在途登记的任务 attempt（无登记返回 `None`）。
    pub(crate) fn outbound_attempt(&self, task_id: &str) -> Option<u32> {
        self.outbound.get(task_id).map(|entry| entry.task.attempt)
    }

    /// 取出并移除所有租约已失效的直提任务登记。
    ///
    /// 判定统一走 [`TaskDefinition::lease_expired_at`]：无有效租约（从未租出
    /// 或已被放弃）与已过截止时刻对检查点等效。编排任务不参与租约重投。
    pub fn take_expired_standalone(&self, now_ms: u64) -> Vec<(String, OutboundTask)> {
        let keys: Vec<String> = self
            .outbound
            .iter()
            .filter(|entry| {
                let task = &entry.value().task;
                task.workflow_id.is_none() && task.lease_expired_at(now_ms)
            })
            .map(|entry| entry.key().clone())
            .collect();
        let mut expired = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((_, entry)) = self.outbound.remove(&key) {
                expired.push((key, entry));
            }
        }
        expired
    }

    /// 取出并移除在途登记，除非结果比登记更旧（严格 fencing）。
    ///
    /// 用于直提任务的结果 fencing：租约重投后旧执行的迟到结果不得被接受，
    /// 也不得移除新 attempt 的登记。
    ///
    /// 判据是 `entry.attempt <= attempt` 而非相等——attempt 有两个独立的
    /// 推进者：origin 的租约重投，以及**执行节点自己的崩溃转移**
    /// （本地派发崩溃后 `attempt += 1` 并在同节点重投）。后者不经过 origin，
    /// origin 的登记仍停留在转发时的 attempt。若要求精确相等，执行节点自愈
    /// 后的结果会被误判为陈旧并丢弃，origin 随后还会按租约再投一次。
    pub fn take_outbound_if_not_stale(&self, task_id: &str, attempt: u32) -> Option<OutboundTask> {
        self.outbound
            .remove_if(task_id, |_, entry| entry.task.attempt <= attempt)
            .map(|(_, entry)| entry)
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
    /// 每次扫描通常零探测。`skip` 是本轮已被第 2 腿判死并处置过的节点：
    /// fail-fast 重插的直提条目目标仍是死节点，再探测无信息增量且白白消耗
    /// 一次 2s 超时。
    async fn probe_outbound_targets(&self, skip: &HashSet<NodeId>) {
        // 去重：同一目标可能承载多个在途任务，只探测一次。
        let mut targets: HashMap<NodeId, Option<String>> = HashMap::new();
        for entry in self.outbound.iter() {
            let target = entry.value().target_node.clone();
            // 探测豁免条目（已归还 origin 的直提任务）直接跳过：其目标大概率
            // 已死，每轮 2s 直连超时只会拖延同循环内的接管/仲裁/重调度腿。
            if entry.value().probe_exempt || skip.contains(&target) || self.is_peer_fresh(&target) {
                continue;
            }
            targets
                .entry(target)
                .or_insert_with(|| entry.value().target_endpoint_addr.clone());
        }
        // 并发探测：串行时 K 个不可达目标会让检测轮线性膨胀到 K×2s，且
        // 推迟同循环内的接管重试与重调度腿。结果统一在并发全部返回后处置。
        let probes = targets.into_iter().map(|(target, addr)| {
            let addr = addr.unwrap_or_else(|| target.as_str().to_string());
            async move {
                let probe = self
                    .network
                    .send_direct_request(&addr, crate::runtime::network::DirectRequest::Ping);
                let alive = matches!(
                    tokio::time::timeout(OUTBOUND_PROBE_TIMEOUT, probe).await,
                    Ok(Ok(crate::runtime::network::DirectResponse::Pong))
                );
                (target, addr, alive)
            }
        });
        for (target, addr, alive) in futures::future::join_all(probes).await {
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

    /// 失联处置第 2 腿：终结编排任务，直提任务转入租约重投。
    ///
    /// - 直提任务：把登记条目的租约置为立即到期，由 Worker 的租约检查点重新
    ///   入队（`attempt` 递增，旧执行结果被 fencing 拒绝）；
    /// - 编排任务：照常发布 `TaskFailed`，由事件泵回灌 orchestrator 决定重试。
    ///
    /// 返回终结为失败的任务数，供调用方决定是否需要打日志。
    async fn fail_outbound_tasks_to(&self, dead: &NodeId) -> usize {
        let drained = self.drain_outbound_to(dead);
        if drained.is_empty() {
            return 0;
        }
        let result_feed = self.result_feed.as_ref();
        let mut failed = 0usize;
        let mut resent = 0usize;
        // 无 result feed 时无法结算编排任务：把登记**放回**表里，下一轮再试。
        // 直接丢弃会让 origin 的编排任务永久停在 Running（没有终态事件、没有重试），
        // 而这是唯一一处能宣告「执行节点已失联」的地方。
        let mut unsettled = 0usize;
        for (task_id, mut entry) in drained {
            if entry.workflow_id.as_str().is_empty() {
                // 直提任务不在此处直接重投：放弃租约后由 origin 的租约检查点
                // （`expire_standalone_leases`）下一 tick 取走并重投——重投权在
                // origin 手上，这里只负责把任务交还。
                entry.task.relinquish_lease();
                entry.probe_exempt = true;
                self.outbound.insert(task_id, entry);
                resent += 1;
                continue;
            }
            let Some(result_feed) = result_feed else {
                self.outbound.insert(task_id, entry);
                unsettled += 1;
                continue;
            };
            crate::metrics::inc_failover_node_lost_tasks();
            tracing::warn!(
                task_id = %task_id,
                node = %dead.as_str(),
                workflow_id = %entry.workflow_id.as_str(),
                in_flight_ms = entry.forwarded_at.elapsed().as_millis() as u64,
                "executor node lost; settling in-flight workflow task as failed"
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
                         the orchestrated task is settled as failed and retried by the \
                         workflow policy",
                        dead.as_str()
                    ),
                ),
                target_node: Some(dead.clone()),
                attempt: entry.task.attempt,
            };
            result_feed.send(TaskLifecycleEvent::TaskFailed(completion));
            failed += 1;
        }
        if resent > 0 {
            tracing::warn!(
                node = %dead.as_str(),
                count = resent,
                "executor node lost; standalone tasks are queued for lease re-dispatch"
            );
        }
        if unsettled > 0 {
            tracing::error!(
                node = %dead.as_str(),
                count = unsettled,
                "no result feed on FailoverManager; in-flight tasks kept registered for a later pass"
            );
        }
        failed
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
        // 工作流状态同步：请求与响应都走全局话题（目标节点在载荷内）。
        // 用固定话题而非 `wf-state-req:<node>`：运行期新建的话题拿不到已连接
        // 的邻居，`broadcast` 返回成功却无人接收，跨节点状态复制会静默失效。
        self.network
            .subscribe(crate::common::Topic::workflow_state_req().as_str())
            .await?;
        self.network
            .subscribe(crate::common::Topic::workflow_state_resp().as_str())
            .await?;
        Ok(())
    }

    /// 广播本节点心跳、活跃 workflow 与可用容量。
    ///
    /// # Errors
    ///
    /// 如果查询本地 workflow 状态、序列化 heartbeat 或网络广播失败，返回错误。
    pub async fn send_heartbeat(&self) -> Result<()> {
        if crate::common::epoch_millis() < self.heartbeat_paused_until_ms.load(Ordering::Relaxed) {
            tracing::debug!(
                node = %self.node_id.0,
                "heartbeat suppressed by fault injection"
            );
            return Ok(());
        }
        // 活跃清单要随心跳上报（对端据此判定失联节点留下了哪些孤儿），但心跳
        // 本身不能因为这次查询失败而停摆：查询失败只降级为"清单缺失"，对端
        // 就看不到本节点名下的工作流，日志里可查。
        let active_workflows = match self.active_workflow_ids().await {
            Ok(ids) => {
                *self.last_active_workflows.write() = ids.clone();
                ids
            }
            Err(e) => {
                tracing::warn!(
                    node = %self.node_id.0,
                    error = %e,
                    "failed to list active workflows for heartbeat; reusing last known inventory"
                );
                self.last_active_workflows.read().clone()
            }
        };
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
            labels: self.labels.read().clone(),
            available_quotas: self
                .local_quota_tracker
                .lock()
                .as_ref()
                .map(|t| t.available_map())
                .unwrap_or_default(),
            signature: None,
            role: self.role,
            draining: self.draining.load(Ordering::Relaxed),
        };
        // 节点记录签名：持有身份密钥时对签名域 ed25519 签名。签名失败不阻塞
        // 心跳（require_signed_records 的接收方将拒绝，问题显式暴露）。
        let mut hb = hb;
        if let Some(ref key) = self.signing_key {
            hb.signature = Some(key.sign(&Self::signing_payload(&hb)).to_bytes().to_vec());
        }
        let msg = WireMessage::NodeHeartbeat(hb);
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg)?)
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

    /// 成员或容量变更后使路由候选缓存失效。
    fn bump_peers_version(&self) {
        self.peers_version.fetch_add(1, Ordering::AcqRel);
    }

    /// 返回路由候选快照；同一版本内复用同一份 `Arc`。
    ///
    /// 路由是每任务路径：命中缓存只做版本比较 + `Arc::clone`。O(N) 构建只在
    /// 成员/容量变更后的首次调用发生，且构建期间不持有缓存锁（写锁只换指针），
    /// 避免阻塞其他路由线程。只搬运路由所需字段，不克隆 `platform` 等
    /// 路由不用的数据。
    pub(crate) fn route_candidates(&self) -> Arc<Vec<RouteCandidate>> {
        let version = self.peers_version.load(Ordering::Acquire);
        {
            let cached = self.route_snapshot.read();
            if let Some((cached_version, snapshot)) = cached.as_ref() {
                if *cached_version == version {
                    return Arc::clone(snapshot);
                }
            }
        }
        let built: Arc<Vec<RouteCandidate>> = Arc::new(
            self.peers
                .iter()
                .map(|entry| {
                    let peer = entry.value();
                    RouteCandidate {
                        node_id: peer.node_id.clone(),
                        endpoint_addr: peer.endpoint_addr.clone(),
                        available_slots: peer.available_slots,
                        max_slots: peer.max_slots,
                        role: peer.role,
                        draining: peer.draining,
                        last_heartbeat_ms: peer.last_heartbeat_ms,
                        labels: peer.labels.clone(),
                        available_quotas: peer.available_quotas.clone(),
                    }
                })
                .collect(),
        );
        {
            let mut cached = self.route_snapshot.write();
            let stale = match cached.as_ref() {
                Some((cached_version, _)) => *cached_version < version,
                None => true,
            };
            if stale {
                *cached = Some((version, Arc::clone(&built)));
            }
        }
        built
    }

    /// 返回当前在线的 peer 视图（节点可见性）。
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
        self.bump_peers_version();
    }

    /// 故障注入：模拟本节点到 `node_id` 的单向网络分区。
    ///
    /// 立即从 peer 视图移除并忽略其后续心跳（直到 [`Self::unblock_peer`]）。
    /// 与真实断连的差异：本节点仍可向其发起直连请求，但心跳/容量视图不可见，
    /// 足以触发失联判定与副本接管。
    pub fn block_peer(&self, node_id: &NodeId) {
        self.blocked_peers.insert(node_id.clone(), ());
        if self.peers.remove(node_id).is_some() {
            crate::metrics::dec_connected_peers();
            self.bump_peers_version();
        }
        tracing::info!(node = %node_id.0, "fault injection: peer blocked");
    }

    /// 解除 [`Self::block_peer`] 施加的分区，允许再次接收其心跳。
    pub fn unblock_peer(&self, node_id: &NodeId) {
        self.blocked_peers.remove(node_id);
        tracing::info!(node = %node_id.0, "fault injection: peer unblocked");
    }

    /// 故障注入：在接下来的 `ms` 毫秒内不广播心跳。
    ///
    /// 传 0 立即恢复。用于模拟网络抖动/心跳延迟，观察对端失联判定。
    pub fn pause_heartbeat_for_ms(&self, ms: u64) {
        let until = crate::common::epoch_millis().saturating_add(ms);
        self.heartbeat_paused_until_ms
            .store(until, Ordering::Relaxed);
        tracing::info!(ms, "fault injection: heartbeat paused");
    }

    /// 故障注入：给本节点 HLC 施加时钟偏移（毫秒，可正可负）。
    pub fn inject_hlc_drift_ms(&self, ms: i64) {
        self.clock.inject_drift_ms(ms);
        tracing::info!(ms, "fault injection: HLC drift injected");
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
        if !removed.is_empty() {
            self.bump_peers_version();
        }
        removed
    }

    /// 声明本节点接管指定 workflow。
    ///
    /// 返回裁决而非 `()`：退让与接管都是成功，但只有接管才允许调用方重调度
    /// 任务。把两者压成 `Ok(())` 会让调用方在退让后照旧重调度——它操作的仍是
    /// 一份没人真正持有的本地状态，而真正的 owner 还在别处推进同一工作流。
    ///
    /// # Errors
    ///
    /// 如果本地 `WorkflowActor` adopt 失败、租约持久化失败、claim 序列化失败
    /// 或网络广播失败，返回错误。
    pub async fn claim_workflow(&self, workflow_id: &WorkflowId) -> Result<ClaimOutcome> {
        if !self.role.can_orchestrate() {
            tracing::debug!(
                workflow = %workflow_id.0,
                role = %self.role,
                "skipping workflow claim: node role does not orchestrate"
            );
            return Ok(ClaimOutcome::NotOrchestrator);
        }
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();
        let lease_duration_ms = self.lease_duration_ms;
        if let Some(existing) = self.leases.get(workflow_id) {
            if existing.is_valid(now_ms, now_monotonic) {
                if existing.node_id == self.node_id {
                    return Ok(ClaimOutcome::AlreadyOwned);
                }
                // 租约仍有效且不属于本节点：直接退让，不通过字典序抢占。
                // 故障转移的仲裁统一由调用方 `should_claim_workflow`（一致性哈希）决定，
                // 避免两种策略互相矛盾导致脑裂。
                tracing::info!(
                    "workflow {} already claimed by {} with valid lease, deferring",
                    workflow_id.0,
                    existing.node_id.0
                );
                return Ok(ClaimOutcome::Deferred);
            }
        }
        let lease = LeaseEntry::new_with_monotonic(
            self.node_id.clone(),
            now_ms,
            now_ms + lease_duration_ms,
            lease_duration_ms,
        );

        // 先落租约再 adopt：adopt 失败时回滚租约，避免“本地已接管但无租约”
        // 的双主窗口。广播失败不回滚——本地租约已有效，peers 可通过后续
        // 心跳/重试 claim 收敛。
        self.persist_lease(workflow_id, &lease)?;
        self.leases.insert(workflow_id.clone(), lease);

        if let Err(e) = self.adopt_workflow(workflow_id).await {
            self.remove_lease(workflow_id);
            return Err(e);
        }

        crate::metrics::inc_failover_claims();

        let claim = OrchestratorClaim {
            node_id: self.node_id.clone(),
            workflow_id: workflow_id.clone(),
            timestamp_ms: now_ms,
        };
        if let Err(e) = self.broadcast_claim(&claim).await {
            tracing::warn!(
                workflow = %workflow_id.0,
                error = %e,
                "claim lease persisted locally but broadcast failed; peers will converge via retry"
            );
        }
        Ok(ClaimOutcome::Adopted)
    }

    /// 构造（含签名）并广播一条 `SignedOrchestratorClaim`。
    ///
    /// 完整接管（`claim_workflow`）与分区恢复仲裁（`arbitrate_lease_overlaps`）
    /// 共用：两条路径的 claim 都必须经同一签名/封装/广播管线，避免旁路。
    async fn broadcast_claim(&self, claim: &OrchestratorClaim) -> Result<()> {
        let signature = self
            .signing_key
            .as_ref()
            .map(|key| {
                key.sign(&Self::claim_signing_payload(claim))
                    .to_bytes()
                    .to_vec()
            })
            .unwrap_or_default();
        let msg = WireMessage::SignedOrchestratorClaim(SignedOrchestratorClaim {
            claim: claim.clone(),
            signature,
        });
        let data = postcard::to_allocvec(&WireEnvelope::wrap(msg)?)
            .map_err(|e| crate::common::ActantError::Serialization(e.to_string()))?;
        self.network.broadcast(TOPIC_FAILOVER, data).await
    }

    /// 验签后处理远端 claim。
    ///
    /// 签名覆盖 `(node_id, workflow_id, timestamp_ms)`；`node_id` 本身是
    /// endpoint 公钥字符串，因此验证通过即证明发送者持有该身份私钥。
    pub async fn handle_signed_claim(&self, signed: &SignedOrchestratorClaim) {
        if signed.signature.is_empty() {
            if self.require_signed_records {
                tracing::warn!(
                    workflow = %signed.claim.workflow_id.0,
                    claimant = %signed.claim.node_id.0,
                    "rejecting unsigned claim while signed records are required"
                );
                return;
            }
            self.handle_claim(&signed.claim).await;
            return;
        }
        let Ok(peer_id) = signed.claim.node_id.0.parse::<iroh::EndpointId>() else {
            tracing::warn!(
                claimant = %signed.claim.node_id.0,
                "rejecting claim: node_id is not a valid endpoint identity"
            );
            return;
        };
        let Ok(sig_arr) = <[u8; iroh::Signature::LENGTH]>::try_from(signed.signature.as_slice())
        else {
            tracing::warn!(
                claimant = %signed.claim.node_id.0,
                "rejecting claim: malformed signature"
            );
            return;
        };
        let signature = iroh::Signature::from_bytes(&sig_arr);
        if peer_id
            .verify(&Self::claim_signing_payload(&signed.claim), &signature)
            .is_err()
        {
            tracing::warn!(
                workflow = %signed.claim.workflow_id.0,
                claimant = %signed.claim.node_id.0,
                "rejecting claim: signature verification failed"
            );
            return;
        }
        self.handle_claim(&signed.claim).await;
    }

    /// 处理远端节点广播的 workflow claim。
    ///
    /// 远端 claim 会更新本地租约表；如果 claim 不属于本节点，本地会移除该 workflow
    /// 的 active 状态以避免双主推进。
    pub async fn handle_claim(&self, claim: &OrchestratorClaim) {
        let lease_duration_ms = self.lease_duration_ms;
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();

        // claim 必须来自编排角色；已知 Worker 的 claim 一律拒绝。
        if let Some(peer) = self.peers.get(&claim.node_id) {
            if !peer.role.can_orchestrate() {
                tracing::warn!(
                    workflow = %claim.workflow_id.0,
                    claimant = %claim.node_id.0,
                    role = %peer.role,
                    "rejecting claim from non-orchestrator peer"
                );
                return;
            }
        }
        // 签名模式下，claimant 必须已通过签名心跳建立身份且仍在失败检测窗口内，
        // 防止未知节点凭一个裸 claim 移除本地 active workflow。
        if self.require_signed_records {
            let known = self.peers.get(&claim.node_id).is_some_and(|peer| {
                peer.role.can_orchestrate()
                    && now_ms.saturating_sub(peer.last_heartbeat_ms) <= self.failure_timeout_ms
            });
            if !known {
                tracing::warn!(
                    workflow = %claim.workflow_id.0,
                    claimant = %claim.node_id.0,
                    "rejecting claim from unknown or stale identity"
                );
                return;
            }
        }

        // 未来时间戳上界：claim 时间戳超前本地时钟超过 failure_timeout
        // （与心跳新鲜度窗口同源的偏移容差）一律拒绝。无上界时，恶意或严重
        // 时钟偏移节点发出的 `now + 一年` claim 会在无有效租约分支被接受，
        // 持久化的 `expires_at_ms` 随之巨大；节点重启后走墙钟恢复
        // （`recover_leases_from_store`，无单调 deadline），该租约"永久有效"，
        // 任何正常时间戳的 claim 都无法胜出——工作流接管被单条消息锁死。
        if claim.timestamp_ms.saturating_sub(now_ms) > self.failure_timeout_ms {
            tracing::warn!(
                workflow = %claim.workflow_id.0,
                claimant = %claim.node_id.0,
                claim_ts = claim.timestamp_ms,
                now_ms,
                "rejecting claim with future timestamp beyond drift tolerance"
            );
            return;
        }

        // claim 必须按 (timestamp_ms, node_id) 全序仲裁，不能无条件覆盖。
        // 否则延迟/重放的旧 claim 会夺走正在驱动的 workflow；并发 claim
        // 也可能让双方互相让位，最终无人驱动。
        //
        // 租约过期/不存在时接受任意时间戳的 claim 是**持久化模型的一部分**
        // （过期租约可写入、由恢复期 lease_expired_at 清理，见
        // leases_survive_manager_rebuild_from_store），但这也意味着签名
        // 关闭的开放部署里，被重放的陈旧 claim 可在过期窗口内写入租约表；
        // require_signed_records 模式下有 claimant 心跳新鲜度校验兜底。
        //
        // 仲裁与写入在 DashMap entry 的分片写锁内一次完成：check-then-insert
        // 两步分离时，并发 claim(ts=200/210) 都可能基于旧租约判胜、后写者胜，
        // 旧 claim 覆盖新 claim，仲裁全序被破坏。胜出时在锁内写入租约，锁外
        // 仅补落盘（持久化失败不回滚内存侧租约，与既有语义一致）。
        let lease_to_persist: Option<LeaseEntry> =
            match self.leases.entry(claim.workflow_id.clone()) {
                dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                    let existing = occupied.get();
                    let supersedes = if existing.is_valid(now_ms, now_monotonic) {
                        claim.timestamp_ms > existing.claimed_at_ms
                            || (claim.timestamp_ms == existing.claimed_at_ms
                                && claim.node_id.0 > existing.node_id.0)
                    } else {
                        true
                    };
                    if supersedes {
                        let lease = Self::claim_lease_entry(claim, lease_duration_ms);
                        occupied.insert(lease.clone());
                        Some(lease)
                    } else {
                        None
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(vacant) => {
                    let lease = Self::claim_lease_entry(claim, lease_duration_ms);
                    vacant.insert(lease.clone());
                    Some(lease)
                }
            };
        let Some(lease) = lease_to_persist else {
            tracing::debug!(
                workflow = %claim.workflow_id.0,
                claimant = %claim.node_id.0,
                claim_ts = claim.timestamp_ms,
                "ignoring stale or older orchestrator claim"
            );
            return;
        };
        // 使用 claimant 的时间戳作为基准，使租期时长在所有节点上一致，不受
        // 时钟偏差 / 网络 latency 影响。单调 deadline 以本节点接收时刻为起点，
        // 避免 NTP 跳变误判。
        if let Err(e) = self.persist_lease(&claim.workflow_id, &lease) {
            // 持久化失败：撤销本次内存写入（锁外补偿，比对键匹配），使重启
            // 恢复面与内存面一致——否则旧持久化租约在重启后复活，可能与
            // 已接管的新主形成双主窗口。撤销失败（期间又被并发写入覆盖）
            // 保留现值，交由第 5 腿仲裁收敛。
            tracing::error!(
                workflow = %claim.workflow_id.0,
                error = %e,
                "failed to persist claimed lease; rolling back in-memory write"
            );
            self.leases.remove_if(&claim.workflow_id, |_, current| {
                current.node_id == lease.node_id && current.claimed_at_ms == lease.claimed_at_ms
            });
        }

        if claim.node_id != self.node_id {
            if let Err(e) = self.remove_active_workflow(&claim.workflow_id).await {
                tracing::warn!(error = %e, workflow_id = %claim.workflow_id.as_str(), "failover: failed to remove active workflow");
            }
            // 编排层让位之后，执行层也必须让位：撤销本地排队任务。只摘 slot 的话，
            // 已入队但尚未出队的任务仍会被 Worker 取出执行——副作用在新旧两个主上
            // 各发生一次，而完成回灌时 slot 已删、结果被静默丢弃。
            //
            // 已经出队（`inflight`/执行中）的任务无法从队列撤销，由派发前的
            // `dispatch_permitted` 租约所有权校验拦截。
            // 先取出句柄再 await：`self.scheduler.lock()` 的临时守卫活到 match
            // 结束，若把 await 放进 match 分支，parking_lot 守卫会跨 await 持有
            // （每个分片都被它锁住，且 future 不再是 Send）。
            let scheduler = self.scheduler.lock().clone();
            let removed = match scheduler {
                Some(scheduler) => scheduler.remove_workflow_tasks(&claim.workflow_id).await,
                None => 0,
            };
            if removed > 0 {
                crate::metrics::inc_failover_relinquished_tasks(removed as u64);
            }
            tracing::info!(
                claimant = %claim.node_id.0,
                workflow = %claim.workflow_id.0,
                cancelled_queued_tasks = removed,
                "relinquished workflow to claimant: removed from local active set and dropped queued tasks"
            );
        }
    }

    /// 由 claim 构造租约条目（handle_claim / 恢复期共用）。
    fn claim_lease_entry(claim: &OrchestratorClaim, lease_duration_ms: u64) -> LeaseEntry {
        LeaseEntry::new_with_monotonic(
            claim.node_id.clone(),
            claim.timestamp_ms,
            claim.timestamp_ms + lease_duration_ms,
            lease_duration_ms,
        )
    }

    pub async fn reschedule_workflow_tasks(&self, workflow_id: &WorkflowId) -> Result<()> {
        let result = self
            .call_workflow(workflow_methods::RESCHEDULE_INCOMPLETE_TASKS, workflow_id)
            .await?;
        let tasks: Vec<crate::common::TaskDefinition> = messaging::decode(&result.payload)?;

        for task_def in &tasks {
            crate::metrics::inc_failover_reschedules();

            // 不再广播合成的 TaskFailed：它没有 attempt fencing，原 owner 仍可能
            // 把它当成真实失败执行重试策略。新 attempt 的 Running/Completed
            // 会以更高 HLC 覆盖旧状态。
            let sched_opt = self.scheduler.lock().clone();
            match sched_opt {
                Some(sched) => match sched.enqueue(task_def.clone()).await {
                    Ok(()) => {
                        self.pending_reschedules.remove(&task_def.id);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "scheduler rejected rescheduled task {}/{}; queued for retry: {}",
                            workflow_id.0,
                            task_def.id.0,
                            e
                        );
                        self.pending_reschedules
                            .insert(task_def.id.clone(), task_def.clone());
                    }
                },
                None => {
                    tracing::warn!(
                        "no scheduler set on FailoverManager; queued rescheduled task {}/{} for retry",
                        workflow_id.0,
                        task_def.id.0
                    );
                    self.pending_reschedules
                        .insert(task_def.id.clone(), task_def.clone());
                }
            }
        }

        Ok(())
    }

    /// 重试上一轮入队失败的重调度任务。
    async fn retry_pending_reschedules(&self) {
        if self.pending_reschedules.is_empty() {
            return;
        }
        let pending: Vec<(TaskId, crate::common::TaskDefinition)> = self
            .pending_reschedules
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        let sched_opt = self.scheduler.lock().clone();
        let Some(sched) = sched_opt else {
            return;
        };
        for (task_id, task) in pending {
            match sched.enqueue(task).await {
                Ok(()) => {
                    self.pending_reschedules.remove(&task_id);
                }
                Err(e) => {
                    tracing::warn!(
                        task = %task_id.0,
                        error = %e,
                        "rescheduled task still rejected; keeping for next retry"
                    );
                }
            }
        }
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

    /// 待接管工作流快照：`(workflow_id, 原 owner)`。
    #[cfg(test)]
    pub(crate) fn pending_claims_snapshot(&self) -> Vec<(String, String)> {
        self.pending_claims
            .iter()
            .map(|e| (e.key().0.clone(), e.value().0.clone()))
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
            drop(peer);
            self.bump_peers_version();
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
    /// 先把失联 peer 清出视图：孤儿 workflow 取自它最后一次心跳声明的活跃集，
    /// 而接管选举的候选集合只包含存活节点（活跃 peer + 本节点）——否则孤儿
    /// workflow 可能被哈希给已失联的节点，永远无人接管。
    ///
    /// 单个 workflow 接管或重调度失败只记录错误并继续处理其他 workflow，避免一个
    /// 损坏状态阻塞整批故障恢复。
    pub async fn detect_and_claim_failed_nodes(&self) {
        if !self.role.can_orchestrate() {
            return;
        }
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
        // 选举候选集合：仅存活且具备编排角色的节点（Worker 永不 claim，
        // 若留在候选集里，孤儿 workflow 可能被哈希给它而无人接管）。
        let candidate_ids: Vec<String> = {
            let mut ids: Vec<_> = self
                .peers
                .iter()
                .filter(|e| e.value().role.can_orchestrate())
                .map(|e| e.key().0.clone())
                .collect();
            ids.push(my_id.0.clone());
            ids
        };

        // 待检测集合：失联快照 + 当前视图（覆盖边界时序下仍超时的 peer）。
        let mut to_check: Vec<(NodeId, PeerInfo)> = stale;
        for (node_id, info) in self.get_peer_infos() {
            to_check.push((node_id, info));
        }

        // 本轮已判死并处置过的节点：探测腿跳过（fail-fast 重插的直提条目
        // 目标仍是这些死节点，再探测无信息增量）。
        let mut failed_nodes: HashSet<NodeId> = HashSet::new();
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
            failed_nodes.insert(node_id.clone());

            // 孤儿清单取自 peer 最后一次心跳声明的活跃集：它在失联判定的时刻
            // 之前一直由对端刷新，因此就是"对端失联时仍在驱动的工作流"。
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
                if !should_claim_workflow(&wf_id.0, &my_id.0, candidate_ids.clone()) {
                    continue;
                }
                match self.claim_workflow(wf_id).await {
                    Ok(ClaimOutcome::Adopted) => {
                        self.pending_claims.remove(wf_id);
                        if let Err(e) = self.reschedule_workflow_tasks(wf_id).await {
                            tracing::error!(
                                "failed to reschedule tasks for workflow {}: {}",
                                wf_id.0,
                                e
                            );
                        }
                    }
                    // 本节点原本就持有有效租约：已由本地驱动，不能再重调度，
                    // 否则会把 Running 任务重置并重复入队。退让同样不是错误，
                    // 但需要保留在待接管表中逐轮重试。
                    Ok(ClaimOutcome::AlreadyOwned) => {
                        self.pending_claims.remove(wf_id);
                    }
                    Ok(outcome) => {
                        tracing::info!(
                            workflow = %wf_id.0,
                            ?outcome,
                            "claim deferred; queued for retry"
                        );
                        self.pending_claims.insert(wf_id.clone(), node_id.clone());
                    }
                    Err(e) => {
                        tracing::error!("failed to claim workflow {}: {}", wf_id.0, e);
                    }
                }
            }
        }

        // 第 2 腿之二：心跳视图不可判的在途目标走主动探测。
        // 放在逐 peer 扫描之后——已被判失联的 peer 其条目已在上文被清空，
        // 此处只处理"连心跳都没来得及被观测到"的残余目标。
        self.probe_outbound_targets(&failed_nodes).await;

        // 第 3 腿：重试上一轮被退让的接管。入口是本表而不是 peer 视图，
        // 因为触发退让的 peer 当轮已被移出视图（见 `pending_claims` 的文档）。
        self.retry_pending_claims().await;
        // 第 5 腿：分区恢复收敛——存活 peer 与本节点的活跃工作流重叠时仲裁。
        self.arbitrate_lease_overlaps().await;
        // 第 4 腿：上一轮 scheduler 拒绝的重调度任务。
        self.retry_pending_reschedules().await;
    }

    /// 分区恢复收敛（第 5 腿）：存活 peer 的活跃工作流与本节点持有的租约重叠时，
    /// 双方可能都在驱动同一工作流。
    ///
    /// 经典场景：对称分区超过 `lease_duration` 后恢复——claim 广播在分区内丢失
    /// （gossip 不重放），`expire_leases` 又对本节点活跃租约无条件续租；恢复后
    /// 双方心跳互见，但 detect 循环只处理已判死的 peer，`handle_heartbeat` 对
    /// 存活 peer 的重叠不做处置——双方各自续租、各自派发直到一方重启。
    ///
    /// 处置：本节点广播当前租约的 claim（时间戳取 `claimed_at_ms`，**不刷新**——
    /// 续租式刷新会让两侧比较失去稳定基准），对端收到后按
    /// `(timestamp_ms, node_id)` 全序与本端租约比较，恰好一方胜出；对端的对称
    /// 扫描保证双向仲裁。输家经 `handle_claim` 移除 active 集合，停止续租与
    /// 派发，收敛到单主。周期性检测覆盖广播与续租交错的竞态窗口——未决的
    /// 下一轮心跳重试。
    async fn arbitrate_lease_overlaps(&self) {
        let now_ms = crate::common::epoch_millis();
        let now_monotonic = std::time::Instant::now();
        let timeout_ms = self.failure_timeout_ms();
        // 快照本节点持有的租约，避免在 DashMap 迭代期间 await。
        let mine: Vec<(WorkflowId, LeaseEntry)> = self
            .leases
            .iter()
            .filter(|e| e.value().node_id == self.node_id)
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (wf_id, lease) in mine {
            let overlapped = self.peers.iter().any(|p| {
                p.value().role.can_orchestrate()
                    && now_ms.saturating_sub(p.value().last_heartbeat_ms) <= timeout_ms
                    && p.value().active_workflows.contains(&wf_id)
            });
            if !overlapped {
                continue;
            }
            if lease.is_valid(now_ms, now_monotonic) {
                tracing::warn!(
                    workflow = %wf_id.0,
                    "lease overlap with alive peer detected after partition heal;                      re-broadcasting claim for arbitration"
                );
                let claim = OrchestratorClaim {
                    node_id: self.node_id.clone(),
                    workflow_id: wf_id.clone(),
                    timestamp_ms: lease.claimed_at_ms,
                };
                if let Err(e) = self.broadcast_claim(&claim).await {
                    tracing::warn!(
                        workflow = %wf_id.0,
                        error = %e,
                        "failed to broadcast overlap arbitration claim"
                    );
                }
            } else {
                // 租约已失效但工作流仍在驱动（expire_leases 续租窗口内）：走完整
                // claim 重置租约并广播，让对端参与仲裁。
                match self.claim_workflow(&wf_id).await {
                    Ok(ClaimOutcome::Adopted) => {
                        if let Err(e) = self.reschedule_workflow_tasks(&wf_id).await {
                            tracing::error!(
                                "failed to reschedule tasks for workflow {}: {}",
                                wf_id.0,
                                e
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(
                            "failed to re-claim lapsed lease for workflow {}: {}",
                            wf_id.0,
                            e
                        );
                    }
                }
            }
        }
    }

    /// 重试被退让的接管。
    ///
    /// 每轮重新参加一致性哈希选举：失效 peer 已移出候选集，职责可能已经转移到
    /// 本节点。三种结局：接管成功（从表里删除并重调度）、仍退让（保留待下轮）、
    /// 原 owner 复活（从表里删除——工作流不再是孤儿）。
    async fn retry_pending_claims(&self) {
        if self.pending_claims.is_empty() {
            return;
        }
        let now_ms = crate::common::epoch_millis();
        let timeout_ms = self.failure_timeout_ms();
        let candidate_ids: Vec<String> = {
            let mut ids: Vec<_> = self
                .peers
                .iter()
                .filter(|e| e.value().role.can_orchestrate())
                .map(|e| e.key().0.clone())
                .collect();
            ids.push(self.node_id.0.clone());
            ids
        };
        let pending: Vec<(WorkflowId, NodeId)> = self
            .pending_claims
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        for (wf_id, owner) in pending {
            // owner 既要在超时窗口内，也要仍在驱动该 workflow；仅“活着”不足以
            // 证明孤儿已被接管（重启后尚未恢复状态的 owner 也会发心跳）。
            let owner_alive = self.peers.get(&owner).is_some_and(|s| {
                now_ms.saturating_sub(s.last_heartbeat_ms) <= timeout_ms
                    && s.active_workflows.contains(&wf_id)
            });
            if owner_alive {
                self.pending_claims.remove(&wf_id);
                tracing::info!(
                    workflow = %wf_id.0,
                    owner = %owner.0,
                    "original orchestrator is alive again; dropping pending claim"
                );
                continue;
            }
            if !should_claim_workflow(&wf_id.0, &self.node_id.0, candidate_ids.clone()) {
                continue;
            }
            match self.claim_workflow(&wf_id).await {
                Ok(ClaimOutcome::Adopted) => {
                    self.pending_claims.remove(&wf_id);
                    tracing::info!(
                        workflow = %wf_id.0,
                        "deferred claim succeeded on retry"
                    );
                    if let Err(e) = self.reschedule_workflow_tasks(&wf_id).await {
                        tracing::error!(
                            "failed to reschedule tasks for workflow {}: {}",
                            wf_id.0,
                            e
                        );
                    }
                }
                Ok(ClaimOutcome::AlreadyOwned) => {
                    // 已持有有效租约：工作流已在驱动，不重复重调度。
                    self.pending_claims.remove(&wf_id);
                }
                // 仍退让：租约尚未到期，保留待下轮。
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("failed to claim workflow {} on retry: {}", wf_id.0, e);
                }
            }
        }
    }

    /// 入站心跳的身份与信任校验。
    ///
    /// `endpoint_addr` 是完整 `EndpointAddr` 编码（hex postcard，或 bootstrap
    /// 配置使用的 `id@ip:port` 简写），身份取其 endpoint id。依次应用
    /// （任一失败拒绝并 warn）：
    /// 1. 成员校验：`allowed_peer_ids` 非空时，心跳的 endpoint id 必须在
    ///    列表内（gossip 侧旁路的封闭；直连侧另有 ALPN 白名单）；
    /// 2. 签名校验：`require_signed_records` 开启时，心跳必须携带有效签名，
    ///    且签名公钥须等于 `endpoint_addr` 中的 endpoint id——节点无法伪造
    ///    他人身份的节点记录。
    fn verify_heartbeat(&self, hb: &NodeHeartbeat) -> bool {
        // 开放模式（无白名单、不要求签名）无需解析地址：心跳只用于容量视图。
        if self.allowed_peer_ids.is_empty() && !self.require_signed_records {
            return true;
        }
        // 身份只由完整地址携带：允许列表与验签都必须比较 endpoint id，
        // 直接拿 hex 地址字符串比较永远不会命中。
        let identity = hb
            .endpoint_addr
            .as_deref()
            .and_then(|addr| crate::runtime::network::parse_endpoint_addr(addr).ok());

        if !self.allowed_peer_ids.is_empty() {
            let Some(addr) = identity.as_ref() else {
                tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: no valid endpoint_addr while allowlist is active");
                return false;
            };
            let peer_id = addr.id.to_string();
            if !self.allowed_peer_ids.iter().any(|a| a == &peer_id) {
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
        let Some(sig_bytes) = &hb.signature else {
            tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: unsigned while require_signed_records is on");
            return false;
        };
        let Some(addr) = identity.as_ref() else {
            tracing::warn!(node = %hb.node_id.0, "heartbeat rejected: missing or invalid endpoint_addr for signature check");
            return false;
        };
        // 身份必须与 node_id 一致：否则持有合法密钥的成员可以冒用他人
        // node_id，让任务/结果/租约落到冒名者身上。展示名请用 node_labels。
        if hb.node_id.as_str() != addr.id.to_string() {
            tracing::warn!(
                node = %hb.node_id.0,
                endpoint_id = %addr.id,
                "heartbeat rejected: node_id does not match endpoint identity"
            );
            return false;
        }
        let pk = &addr.id;
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
        if hb.node_id != self.node_id && self.blocked_peers.contains_key(&hb.node_id) {
            tracing::debug!(
                node = %hb.node_id.0,
                "ignoring heartbeat from blocked peer (fault injection)"
            );
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
                available_quotas: BTreeMap::new(),
                role: crate::common::NodeRole::default(),
                draining: false,
            });
            peer.last_heartbeat_ms = received_at_ms;
            peer.active_workflows = hb.active_workflows.iter().cloned().collect();
            peer.available_slots = hb.available_slots;
            peer.max_slots = hb.max_slots;
            peer.endpoint_addr = hb.endpoint_addr.clone();
            peer.available_quotas = hb.available_quotas.clone();
            peer.role = hb.role;
            peer.draining = hb.draining;
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
            // 每条心跳都必须使候选快照失效：`last_heartbeat_ms` 参与
            // `is_eligible` 的 TTL 判活（`now - last_heartbeat <= failure_timeout`），
            // 保活心跳若不 bump 版本，快照里会残留过期时间戳——空闲期（无容量/
            // 成员变更）超过 ttl 后该 peer 被路由永久判死，新提交只能 defer 到
            // 饥饿失败。无条件的 bump 代价是每心跳一次 O(N) 快照重建，在目标
            // 集群规模（≤ 百节点）下可忽略。
            self.bump_peers_version();
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
            // 续租在 entry 分片写锁内复检「仍为本节点持有」：判陈旧与写入
            // 分离时，并发的远端 claim（handle_claim）可能刚覆盖租约，无条件
            // 续租会把租约改写回本节点——远端新主与本节点双双驱动（双主）。
            let renewed = match self.leases.entry(wf_id.clone()) {
                dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                    let current = occupied.get();
                    if current.node_id != self.node_id {
                        tracing::info!(
                            workflow = %wf_id.0,
                            owner = %current.node_id.0,
                            "skipping renewal: lease was taken over by remote claim"
                        );
                        None
                    } else {
                        occupied.insert(renewed.clone());
                        Some(renewed)
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(_) => None,
            };
            let Some(renewed) = renewed else {
                continue;
            };
            // 持久化失败保留旧租约记录，下一轮 expire_leases 重试续租。
            if let Err(e) = self.persist_lease(wf_id, &renewed) {
                tracing::warn!(
                    workflow = %wf_id.0,
                    error = %e,
                    "failed to persist renewed lease for active workflow"
                );
                continue;
            }
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
                        // 恢复期消毒：claimed_at 超前本地时钟超过 failure_timeout
                        // 的租约（历史未来时间戳 claim 的残留或数据损坏）不恢复
                        // ——墙钟恢复无单调基线，未来租约重启后"永久有效"，任何
                        // 正常 claim 都无法胜出。删除而非保留，让接管重新仲裁。
                        if persisted.claimed_at_ms.saturating_sub(now_ms) > self.failure_timeout_ms
                        {
                            tracing::warn!(
                                workflow = %wf_id_str,
                                claimed_at_ms = persisted.claimed_at_ms,
                                now_ms,
                                "discarding future-dated lease from store beyond drift tolerance"
                            );
                            if let Err(e) = store.delete(&key) {
                                tracing::warn!(
                                    "failed to delete future-dated lease {}: {}",
                                    wf_id_str,
                                    e
                                );
                            }
                        } else if now_ms < persisted.expires_at_ms {
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
