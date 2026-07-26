//! Orchestrator 的 `state` 职责子模块。
//!
//! 负责持有 `Orchestrator` 结构体定义、构造器、克隆/drop 实现以及
//! 字段访问器。所有业务方法分布在 `persistence` / `execution` / `queries`
//! 三个子模块中，通过 `pub(crate)` 字段共享内部状态。

use std::sync::Arc;

use crate::common::{ActantConfig, NodeId};
use crate::runtime::network::Transport;
use crate::runtime::state::event_log::EventLog;
use crate::runtime::state::{HybridLogicalClock, Store};

use super::types::{ConditionEvaluator, OrchestratorState};

pub struct Orchestrator {
    pub(crate) state: Arc<OrchestratorState>,
    pub(crate) config: ActantConfig,
    pub(crate) store: Option<Store>,
    pub(crate) event_log: Option<Arc<dyn EventLog>>,
    /// 条件分支求值器。`None` 时 `on_task_completed` 将条件边返回给调用方
    ///（如 Python 编排循环）外部评估。
    pub(crate) condition_evaluator: Option<Arc<dyn ConditionEvaluator>>,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) hlc: Arc<HybridLogicalClock>,
    /// 网络传输层。`None` 时（如单元测试或无网络场景）超时监控仅标记工作流
    /// 失败，不广播取消消息；`Some` 时（生产运行时）超时监控通过 gossip
    /// topic ``actant:cancel`` 广播 `CancelBroadcast`，触发所有相关节点
    ///（包括本地与远端）取消运行中的任务。
    pub(crate) network: Option<Arc<dyn Transport>>,
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        tracing::debug!(
            "Orchestrator::drop — store is_some = {}",
            self.store.is_some()
        );
    }
}

impl Clone for Orchestrator {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            config: self.config.clone(),
            store: self.store.clone(),
            event_log: self.event_log.clone(),
            condition_evaluator: self.condition_evaluator.clone(),
            node_id: self.node_id.clone(),
            hlc: self.hlc.clone(),
            network: self.network.clone(),
        }
    }
}

impl Orchestrator {
    pub fn new() -> Self {
        Self {
            state: Arc::new(OrchestratorState::new()),
            config: ActantConfig::default(),
            store: None,
            event_log: None,
            condition_evaluator: None,
            node_id: None,
            hlc: Arc::new(HybridLogicalClock::new()),
            network: None,
        }
    }

    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_signing_key(mut self, key: Vec<u8>) -> Self {
        self.config.payload_signing_key = key;
        self
    }

    pub fn with_config(mut self, config: ActantConfig) -> Self {
        self.hlc = Arc::new(HybridLogicalClock::with_max_drift_ms(
            config.network.hlc_max_drift_ms,
        ));
        self.config = config;
        self
    }

    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_event_log(mut self, event_log: Arc<dyn EventLog>) -> Self {
        self.event_log = Some(event_log);
        self
    }

    pub fn with_condition_evaluator(mut self, evaluator: Arc<dyn ConditionEvaluator>) -> Self {
        self.condition_evaluator = Some(evaluator);
        self
    }

    /// 注入网络传输层，启用工作流级硬超时的主动取消能力（B2）。
    ///
    /// 未注入时：超时监控仅将工作流标记为失败，运行中的任务继续执行至完成或自身超时。
    /// 注入后：超时监控在标记失败前，对所有运行中任务广播 `CancelBroadcast`，
    /// 触发本地与远端 Worker 协作式取消。
    pub fn with_network(mut self, network: Arc<dyn Transport>) -> Self {
        self.network = Some(network);
        self
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    pub fn store(&self) -> &Option<Store> {
        &self.store
    }

    pub fn state_handle(&self) -> Arc<OrchestratorState> {
        self.state.clone()
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}
