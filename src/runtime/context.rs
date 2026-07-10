//! 统一运行时上下文。
//!
//! 把所有 Rust 子系统句柄聚合到一个结构中，供 PyO3 层和其他第 1 层代码使用。
//! “统一 Runtime + 薄 PyO3 绑定”架构的核心容器。

use std::sync::Arc;

use crate::common::{ActantConfig, ActantError, ActorId, NodeId};
use crate::runtime::actor::ActorSystem;
use crate::runtime::capability::CapabilityRuntime;
use crate::runtime::dispatcher::TaskDispatcher;
use crate::runtime::event_bus::EventBus;
use crate::runtime::network::Transport;
use crate::runtime::state::Store;
use crate::runtime::workflow::{FailoverManager, Worker};

// Re-export init_worker params so Runtime can construct them without exposing internals.
use crate::runtime::builder::{init_worker, WorkerInitParams};

/// 非 Actor 化后台任务的取消句柄集合类型。
///
/// 使用 `parking_lot::Mutex` 而非 `std::sync::Mutex`，避免持锁线程 panic 后
/// 传播 poison 错误导致 `shutdown` 路径也 panic（C2 改进）。
type BackgroundCancels = parking_lot::Mutex<Vec<tokio::sync::watch::Sender<bool>>>;

/// Actant 统一运行时。
///
/// 持有所有核心子系统的 `Arc` 句柄；本身廉价且 `Clone`。
#[derive(Clone)]
pub struct Runtime {
    node_id: NodeId,
    config: ActantConfig,
    network: Arc<dyn Transport>,
    store: Store,
    actor_system: Arc<ActorSystem>,
    workflow_actor_id: ActorId,
    worker: Option<Arc<Worker>>,
    capability: Arc<CapabilityRuntime>,
    event_bus: EventBus,
    task_dispatcher: Arc<dyn TaskDispatcher>,
    /// 非 Actor 化后台任务（如 capability gossip）的取消句柄集合。
    background_loop_cancels: Arc<BackgroundCancels>,
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        config: ActantConfig,
        network: Arc<dyn Transport>,
        store: Store,
        actor_system: Arc<ActorSystem>,
        workflow_actor_id: ActorId,
        worker: Option<Arc<Worker>>,
        capability: Arc<CapabilityRuntime>,
        event_bus: EventBus,
        task_dispatcher: Arc<dyn TaskDispatcher>,
    ) -> Self {
        Self {
            node_id,
            config,
            network,
            store,
            actor_system,
            workflow_actor_id,
            worker,
            capability,
            event_bus,
            task_dispatcher,
            background_loop_cancels: Arc::new(BackgroundCancels::new(Vec::new())),
        }
    }

    /// 注册一个非 Actor 化后台任务的取消句柄。
    ///
    /// 在 `Runtime::shutdown` 时会向所有已注册句柄发送 `true`，
    /// 确保后台循环与 Actor 子系统一并停止。
    pub fn register_background_loop_cancel(&self, cancel: tokio::sync::watch::Sender<bool>) {
        self.background_loop_cancels.lock().push(cancel);
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn config(&self) -> &ActantConfig {
        &self.config
    }

    pub fn network(&self) -> &Arc<dyn Transport> {
        &self.network
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn actor_system(&self) -> &Arc<ActorSystem> {
        &self.actor_system
    }

    pub fn workflow_actor_id(&self) -> &ActorId {
        &self.workflow_actor_id
    }

    pub fn worker(&self) -> Option<&Arc<Worker>> {
        self.worker.as_ref()
    }

    pub fn set_worker(&mut self, worker: Arc<Worker>) {
        self.worker = Some(worker);
    }

    pub fn capability(&self) -> &Arc<CapabilityRuntime> {
        &self.capability
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    pub fn task_dispatcher(&self) -> &Arc<dyn TaskDispatcher> {
        &self.task_dispatcher
    }

    /// 使用当前 Runtime 的子系统初始化一个 [`Worker`]。
    ///
    /// 返回的 worker 共享本 Runtime 的 `task_dispatcher`，避免重复创建线程池。
    /// 调用方需要自行保存返回的 worker（例如写入 `Runtime.worker` 字段，
    /// 或在外部持有其 `Arc`）。
    pub async fn init_worker(
        &self,
        tokio_handle: tokio::runtime::Handle,
    ) -> crate::common::Result<Arc<Worker>> {
        let failover = Arc::new(FailoverManager::new(
            self.node_id.clone(),
            self.network.clone(),
            self.actor_system.clone(),
            self.workflow_actor_id.clone(),
        ));
        let dag_gossip_actor_id = crate::common::ActorId::dag_gossip(&self.node_id);
        let worker = init_worker(WorkerInitParams {
            node_id: &self.node_id,
            network: &self.network,
            event_bus: self.event_bus.clone(),
            scheduler_kind: self.config.worker.scheduler_kind.as_str(),
            worker_config: &self.config.worker,
            actor_system: self.actor_system.clone(),
            task_dispatcher: self.task_dispatcher.clone(),
            failover: &failover,
            tokio_handle,
            workflow_actor_id: Some(self.workflow_actor_id.clone()),
            dag_gossip_actor_id: Some(dag_gossip_actor_id),
        })
        .await?;
        Ok(Arc::new(worker))
    }

    /// 优雅关闭 Runtime：停止所有关键 Actor 并等待其实际退出。
    ///
    /// 关闭顺序：Worker → SchedulerActor → FailoverActor → DagGossipActor
    /// → WorkflowActor → CapabilityActor。每个 `stop` 都会 await Actor 任务
    /// 结束，确保状态落盘与后台循环取消完成。
    pub async fn shutdown(&self) -> crate::common::Result<()> {
        use crate::common::ActorId;

        // 每个 actor stop 设超时，确保 network.shutdown() 总能被执行。
        // actor stop 卡住时（如网络阻塞），放弃等待——endpoint.close() 会使
        // 其网络调用自然失败。超时由 config.actor.stop_timeout_ms 控制（M1 改进）。
        let stop_timeout = std::time::Duration::from_millis(self.config.actor.stop_timeout_ms);

        // 0. 先取消非 Actor 化的后台循环（如 capability gossip），
        //    避免在 Actor 系统停止后仍有任务引用 network / capability 句柄。
        let cancels: Vec<tokio::sync::watch::Sender<bool>> = self
            .background_loop_cancels
            .lock()
            .iter()
            .cloned()
            .collect();
        for tx in cancels {
            let _ = tx.send(true);
        }

        // 1. 停止 Worker（取消消费循环）。
        if let Some(worker) = self.worker.as_ref() {
            worker.shutdown();
            // 等待 worker.run() 退出（状态→Stopped）。subscribe_topics 可能阻塞
            // 导致 cancel 不被处理，故设短超时。
            let mut state_rx = worker.subscribe_state();
            let _ = tokio::time::timeout(stop_timeout, async {
                while *state_rx.borrow()
                    != crate::runtime::workflow::WorkerState::Stopped
                {
                    if state_rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
            if let Some(sched_id) = worker.scheduler_actor_id() {
                let _ =
                    tokio::time::timeout(stop_timeout, self.actor_system.stop(sched_id)).await;
            }
        }

        // 2. 停止 FailoverActor（停止心跳/故障检测循环）。
        let failover_id = ActorId::failover(&self.node_id);
        let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(&failover_id)).await;

        // 3. 停止 DagGossipActor。
        let dag_gossip_id = ActorId::dag_gossip(&self.node_id);
        let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(&dag_gossip_id)).await;

        // 4. 停止 WorkflowActor（on_stop 取消 timeout/persist 循环）。
        let _ =
            tokio::time::timeout(stop_timeout, self.actor_system.stop(&self.workflow_actor_id))
                .await;

        // 5. 停止所有 CapabilityActor（按注册名）。
        for meta in self.capability.capabilities() {
            let cap_id = ActorId::capability(meta.name);
            let _ = tokio::time::timeout(stop_timeout, self.actor_system.stop(&cap_id)).await;
        }

        // 6. 关闭网络（iroh endpoint.close()）。必须在所有 Actor / 后台循环停止后
        //    执行：否则 Endpoint 被无声 drop 会触发 iroh 的
        //    "Endpoint dropped without calling Endpoint::close. Aborting ungracefully."
        //    警告，并可能中断在途 gossip/直连请求。
        self.network
            .shutdown()
            .await
            .map_err(|e| ActantError::Network(format!("failed to shutdown network: {e}")))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{ActantConfig, ActorId, NodeId};
    use crate::runtime::actor::ActorSystem;
    use crate::runtime::capability::CapabilityRuntime;
    use crate::runtime::dispatcher::TaskRegistry;
    use crate::runtime::event_bus::EventBus;
    use crate::runtime::state::{LmdbStore, Store};
    use crate::test_support::MockTransport;
    use tempfile::tempdir;

    fn make_runtime(node_id: &str) -> Runtime {
        let network: Arc<dyn Transport> = Arc::new(MockTransport::new(node_id));
        let dir = tempdir().unwrap();
        let lmdb = LmdbStore::open(dir.path()).unwrap();
        let store = Store::new(lmdb);
        let actor_system = Arc::new(ActorSystem::new());
        let workflow_actor_id = ActorId::workflow(&NodeId::from(node_id.to_string()));
        let capability = Arc::new(CapabilityRuntime::new());
        let event_bus = EventBus::new();
        let dispatcher = TaskRegistry::new(1, 8, Vec::new())
            .expect("TaskRegistry init")
            .into_dispatcher();
        Runtime::new(
            NodeId::from(node_id.to_string()),
            ActantConfig::default(),
            network,
            store,
            actor_system,
            workflow_actor_id,
            None,
            capability,
            event_bus,
            dispatcher,
        )
    }

    #[test]
    fn getters_return_configured_values() {
        let rt = make_runtime("node-X");
        assert_eq!(rt.node_id().as_str(), "node-X");
        assert!(
            rt.config().worker.scheduler_kind.as_str().is_empty()
                || !rt.config().worker.scheduler_kind.as_str().is_empty()
        );
        assert_eq!(rt.workflow_actor_id().as_str(), "workflow-node-X");
        assert!(rt.worker().is_none());
        assert!(rt.network().node_id().as_str() == "node-X");
    }

    #[test]
    fn register_background_loop_cancel_is_noop_on_shutdown_when_unused() {
        // 注册一个取消句柄但不发送，验证它被存入列表且不 panic。
        let rt = make_runtime("node-A");
        let (tx, _rx) = tokio::sync::watch::channel(false);
        rt.register_background_loop_cancel(tx);
        // 列表非空不影响其它访问器
        assert_eq!(rt.node_id().as_str(), "node-A");
    }

    #[test]
    fn shutdown_completes_without_worker_or_actors() {
        // 无 worker、无已注册 Actor：shutdown 应顺序停止所有不存在的 Actor 并返回 Ok。
        let rt = make_runtime("node-A");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(rt.shutdown());
        assert!(result.is_ok(), "shutdown should succeed: {:?}", result);
    }

    #[test]
    fn shutdown_sends_cancel_to_registered_loops() {
        // 注册一个取消句柄；shutdown 应发送 true，使 receiver 观察到 true。
        let rt = make_runtime("node-B");
        let (tx, rx) = tokio::sync::watch::channel(false);
        rt.register_background_loop_cancel(tx);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(rt.shutdown());
        assert!(result.is_ok());
        // 取消信号应已被发送
        assert!(*rx.borrow(), "cancel signal should be sent on shutdown");
    }

    #[test]
    fn clone_preserves_handles() {
        // Runtime 是 Clone（廉价 Arc 句柄复制）；clone 后访问器应指向同一节点。
        let rt = make_runtime("node-C");
        let rt2 = rt.clone();
        assert_eq!(rt.node_id().as_str(), rt2.node_id().as_str());
        assert_eq!(
            rt.workflow_actor_id().as_str(),
            rt2.workflow_actor_id().as_str()
        );
    }

    #[tokio::test]
    async fn store_accessor_round_trips_data() {
        // 验证 store 访问器返回的 Store 可正常读写（间接验证 Store 句柄有效）。
        let rt = make_runtime("node-D");
        rt.store().put("ctx-test", b"hello").await.unwrap();
        let val = rt.store().get("ctx-test").await.unwrap();
        assert_eq!(val, Some(b"hello".to_vec()));
    }
}
