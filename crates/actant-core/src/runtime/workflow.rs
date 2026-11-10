//! Workflow 编排：DAG 提交、执行与完成追踪。
//!
//! `Orchestrator` 协调工作流生命周期：提交 → 执行 → 完成 → 恢复 → 淘汰。
//!
//! 子模块职责见 `docs/architecture/overview.md`；恢复与淘汰语义见
//! `docs/architecture/recovery.md`。

pub mod actor;
pub mod dag;
pub mod failover;
pub mod gossip;
pub mod messaging;
pub mod orchestrator;
pub mod route;
pub mod runtime;
pub mod scheduler;

pub use actor::{
    dag_gossip_actor, failover_actor, failover_methods, fifo_scheduler_actor, gossip_methods,
    priority_scheduler_actor, scheduler_methods, workflow_methods, DagGossipActor, FailoverActor,
    SchedulerActor, TaskCompletionResponse, WorkflowActor, DAG_GOSSIP_ACTOR_TYPE,
    FAILOVER_ACTOR_TYPE, SCHEDULER_ACTOR_TYPE, WORKFLOW_ACTOR_TYPE,
};
pub use dag::WorkflowExecution;
pub use dag::{
    Dag, DagEdge, DagNode, FailureScope, FailureStrategy, Phase, TaskState, Terminal,
    WaitCondition, WaitPoint, WaitPointState,
};
pub use failover::{FailoverManager, PeerInfo};
pub use gossip::DagGossip;
// 该 re-export 的唯一消费者是绑定层（`src/py/runtime.rs`）——core 内部
// （`orchestrator/execution.rs`）经 `super::types::*` 直接取用，不经过此处。
// 因此按 `python` 特性门控：纯框架构建（`--no-default-features`）下不应产生
// unused_imports 告警（框架构建必须零告警）。
pub use orchestrator::types::AddNodeOutcome;
pub(crate) use orchestrator::Orchestrator;
pub use route::{
    labels_match, quota_available, unschedulable_quota_reason, unschedulable_reason,
    DefaultRoutePolicy, LabelAffinityRoutePolicy, RouteCandidate, RouteContext, RouteDecision,
    RoutePolicy,
};
pub use runtime::{Worker, WorkerState};
#[doc(hidden)]
pub use scheduler::spawn_fast_path_scheduler;
pub use scheduler::{ActorScheduler, Scheduler};
