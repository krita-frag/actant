//! Workflow 编排：DAG 提交、执行与完成追踪。
//!
//! `Orchestrator` 是工作流生命周期的中央协调器：
//! - **提交**：接收 [`Dag`] 结构，通过 `Store` 持久化。
//! - **执行**：计算根任务、追踪待完成依赖，在前驱完成时产出可执行的
//!   [`TaskDefinition`](crate::common::TaskDefinition)。
//! - **完成**：工作流进入终态时通知等待者。
//! - **恢复**：`Orchestrator::recover`
//!   在重启后从存储恢复状态，并将 in-flight 任务重置为 Pending。
//! - **淘汰**：超过可配置保留数量的已完成工作流会被自动从内存与存储中移除。
//!
//! ## 子模块
//!
//! | 模块 | 职责 |
//! |------|------|
//! | [`dag`] | DAG 结构、边校验、拓扑查询；`WorkflowExecution` / `TaskState` |
//! | `orchestrator` | `Orchestrator` 状态机与事件溯源 |
//! | [`scheduler`] | 任务调度策略（FIFO / 优先级） |
//! | [`failover`] | 基于租约的 orchestrator 故障转移 |
//! | [`gossip`] | 基于 gossipsub 的 DAG 状态复制 |
//! | [`actor`] | 上述组件的 Actor 化封装 |
//! | [`messaging`] | Actor 消息编解码 |
//! | [`runtime`] | `Worker` 执行循环 |

pub mod actor;
pub mod dag;
pub mod failover;
pub mod gossip;
pub mod messaging;
pub(crate) mod orchestrator;
pub mod runtime;
pub mod scheduler;

pub use actor::{
    dag_gossip_actor, failover_actor, failover_methods, fifo_scheduler_actor, gossip_methods,
    priority_scheduler_actor, scheduler_methods, workflow_methods, DagGossipActor, FailoverActor,
    SchedulerActor, TaskCompletionResponse, WorkflowActor, DAG_GOSSIP_ACTOR_TYPE,
    FAILOVER_ACTOR_TYPE, SCHEDULER_ACTOR_TYPE, WORKFLOW_ACTOR_TYPE,
};
pub(crate) use dag::WorkflowExecution;
pub use dag::{
    Dag, DagNode, FailureScope, FailureStrategy, Phase, TaskState, Terminal, WaitCondition,
    WaitPoint, WaitPointState,
};
pub use failover::{FailoverManager, PeerInfo};
pub use gossip::DagGossip;
pub(crate) use orchestrator::Orchestrator;
pub use runtime::{Worker, WorkerState};
#[doc(hidden)]
pub use scheduler::spawn_fast_path_scheduler;
pub use scheduler::{is_registered, registered_names, ActorScheduler, Scheduler};
