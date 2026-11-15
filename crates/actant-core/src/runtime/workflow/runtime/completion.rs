//! Worker 完成结算与结果投递辅助。
//!
//! 本文件承载任务完成路径的纯辅助逻辑：
//! - `is_worker_crash`：dispatcher 结果的崩溃分类；
//! - `build_completion_from_dispatch_result`：派发结果 → `TaskCompletion` 映射
//!   （超时/取消/业务失败的终态归类）；
//! - `settle_local_completion`：本地任务终态结算入口（编排回灌 + 事件发布）；
//! - `OrchestratorBridge`：本地结果 → orchestrator 的回灌桥（仅 Rust 嵌入启用）；
//! - `publish_drained_task_cancellation`：drain 丢弃任务的 Cancelled 通知。
//!
//! 全部为模块级自由函数/类型；Worker 主循环（`runtime.rs`）按名调用。

use std::sync::Arc;
use std::time::Duration;

use super::result_delivery::{enqueue_pending_result, PendingResult};
use crate::common::{
    format_error_kind, ActantError, NodeId, TaskCompletion, TaskDefinition, WorkflowId,
};

/// 派发结果三态：Ok(Ok(bytes)) 成功 / Ok(Err(e)) 业务失败或超时 / Err(panic)。
/// 与 `runtime.rs` 主循环的同名别名同一定义（同步维护）。
pub(crate) type DispatchResult =
    std::result::Result<std::result::Result<Vec<u8>, ActantError>, PanicPayload>;
type PanicPayload = Box<dyn std::any::Any + Send>;
use crate::runtime::event_bus::{BusEvent, EventBus};
use crate::runtime::network::Transport;
use crate::runtime::workflow::actor::{ResultSource, TaskCompletionResponse, TaskResultOutcome};
use crate::runtime::workflow::messaging::{decode, encode};

pub(super) fn is_worker_crash(result: &DispatchResult) -> bool {
    matches!(result, Ok(Err(ActantError::Worker(_))))
}

/// 将任务派发结果转换为 ``TaskCompletion``。
///
/// 抽出为纯函数以便覆盖四种结果分支（成功、失败、panic、超时），
/// 无需构造完整 Worker 与后台执行循环。
pub(super) fn build_completion_from_dispatch_result(
    dispatch_result: DispatchResult,
    task: &TaskDefinition,
    dispatch_start_ms: u64,
    effective_timeout: Duration,
) -> TaskCompletion {
    let workflow_id = task
        .workflow_id
        .clone()
        .unwrap_or_else(|| WorkflowId::from("".to_string()));
    match dispatch_result {
        Ok(Ok(result)) => {
            crate::metrics::inc_tasks_completed();
            crate::metrics::dec_running_tasks();
            crate::metrics::observe_task_duration_ms(
                crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
            );
            TaskCompletion::Completed {
                workflow_id,
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                result,
                target_node: task.target_node.clone(),
            }
        }
        Ok(Err(e)) => {
            // 硬超时由 dispatcher 内部强杀 worker 并回收槽位后返回
            // `ActantError::Timeout`，计入超时指标；取消返回
            // `ActantError::Cancelled`（dispatcher 在宽限期耗尽后强杀并回传），
            // 必须映射为取消终态——**不得**按失败处理：失败会进入重试裁决，
            // 把已取消的任务重新入队执行（取消被"复活"），且在 fail-fast 策略下
            // 把工作流错误地判为 Failed。其余错误按任务失败处理。
            match e {
                ActantError::Timeout(_) => {
                    crate::metrics::inc_tasks_timeout();
                    crate::metrics::dec_running_tasks();
                    crate::metrics::observe_task_duration_ms(
                        crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                    );
                    TaskCompletion::Failed {
                        workflow_id,
                        task_id: task.id.clone(),
                        task_name: task.name.clone(),
                        error: format_error_kind(
                            "timeout",
                            &format!("task timed out after {}ms", effective_timeout.as_millis()),
                        ),
                        target_node: task.target_node.clone(),
                    }
                }
                ActantError::Cancelled(_) => {
                    crate::metrics::dec_running_tasks();
                    crate::metrics::observe_task_duration_ms(
                        crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                    );
                    TaskCompletion::Cancelled {
                        workflow_id,
                        task_id: task.id.clone(),
                        task_name: task.name.clone(),
                        target_node: task.target_node.clone(),
                    }
                }
                other => {
                    crate::metrics::inc_tasks_failed();
                    crate::metrics::dec_running_tasks();
                    crate::metrics::observe_task_duration_ms(
                        crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
                    );
                    TaskCompletion::Failed {
                        workflow_id,
                        task_id: task.id.clone(),
                        task_name: task.name.clone(),
                        error: format_error_kind("task", &other.to_string()),
                        target_node: task.target_node.clone(),
                    }
                }
            }
        }
        Err(panic_payload) => {
            // dispatcher panic：提取 panic 消息仅用于本地日志（含可能的敏感
            // 路径 / 变量值），**不**透传到 wire —— TaskCompletion::Failed.error
            // 会被序列化为 WireTaskOutcome::Failed(String) 发往 orchestrator 节点，
            // panic 原文不应跨节点泄露。降级为 Failed 让 workflow 能继续推进而非
            // 永久挂起。
            crate::metrics::inc_tasks_failed();
            crate::metrics::dec_running_tasks();
            crate::metrics::observe_task_duration_ms(
                crate::common::epoch_millis().saturating_sub(dispatch_start_ms),
            );
            let panic_msg = panic_payload
                .downcast_ref::<&'static str>()
                .copied()
                .map(String::from)
                .or_else(|| {
                    panic_payload
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .map(String::from)
                })
                .unwrap_or_else(|| "<non-string panic>".to_string());
            tracing::error!(
                task_id = ?task.id,
                panic = %panic_msg,
                "dispatcher panicked; emitting TaskCompletion::Failed"
            );
            TaskCompletion::Failed {
                workflow_id,
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                error: format_error_kind("internal", "dispatcher panicked"),
                target_node: task.target_node.clone(),
            }
        }
    }
}

/// 为 drain 时被丢弃的排队/inflight 任务发布 Cancelled 完成事件。
///
/// drain 后这些任务不会被执行，复用 [`publish_task_completion`] 走既有投递
/// 路径：远端任务（origin != 本节点）直连回传 Cancelled 结果给 origin 节点，
/// 本地任务发布 `BusEvent::TaskCancelled` 给事件总线订阅者。
/// drain 通知所需的共享依赖集合（`publish_drained_task_cancellation` 参数收敛）。
pub(super) struct DrainNotifyCtx<'a> {
    pub(super) node_id: &'a NodeId,
    pub(super) network: &'a dyn crate::runtime::network::Transport,
    pub(super) event_bus: &'a EventBus,
    pub(super) pending_results: &'a tokio::sync::mpsc::Sender<PendingResult>,
    pub(super) pending_capacity: usize,
}

pub(super) async fn publish_drained_task_cancellation(
    task: TaskDefinition,
    ctx: &DrainNotifyCtx<'_>,
) {
    tracing::info!(
        task_id = %task.id.as_str(),
        "dropping queued task during drain, publishing cancellation"
    );
    let completion = TaskCompletion::Cancelled {
        workflow_id: task
            .workflow_id
            .clone()
            .unwrap_or_else(|| WorkflowId::from(String::new())),
        task_id: task.id.clone(),
        task_name: task.name.clone(),
        target_node: task.target_node.clone(),
    };
    // drain 丢弃不回灌 orchestrator：任务未执行，Cancelled 经事件路径让
    // 提交方终止（与既有语义一致）；workflow 视图由失联接管路径兜底。
    settle_local_completion(
        completion,
        &task,
        ctx.node_id,
        ctx.network,
        ctx.event_bus,
        ctx.pending_results,
        ctx.pending_capacity,
        None,
    )
    .await;
}

/// 延迟重入队的共享依赖与终态回退上下文。
///
/// detached 重入队任务不在 drain 的排队/inflight 视野内，需要自带
/// worker 状态与结果投递依赖，才能在 drain 或重试耗尽时补发终态。
pub(super) struct ReenqueueCtx {
    pub(super) node_id: NodeId,
    pub(super) network: Arc<dyn crate::runtime::network::Transport>,
    pub(super) event_bus: EventBus,
    pub(super) pending_results: tokio::sync::mpsc::Sender<PendingResult>,
    pub(super) pending_capacity: usize,
    pub(super) state: tokio::sync::watch::Receiver<super::WorkerState>,
}

/// 重入队尝试次数与退避基数。
const REENQUEUE_MAX_ATTEMPTS: usize = 3;
const REENQUEUE_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// 延迟后把任务重新入队。
pub(super) async fn reenqueue_after_delay(
    scheduler: Arc<dyn crate::runtime::workflow::Scheduler>,
    task: TaskDefinition,
    delay: Duration,
    ctx: ReenqueueCtx,
    reason: &'static str,
) {
    tokio::time::sleep(delay).await;
    reenqueue_with_retry(scheduler, task, &ctx, reason).await;
}

/// 重新入队并按需补发终态。
///
/// 延迟/配额等待后的重入队是 detached 的：若调度器已关闭或持续拒绝，
/// 任务既不会执行也不会被取消，origin 可能永久等待。这里在 drain 时发
/// Cancelled、重试耗尽时发 Failed，保证任务不会静默丢失。
pub(super) async fn reenqueue_with_retry(
    scheduler: Arc<dyn crate::runtime::workflow::Scheduler>,
    task: TaskDefinition,
    ctx: &ReenqueueCtx,
    reason: &'static str,
) {
    let mut backoff = REENQUEUE_RETRY_BASE_DELAY;
    for attempt in 1..=REENQUEUE_MAX_ATTEMPTS {
        let draining = *ctx.state.borrow() == super::WorkerState::Draining;
        if draining {
            tracing::info!(
                task_id = %task.id.as_str(),
                reason,
                "worker draining; cancelling pending re-enqueue"
            );
            let completion = TaskCompletion::Cancelled {
                workflow_id: task
                    .workflow_id
                    .clone()
                    .unwrap_or_else(|| WorkflowId::from(String::new())),
                task_id: task.id.clone(),
                task_name: task.name.clone(),
                target_node: task.target_node.clone(),
            };
            publish_reenqueue_terminal(&task, ctx, completion).await;
            return;
        }
        match scheduler.enqueue(task.clone()).await {
            Ok(()) => return,
            Err(e) if attempt < REENQUEUE_MAX_ATTEMPTS => {
                tracing::warn!(
                    task_id = %task.id.as_str(),
                    attempt,
                    error = %e,
                    reason,
                    "re-enqueue failed; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            Err(e) => {
                tracing::error!(
                    task_id = %task.id.as_str(),
                    attempt,
                    error = %e,
                    reason,
                    "re-enqueue failed after retries; publishing failure"
                );
                let completion = TaskCompletion::Failed {
                    workflow_id: task
                        .workflow_id
                        .clone()
                        .unwrap_or_else(|| WorkflowId::from(String::new())),
                    task_id: task.id.clone(),
                    task_name: task.name.clone(),
                    error: format_error_kind(
                        "scheduler",
                        &format!(
                            "{reason} re-enqueue failed after {REENQUEUE_MAX_ATTEMPTS} attempts: {e}"
                        ),
                    ),
                    target_node: task.target_node.clone(),
                };
                publish_reenqueue_terminal(&task, ctx, completion).await;
                return;
            }
        }
    }
}

async fn publish_reenqueue_terminal(
    task: &TaskDefinition,
    ctx: &ReenqueueCtx,
    completion: TaskCompletion,
) {
    settle_local_completion(
        completion,
        task,
        &ctx.node_id,
        ctx.network.as_ref(),
        &ctx.event_bus,
        &ctx.pending_results,
        ctx.pending_capacity,
        None,
    )
    .await;
}

/// 本地任务终态结算。
///
/// 本地任务的结算**只做事件发布**：[`publish_task_completion`] 把终态投递给
/// 事件总线（无 `workflow_id` 的独立 `@task` 直调由此解析提交方句柄）；
/// 携带 `workflow_id` 的 flow 编排节点，事件同时由 Python 事件泵
/// （`Runtime._on_task_result`）消费。
///
/// ## 编排状态推进不在本函数内
///
/// worker 结果帧正文是 `dumps((success, payload))`——**payload 对 Rust 不透明**，
/// 且 `ProcessTaskDispatcher` 对「任务成功」与「任务业务失败」都返回
/// `Ok(Ok(body))`（失败被 worker 编码进 body 而非协议层）。因此 Rust 无法区分
/// 二者：若在此按 `Completed` 回灌 orchestrator，业务失败会被记成成功。
///
/// 故编排推进与重试裁决由**唯一能解析 payload 的一方**发起：Python 事件泵
/// 解析结果后经 [`Worker::report_task_result`] 桥上报，再由
/// `WorkflowActor::on_task_result` 统一裁决。
#[allow(clippy::too_many_arguments)]
pub(super) async fn settle_local_completion(
    completion: TaskCompletion,
    task: &TaskDefinition,
    node_id: &NodeId,
    network: &dyn Transport,
    event_bus: &EventBus,
    pending_results: &tokio::sync::mpsc::Sender<PendingResult>,
    pending_capacity: usize,
    orchestrator_bridge: Option<&OrchestratorBridge>,
) {
    // 本地编排回灌：workflow 任务的完成事件除发布到 EventBus 外，还回灌
    // orchestrator——Python 路径的后继派发由「提交方阻塞解析依赖 + 事件泵
    // 回灌」驱动，而 Rust 原生 DAG 提交（submit + start）没有事件泵，若不回灌，
    // 依赖推进产出的后继任务无人入队，DAG 永不推进。core 内补齐：Completed 经
    // COMPLETE_TASK 通道取 ready_successors 入队；Failed 经 ON_TASK_RESULT
    // 做重试裁决（bridge.ingest 内部处理）。
    if let Some(bridge) = orchestrator_bridge {
        match bridge.apply(&completion, task).await {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!(
                    task_id = %task.id.as_str(),
                    error = %e,
                    "local orchestration ingest failed; retrying in background"
                );
                let bridge = bridge.clone();
                let completion = completion.clone();
                let task = task.clone();
                tokio::spawn(retry_local_orchestration_ingest(bridge, completion, task));
            }
        }
    }

    publish_task_completion(
        completion,
        task,
        node_id,
        network,
        event_bus,
        pending_results,
        pending_capacity,
    )
    .await;
}

/// 本地编排回灌失败后的后台重试次数与退避基数。
const ORCHESTRATION_INGEST_MAX_ATTEMPTS: usize = 3;
const ORCHESTRATION_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
/// 本地入队（后继 / 重试任务）的重试次数与退避基数。
const LOCAL_ENQUEUE_MAX_ATTEMPTS: usize = 3;
const LOCAL_ENQUEUE_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// 编排回灌失败后按指数退避重试。
///
/// actor 短暂不可用或消息丢失不应让 workflow 永久停摆；重试耗尽后记录
/// error（workflow 视图由恢复/失联接管兜底）。
async fn retry_local_orchestration_ingest(
    bridge: OrchestratorBridge,
    completion: TaskCompletion,
    task: TaskDefinition,
) {
    let mut backoff = ORCHESTRATION_RETRY_BASE_DELAY;
    for attempt in 1..=ORCHESTRATION_INGEST_MAX_ATTEMPTS {
        tokio::time::sleep(backoff).await;
        match bridge.apply(&completion, &task).await {
            Ok(()) => {
                tracing::info!(
                    task_id = %task.id.as_str(),
                    attempt,
                    "local orchestration ingest retry succeeded"
                );
                return;
            }
            Err(e) if attempt < ORCHESTRATION_INGEST_MAX_ATTEMPTS => {
                tracing::warn!(
                    task_id = %task.id.as_str(),
                    attempt,
                    error = %e,
                    "local orchestration ingest retry failed"
                );
                backoff = backoff.saturating_mul(2);
            }
            Err(e) => {
                tracing::error!(
                    task_id = %task.id.as_str(),
                    attempt,
                    error = %e,
                    "local orchestration ingest failed after retries; workflow may stall"
                );
                return;
            }
        }
    }
}

/// 入队就绪后继或编排重试任务；失败按指数退避重试。
///
/// `initial_delay` 用于尊重编排器给出的重试延迟。重试耗尽后记录 error：
/// 任务保持未入队状态，由工作流恢复/失联接管重新评估。
async fn enqueue_with_retry(
    scheduler: Arc<dyn crate::runtime::workflow::Scheduler>,
    tasks: Vec<TaskDefinition>,
    initial_delay: Duration,
    description: &'static str,
) {
    if !initial_delay.is_zero() {
        tokio::time::sleep(initial_delay).await;
    }
    let mut backoff = LOCAL_ENQUEUE_RETRY_BASE_DELAY;
    for attempt in 1..=LOCAL_ENQUEUE_MAX_ATTEMPTS {
        let result = match tasks.as_slice() {
            [single] => scheduler.enqueue(single.clone()).await,
            _ => scheduler.enqueue_batch(tasks.clone()).await,
        };
        match result {
            Ok(()) => return,
            Err(e) if attempt < LOCAL_ENQUEUE_MAX_ATTEMPTS => {
                tracing::warn!(
                    attempt,
                    error = %e,
                    description,
                    "failed to enqueue tasks; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            Err(e) => {
                tracing::error!(
                    attempt,
                    error = %e,
                    description,
                    "failed to enqueue tasks after retries; workflow may stall"
                );
                return;
            }
        }
    }
}

/// 本地结果 → orchestrator 的回灌桥。
///
/// 持有 ON_TASK_RESULT 通道两端（actor_system + workflow_actor_id）与调度器
/// 引用；`ingest` 返回 [`IngestAction`] 描述需要入队的后继/重试任务。
/// `None` 变体由 `settle_local_completion` 的调用方在 spawn 时按 Worker 内部
/// 状态构造（未绑定 workflow actor 时为 `None`，降级为纯事件结算）。
#[derive(Clone)]
pub(super) struct OrchestratorBridge {
    pub(super) actor_system: Arc<crate::runtime::actor::ActorSystem>,
    pub(super) workflow_actor_id: crate::common::ActorId,
    pub(super) scheduler: Arc<dyn crate::runtime::workflow::Scheduler>,
}

/// 编排回灌的结果：需要由调用方入队的任务。
enum IngestAction {
    /// 无需入队（终局或非工作流任务）。
    None,
    /// 依赖推进产出的就绪后继任务。
    Successors(Vec<TaskDefinition>),
    /// 编排器裁决的重试任务及其延迟。
    Retry {
        task: Box<TaskDefinition>,
        delay_ms: u64,
    },
}

impl OrchestratorBridge {
    /// 回灌结果并将就绪后继/重试任务入队。入队失败在内部重试；
    /// `Err` 仅表示回灌本身失败。
    async fn apply(
        &self,
        completion: &TaskCompletion,
        task: &TaskDefinition,
    ) -> crate::common::Result<()> {
        match self.ingest(completion, task).await? {
            IngestAction::None => {}
            IngestAction::Successors(tasks) => {
                enqueue_with_retry(
                    self.scheduler.clone(),
                    tasks,
                    Duration::ZERO,
                    "ready successor tasks",
                )
                .await;
            }
            IngestAction::Retry {
                task: retry_task,
                delay_ms,
            } => {
                enqueue_with_retry(
                    self.scheduler.clone(),
                    vec![*retry_task],
                    Duration::from_millis(delay_ms),
                    "orchestrator-driven retry task",
                )
                .await;
            }
        }
        Ok(())
    }

    /// 结果单入口回灌。`Err` = 回灌失败（调用方退避重试）。
    async fn ingest(
        &self,
        completion: &TaskCompletion,
        task: &TaskDefinition,
    ) -> crate::common::Result<IngestAction> {
        // 返回需要入队的任务（就绪后继 / 重试裁决），由调用方统一入队。
        let Some(ref workflow_id) = task.workflow_id else {
            return Ok(IngestAction::None);
        };
        if workflow_id.as_str().is_empty() {
            return Ok(IngestAction::None);
        }
        match completion {
            TaskCompletion::Completed { result, .. } => {
                // Completed 走 COMPLETE_TASK 通道：响应是 TaskCompletionResponse
                //（含 ready_successors——Rust 原生 DAG 提交路径的后继派发腿，
                // Python 路径由提交方阻塞解析依赖而无需此腿）。
                let payload = encode(&(workflow_id.clone(), task.id.clone(), result.clone()))?;
                let response = self
                    .actor_system
                    .call(
                        &self.workflow_actor_id,
                        crate::runtime::workflow::actor::workflow_methods::COMPLETE_TASK,
                        payload,
                    )
                    .await?;
                if let Some(error) = response.error {
                    return Err(ActantError::from(error));
                }
                let parsed: TaskCompletionResponse = if response.payload.is_empty() {
                    tracing::warn!(
                        "empty COMPLETE_TASK response payload; treating as no ready successors"
                    );
                    return Ok(IngestAction::None);
                } else {
                    decode(&response.payload)?
                };
                Ok(IngestAction::Successors(parsed.ready_successors))
            }
            TaskCompletion::Failed { error, .. } => {
                let outcome = TaskResultOutcome::Failed(error.clone());
                let retry = self
                    .ingest_for_retry(workflow_id, &task.id, outcome)
                    .await?;
                Ok(match retry {
                    Some((task, delay_ms)) => IngestAction::Retry {
                        task: Box::new(task),
                        delay_ms,
                    },
                    None => IngestAction::None,
                })
            }
            // Cancelled：编排侧由 cancel 路径收敛，事件结算即可。
            TaskCompletion::Cancelled { .. } => Ok(IngestAction::None),
        }
    }

    /// Failed 结果的重试裁决通道（ON_TASK_RESULT 单入口）。
    async fn ingest_for_retry(
        &self,
        workflow_id: &crate::common::WorkflowId,
        task_id: &crate::common::TaskId,
        outcome: TaskResultOutcome,
    ) -> crate::common::Result<Option<(TaskDefinition, u64)>> {
        let payload = encode(&(
            workflow_id.clone(),
            task_id.clone(),
            outcome,
            None::<u32>,
            ResultSource::Local,
        ))?;
        let result = self
            .actor_system
            .call(
                &self.workflow_actor_id,
                crate::runtime::workflow::actor::workflow_methods::ON_TASK_RESULT,
                payload,
            )
            .await?;
        if let Some(error) = result.error {
            return Err(ActantError::from(error));
        }
        // 响应载荷是 `Option<(TaskDefinition, u64)>`：`None` = 终局。
        // postcard 的 `None` 占一个判别字节，必须按 `Option` 解码。
        let verdict: Option<(TaskDefinition, u64)> = if result.payload.is_empty() {
            // 生产 handler 总是 encode（None 也有判别字节）；空载荷意味着
            // 协议被破坏，显性化「可能丢失的重试裁决」而不是静默当终局。
            tracing::warn!("empty ON_TASK_RESULT response payload; treating verdict as terminal");
            None
        } else {
            decode(&result.payload)?
        };
        Ok(verdict)
    }
}

/// 将任务完成结果发布到事件总线或回传给远端 orchestrator。
///
/// 抽出为独立 async 函数，覆盖本地发布、远程 ``TaskResultAck`` 接受/拒绝、
/// 异常响应、网络错误等路径，无需构造完整 Worker 执行循环。
pub(super) async fn publish_task_completion(
    completion: TaskCompletion,
    task: &TaskDefinition,
    node_id: &NodeId,
    network: &dyn crate::runtime::network::Transport,
    event_bus: &EventBus,
    pending_results: &tokio::sync::mpsc::Sender<PendingResult>,
    _pending_capacity: usize,
) {
    let is_remote_task = task.origin_node.as_ref().is_some_and(|o| o != node_id);

    if is_remote_task {
        let workflow_id = task
            .workflow_id
            .clone()
            .unwrap_or_else(|| WorkflowId::from(String::new()));
        let wire_result = completion.to_wire_result(workflow_id.clone());

        if let Some(ref origin) = task.origin_node {
            // 优先使用 origin_endpoint_addr（iroh 公钥），否则回退到 node_id
            let origin_addr = task
                .origin_endpoint_addr
                .as_deref()
                .unwrap_or(origin.as_str());
            let request = crate::runtime::network::DirectRequest::TaskResult {
                workflow_id: workflow_id.clone(),
                task_id: wire_result.task_id.clone(),
                task_name: wire_result.task_name.clone(),
                outcome: wire_result.outcome.clone(),
                worker_node: node_id.clone(),
                attempt: task.attempt,
            };
            // 尝试一次；失败则入队异步重试
            match network
                .send_direct_request(origin_addr, request.clone())
                .await
            {
                Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: true }) => {
                    tracing::debug!(
                        "task result delivered directly to orchestrator {}",
                        origin_addr
                    );
                }
                Ok(crate::runtime::network::DirectResponse::TaskResultAck { accepted: false }) => {
                    tracing::warn!(
                        "orchestrator {} rejected task result, enqueuing for retry",
                        origin_addr
                    );
                    // 队列满时等待（背压）而不是在本地发 TaskFailed——origin
                    // 在远端，本地事件无法终止它的等待。
                    if !enqueue_pending_result(pending_results, origin_addr.to_string(), request, 0)
                        .await
                    {
                        tracing::warn!(
                            origin = %origin_addr,
                            "result retry queue closed during shutdown; origin will re-dispatch"
                        );
                    }
                }
                Ok(_) => {
                    tracing::warn!(
                        "unexpected response from {}, enqueuing result for retry",
                        origin_addr
                    );
                    if !enqueue_pending_result(pending_results, origin_addr.to_string(), request, 0)
                        .await
                    {
                        tracing::warn!(
                            origin = %origin_addr,
                            "result retry queue closed during shutdown; origin will re-dispatch"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "direct result delivery to {} failed: {}, enqueuing for retry",
                        origin_addr,
                        e
                    );
                    if !enqueue_pending_result(pending_results, origin_addr.to_string(), request, 0)
                        .await
                    {
                        tracing::warn!(
                            origin = %origin_addr,
                            "result retry queue closed during shutdown; origin will re-dispatch"
                        );
                    }
                }
            }
        }
    } else {
        // 本地 task 完成：始终发布到 EventBus（即使无 workflow_id），
        // 使 Python @task 等非工作流任务也能通过事件总线获取结果。
        let bus_event = match completion {
            TaskCompletion::Failed { .. } => BusEvent::TaskFailed(completion),
            TaskCompletion::Cancelled { .. } => BusEvent::TaskCancelled(completion),
            TaskCompletion::Completed { .. } => BusEvent::TaskCompleted(completion),
        };
        event_bus.publish(bus_event);
    }
}
