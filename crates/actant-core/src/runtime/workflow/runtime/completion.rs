//! Worker 完成结算与结果投递辅助（C1 移动性拆分自 `runtime.rs`）。
//!
//! 本文件承载任务完成路径的纯辅助逻辑：
//! - `is_worker_crash`：dispatcher 结果的崩溃分类；
//! - `build_completion_from_dispatch_result`：派发结果 → `TaskCompletion` 映射
//!   （超时/取消/业务失败的终态归类）；
//! - `settle_local_completion`：本地任务终态结算入口（编排回灌 + 事件发布）；
//! - `OrchestratorBridge`：本地结果 → orchestrator 的回灌桥（X2，仅 Rust 嵌入启用）；
//! - `publish_drained_task_cancellation`：drain 丢弃任务的 Cancelled 通知。
//!
//! 全部为模块级自由函数/类型；Worker 主循环（`runtime.rs`）按名调用。

use std::sync::Arc;
use std::time::Duration;

use super::result_delivery::{try_enqueue_pending_result, PendingResult};
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
    // 本地编排回灌（X2 验收实验发现的缺口）：workflow 任务的完成事件此前只
    // 发布到 EventBus——Python 路径的后继派发由「提交方阻塞解析依赖 + 事件泵
    // 回灌」驱动，而 Rust 原生 DAG 提交（submit + start）没有事件泵，依赖
    // 推进产出的后继任务无人入队，DAG 永不推进。core 内补齐：Completed 经
    // COMPLETE_TASK 通道取 ready_successors 入队；Failed 经 ON_TASK_RESULT
    // 做重试裁决（bridge.ingest 内部处理）。
    if let Some(bridge) = orchestrator_bridge {
        match bridge.ingest(&completion, task).await {
            Ok(ready_successors) if !ready_successors.is_empty() => {
                let scheduler = bridge.scheduler.clone();
                tokio::spawn(async move {
                    if let Err(e) = scheduler.enqueue_batch(ready_successors).await {
                        tracing::error!(
                            error = %e,
                            "failed to enqueue ready successor tasks (local result report)"
                        );
                    }
                });
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    task_id = %task.id.as_str(),
                    error = %e,
                    "local orchestration ingest failed; falling back to event-only settlement"
                );
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

/// 本地结果 → orchestrator 的回灌桥（X2）。
///
/// 持有 ON_TASK_RESULT 通道两端（actor_system + workflow_actor_id）与调度器
/// 引用；`ingest` 返回 `Some(OrchestratorRetry)` 表示 orchestrator 裁决重试。
/// `None` 变体由 `settle_local_completion` 的调用方在 spawn 时按 Worker 内部
/// 状态构造（未绑定 workflow actor 时为 `None`，降级为纯事件结算）。
pub(super) struct OrchestratorBridge {
    pub(super) actor_system: Arc<crate::runtime::actor::ActorSystem>,
    pub(super) workflow_actor_id: crate::common::ActorId,
    pub(super) scheduler: Arc<dyn crate::runtime::workflow::Scheduler>,
}

impl OrchestratorBridge {
    /// 结果单入口回灌。成功返回就绪后继任务（可为空）；`Err` = 回灌失败
    /// （调用方降级为事件结算并告警）。重试裁决在 Failed 分支内部处理。
    async fn ingest(
        &self,
        completion: &TaskCompletion,
        task: &TaskDefinition,
    ) -> crate::common::Result<Vec<TaskDefinition>> {
        // 返回「就绪后继任务」（依赖推进产出），由调用方入队调度器。
        // 重试裁决走同通道：Failed 时 ON_TASK_RESULT 响应携带 retry verdict，
        // 由 Worker::report_task_result 的既有路径延迟入队（此处不重复）。
        let Some(ref workflow_id) = task.workflow_id else {
            return Ok(Vec::new());
        };
        if workflow_id.as_str().is_empty() {
            return Ok(Vec::new());
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
                    return Ok(Vec::new());
                } else {
                    decode(&response.payload)?
                };
                Ok(parsed.ready_successors)
            }
            TaskCompletion::Failed { error, .. } => {
                let outcome = TaskResultOutcome::Failed(error.clone());
                let retry = self
                    .ingest_for_retry(workflow_id, &task.id, outcome)
                    .await?;
                if let Some((retry_task, delay_ms)) = retry {
                    let scheduler = self.scheduler.clone();
                    tokio::spawn(async move {
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                        if let Err(e) = scheduler.enqueue(retry_task).await {
                            tracing::error!(
                                error = %e,
                                "failed to enqueue orchestrator-driven retry task (local result report)"
                            );
                        }
                    });
                }
                Ok(Vec::new())
            }
            // Cancelled / Skipped：编排侧由 cancel 路径与条件边处理收敛，
            // 事件结算即可。
            TaskCompletion::Cancelled { .. } | TaskCompletion::Skipped { .. } => Ok(Vec::new()),
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
    pending_capacity: usize,
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
                    if !try_enqueue_pending_result(
                        pending_results,
                        origin_addr.to_string(),
                        request,
                        0,
                        pending_capacity,
                    )
                    .await
                    {
                        // 通道满：降级为 TaskFailed 事件，避免结果静默丢失。
                        let failed = TaskCompletion::Failed {
                            workflow_id: workflow_id.clone(),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format_error_kind(
                                "network",
                                &format!(
                                    "result delivery to orchestrator {} rejected and retry queue full",
                                    origin_addr
                                ),
                            ),
                            target_node: task.target_node.clone(),
                        };
                        event_bus.publish(BusEvent::TaskFailed(failed));
                    }
                }
                Ok(_) => {
                    tracing::warn!(
                        "unexpected response from {}, enqueuing result for retry",
                        origin_addr
                    );
                    if !try_enqueue_pending_result(
                        pending_results,
                        origin_addr.to_string(),
                        request,
                        0,
                        pending_capacity,
                    )
                    .await
                    {
                        let failed = TaskCompletion::Failed {
                            workflow_id: workflow_id.clone(),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format_error_kind(
                                "network",
                                &format!(
                                    "unexpected response from {} and retry queue full",
                                    origin_addr
                                ),
                            ),
                            target_node: task.target_node.clone(),
                        };
                        event_bus.publish(BusEvent::TaskFailed(failed));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "direct result delivery to {} failed: {}, enqueuing for retry",
                        origin_addr,
                        e
                    );
                    if !try_enqueue_pending_result(
                        pending_results,
                        origin_addr.to_string(),
                        request,
                        0,
                        pending_capacity,
                    )
                    .await
                    {
                        let failed = TaskCompletion::Failed {
                            workflow_id: workflow_id.clone(),
                            task_id: task.id.clone(),
                            task_name: task.name.clone(),
                            error: format_error_kind(
                                "network",
                                &format!(
                                    "delivery to {} failed: {} and retry queue full",
                                    origin_addr, e
                                ),
                            ),
                            target_node: task.target_node.clone(),
                        };
                        event_bus.publish(BusEvent::TaskFailed(failed));
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
            TaskCompletion::Skipped { .. } => BusEvent::TaskSkipped(completion),
            TaskCompletion::Completed { .. } => BusEvent::TaskCompleted(completion),
        };
        event_bus.publish(bus_event);
    }
}
