//! 直接请求-响应协议类型，用于点对点通信。
//!
//! 与 gossipsub（广播）不同，请求-响应提供两个特定节点之间的直接、可靠传输。用于：
//! - 任务结果交付（工作节点 → 协调节点）
//! - 任务分发（协调节点 → 特定工作节点）
//! - 工作流状态查询

use serde::{Deserialize, Serialize};

use crate::common::model::{NodeId, TaskId, WorkflowId};
use crate::common::wire::WireTaskOutcome;

/// 直接请求-响应协议类型，用于点对点通信。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectRequest {
    /// 直接将任务完成结果交付给协调节点。
    TaskResult {
        workflow_id: WorkflowId,
        task_id: TaskId,
        task_name: String,
        outcome: WireTaskOutcome,
        /// 执行任务的工作节点。
        worker_node: NodeId,
    },
    /// 直接将任务分发给特定工作节点。
    DispatchTask {
        task: crate::common::model::TaskDefinition,
    },
    /// 从协调节点查询工作流状态。
    QueryWorkflowState {
        workflow_id: WorkflowId,
        requesting_node: NodeId,
    },
    /// 远端 Actor 方法调用（点对点）。
    ActorCall {
        target: crate::common::ActorId,
        method: String,
        payload: Vec<u8>,
        /// 调用方节点 ID + correlation ID，用于回送响应。
        reply_to: crate::common::RemoteReplyAddress,
    },
}

/// 直接响应-请求协议类型，用于点对点通信。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectResponse {
    /// 任务结果交付的确认。
    TaskResultAck { accepted: bool },
    /// 任务分发的确认。
    DispatchAck { accepted: bool },
    /// 工作流状态查询的响应。
    WorkflowState {
        dag: Option<Vec<u8>>,
        execution: Option<Vec<u8>>,
        pending: Option<Vec<u8>>,
    },
    /// 远端 Actor 调用的响应。
    ActorCallResult {
        /// postcard 编码的 ActorMessageResult。
        result: Vec<u8>,
    },
}
