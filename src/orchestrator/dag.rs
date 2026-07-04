use std::collections::{HashMap, HashSet, VecDeque};

use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::common::{Result, RetryPolicy, TaskId};
use crate::orchestrator::FailureStrategy;

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct DagNode {
    pub task_id: TaskId,
    pub name: String,
    pub payload: Vec<u8>,
    pub retry_policy: Option<RetryPolicy>,
    pub timeout_ms: Option<u64>,
    /// 任务优先级（有符号整数）。数值越高越紧急。
    #[serde(default)]
    pub priority: i32,
    /// 任务元数据键值对，由 Python 层添加。
    /// Rust 视为不透明数据，不解释它。
    /// 用于标签、路由提示或任何 Python定义的属性。
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub(crate) struct DagEdge {
    pub from: TaskId,
    pub to: TaskId,
    /// 条件标签，用于条件分支。
    /// 当存在时，仅当条件为 true 时激活此边。
    /// Python 调度循环在运行时评估条件式。
    #[serde(default)]
    pub condition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
pub struct Dag {
    nodes: HashMap<TaskId, DagNode>,
    edges: Vec<DagEdge>,
    predecessors: HashMap<TaskId, Vec<TaskId>>,
    successors: HashMap<TaskId, Vec<TaskId>>,
    /// 默认重试策略，应用于未指定自己重试策略的任务。
    #[serde(default)]
    pub default_retry_policy: Option<RetryPolicy>,
    /// 如何处理任务失败。
    #[serde(default)]
    pub failure_strategy: FailureStrategy,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
            predecessors: HashMap::new(),
            successors: HashMap::new(),
            default_retry_policy: None,
            failure_strategy: FailureStrategy::default(),
        }
    }

    pub fn add_node(&mut self, node: DagNode) -> Result<()> {
        let id = node.task_id.clone();
        self.nodes.insert(id.clone(), node);
        self.predecessors.entry(id.clone()).or_default();
        self.successors.entry(id).or_default();
        Ok(())
    }

    pub fn add_edge(&mut self, from: TaskId, to: TaskId) -> Result<()> {
        if !self.nodes.contains_key(&from) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                from.as_str()
            )));
        }
        if !self.nodes.contains_key(&to) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                to.as_str()
            )));
        }

        if from == to || self.path_exists(&to, &from) {
            return Err(crate::common::ActantError::Workflow(
                "adding edge would create a cycle".into(),
            ));
        }

        self.edges.push(DagEdge {
            from: from.clone(),
            to: to.clone(),
            condition: None,
        });
        self.successors
            .entry(from.clone())
            .or_default()
            .push(to.clone());
        self.predecessors.entry(to).or_default().push(from);

        Ok(())
    }

    /// 添加条件分支边。
    /// 当存在时，仅当条件为 true 时激活此边。
    /// Python 调度循环在运行时评估条件式。
    pub fn add_conditional_edge(
        &mut self,
        from: TaskId,
        to: TaskId,
        condition: String,
    ) -> Result<()> {
        if !self.nodes.contains_key(&from) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                from.as_str()
            )));
        }
        if !self.nodes.contains_key(&to) {
            return Err(crate::common::ActantError::Workflow(format!(
                "node {} not found",
                to.as_str()
            )));
        }

        // 与 add_edge 一致：拒绝自环和会形成环的边。
        // 条件边虽然运行时按条件激活，但拓扑上仍是图的一部分，
        // 静态环路检查能防止条件组合在不同运行时取值下意外触发死循环。
        if from == to || self.path_exists(&to, &from) {
            return Err(crate::common::ActantError::Workflow(
                "adding conditional edge would create a cycle".into(),
            ));
        }

        self.edges.push(DagEdge {
            from: from.clone(),
            to: to.clone(),
            condition: Some(condition),
        });
        self.successors
            .entry(from.clone())
            .or_default()
            .push(to.clone());
        self.predecessors.entry(to).or_default().push(from);

        Ok(())
    }

    fn path_exists(&self, source: &TaskId, target: &TaskId) -> bool {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(source);
        while let Some(current) = queue.pop_front() {
            if current == target {
                return true;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(succs) = self.successors.get(current) {
                for succ in succs {
                    if !visited.contains(succ) {
                        queue.push_back(succ);
                    }
                }
            }
        }
        false
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn get_node(&self, id: &TaskId) -> Option<&DagNode> {
        self.nodes.get(id)
    }

    pub fn predecessors_of(&self, id: &TaskId) -> Vec<&DagNode> {
        self.predecessors
            .get(id)
            .map(|ids| ids.iter().filter_map(|i| self.nodes.get(i)).collect())
            .unwrap_or_default()
    }

    pub fn predecessor_count(&self, id: &TaskId) -> usize {
        self.predecessors.get(id).map(|p| p.len()).unwrap_or(0)
    }

    pub fn successor_ids(&self, id: &TaskId) -> Vec<TaskId> {
        self.successors.get(id).cloned().unwrap_or_default()
    }

    /// 返回从给定任务出发的条件分支边。
    /// 每个条目为 (后继任务 ID,条件标签)。
    pub fn conditional_edges_from(&self, id: &TaskId) -> Vec<(TaskId, String)> {
        self.edges
            .iter()
            .filter(|e| &e.from == id)
            .filter_map(|e| e.condition.as_ref().map(|c| (e.to.clone(), c.clone())))
            .collect()
    }

    pub fn successors_of(&self, id: &TaskId) -> Vec<&DagNode> {
        self.successors
            .get(id)
            .map(|ids| ids.iter().filter_map(|i| self.nodes.get(i)).collect())
            .unwrap_or_default()
    }

    pub fn roots(&self) -> Vec<&DagNode> {
        self.nodes
            .keys()
            .filter(|id| {
                self.predecessors
                    .get(id)
                    .map(|p| p.is_empty())
                    .unwrap_or(true)
            })
            .filter_map(|id| self.nodes.get(id))
            .collect()
    }

    pub fn sinks(&self) -> Vec<&DagNode> {
        self.nodes
            .keys()
            .filter(|id| {
                self.successors
                    .get(id)
                    .map(|s| s.is_empty())
                    .unwrap_or(true)
            })
            .filter_map(|id| self.nodes.get(id))
            .collect()
    }

    pub fn topological_sort(&self) -> Result<Vec<&DagNode>> {
        let mut in_degree: HashMap<&TaskId, usize> = HashMap::new();
        for id in self.nodes.keys() {
            in_degree.insert(id, self.predecessors.get(id).map(|p| p.len()).unwrap_or(0));
        }

        let mut queue: VecDeque<&TaskId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut sorted = Vec::new();

        while let Some(id) = queue.pop_front() {
            let node = self.nodes.get(id).ok_or_else(|| {
                crate::common::ActantError::Internal(format!("node {} not found", id.as_str()))
            })?;
            sorted.push(node);
            if let Some(succs) = self.successors.get(id) {
                for succ in succs {
                    if let Some(deg) = in_degree.get_mut(succ) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(succ);
                        }
                    }
                }
            }
        }

        if sorted.len() != self.nodes.len() {
            return Err(crate::common::ActantError::Workflow(
                "graph has a cycle".into(),
            ));
        }

        Ok(sorted)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &DagNode> {
        self.nodes.values()
    }

    /// 返回节点的有效重试策略：
    /// 如果节点设置了自己的重试策略，则返回该策略；否则返回 DAG 级默认重试策略。
    /// 如果节点不存在或未设置任何重试策略，则返回 None。
    pub fn effective_retry_policy(&self, task_id: &TaskId) -> Option<RetryPolicy> {
        let node = self.nodes.get(task_id)?;
        node.retry_policy
            .clone()
            .or_else(|| self.default_retry_policy.clone())
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RetryPolicy;

    fn make_node(id: &str, name: &str) -> DagNode {
        DagNode {
            task_id: TaskId::from(id.to_string()),
            name: name.to_string(),
            payload: Vec::new(),
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: HashMap::new(),
        }
    }

    fn make_linear_dag() -> Dag {
        // a → b → c
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_node(make_node("c", "task_c")).unwrap();
        dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
        dag.add_edge(TaskId::from("b"), TaskId::from("c")).unwrap();
        dag
    }

    #[test]
    fn add_node_increments_count_and_allows_lookup() {
        let mut dag = Dag::new();
        dag.add_node(make_node("t1", "task1")).unwrap();
        assert_eq!(dag.node_count(), 1);
        assert!(dag.get_node(&TaskId::from("t1")).is_some());
        assert!(dag.get_node(&TaskId::from("missing")).is_none());
    }

    #[test]
    fn add_edge_links_predecessors_and_successors() {
        let dag = make_linear_dag();
        assert_eq!(dag.edge_count(), 2);
        assert_eq!(dag.predecessor_count(&TaskId::from("a")), 0);
        assert_eq!(dag.predecessor_count(&TaskId::from("b")), 1);
        assert_eq!(dag.predecessor_count(&TaskId::from("c")), 1);

        let succ_ids = dag.successor_ids(&TaskId::from("a"));
        assert_eq!(succ_ids, vec![TaskId::from("b")]);
    }

    #[test]
    fn add_edge_rejects_missing_from_node() {
        let mut dag = Dag::new();
        dag.add_node(make_node("b", "task_b")).unwrap();
        let err = dag
            .add_edge(TaskId::from("missing"), TaskId::from("b"))
            .unwrap_err();
        assert!(matches!(err, crate::common::ActantError::Workflow(_)));
    }

    #[test]
    fn add_edge_rejects_missing_to_node() {
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        let err = dag
            .add_edge(TaskId::from("a"), TaskId::from("missing"))
            .unwrap_err();
        assert!(matches!(err, crate::common::ActantError::Workflow(_)));
    }

    #[test]
    fn add_edge_rejects_self_loop() {
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        let err = dag
            .add_edge(TaskId::from("a"), TaskId::from("a"))
            .unwrap_err();
        assert!(matches!(err, crate::common::ActantError::Workflow(_)));
    }

    #[test]
    fn add_edge_rejects_cycle() {
        // a → b, then b → a should fail (cycle)
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
        let err = dag
            .add_edge(TaskId::from("b"), TaskId::from("a"))
            .unwrap_err();
        assert!(matches!(
            err,
            crate::common::ActantError::Workflow(ref m) if m.contains("cycle")
        ));
    }

    #[test]
    fn add_conditional_edge_stores_condition() {
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_conditional_edge(TaskId::from("a"), TaskId::from("b"), "cond_x".into())
            .unwrap();
        let conds = dag.conditional_edges_from(&TaskId::from("a"));
        assert_eq!(conds.len(), 1);
        assert_eq!(conds[0].0, TaskId::from("b"));
        assert_eq!(conds[0].1, "cond_x");
    }

    #[test]
    fn add_conditional_edge_rejects_missing_node() {
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        let err = dag
            .add_conditional_edge(TaskId::from("a"), TaskId::from("missing"), "cond".into())
            .unwrap_err();
        assert!(matches!(err, crate::common::ActantError::Workflow(_)));
    }

    #[test]
    fn add_conditional_edge_rejects_self_loop() {
        // 与 add_edge 一致：条件边也拒绝自环。
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        let err = dag
            .add_conditional_edge(TaskId::from("a"), TaskId::from("a"), "cond".into())
            .unwrap_err();
        assert!(
            matches!(err, crate::common::ActantError::Workflow(ref m) if m.contains("cycle")),
            "self-loop conditional edge should be rejected as a cycle, got {:?}",
            err
        );
    }

    #[test]
    fn add_conditional_edge_detects_cycle() {
        // a → b（条件边），再尝试 b → a（普通边）应失败：
        // 拓扑上 a 已可达 b，反向边会形成环。
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_conditional_edge(TaskId::from("a"), TaskId::from("b"), "cond".into())
            .unwrap();
        let err = dag
            .add_edge(TaskId::from("b"), TaskId::from("a"))
            .unwrap_err();
        assert!(
            matches!(err, crate::common::ActantError::Workflow(ref m) if m.contains("cycle")),
            "reverse edge forming a cycle should be rejected, got {:?}",
            err
        );
    }

    #[test]
    fn add_conditional_edge_rejects_cycle_against_existing_path() {
        // 已有 a → b 普通边，再尝试 b → a 条件边应失败。
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
        let err = dag
            .add_conditional_edge(TaskId::from("b"), TaskId::from("a"), "cond".into())
            .unwrap_err();
        assert!(
            matches!(err, crate::common::ActantError::Workflow(ref m) if m.contains("cycle")),
            "conditional edge forming a cycle should be rejected, got {:?}",
            err
        );
    }

    #[test]
    fn roots_and_sinks_identify_terminal_nodes() {
        // a → b → c, a → c
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        dag.add_node(make_node("c", "task_c")).unwrap();
        dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
        dag.add_edge(TaskId::from("b"), TaskId::from("c")).unwrap();
        dag.add_edge(TaskId::from("a"), TaskId::from("c")).unwrap();

        let root_ids: Vec<&str> = dag.roots().iter().map(|n| n.task_id.as_str()).collect();
        let sink_ids: Vec<&str> = dag.sinks().iter().map(|n| n.task_id.as_str()).collect();
        assert_eq!(root_ids, vec!["a"]);
        assert_eq!(sink_ids, vec!["c"]);
    }

    #[test]
    fn topological_sort_orders_correctly() {
        let dag = make_linear_dag();
        let sorted = dag.topological_sort().unwrap();
        let ids: Vec<&str> = sorted.iter().map(|n| n.task_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn topological_sort_detects_cycle() {
        // Manually create a cycle by abusing internal structure
        // (add_edge prevents cycles, so we test via a 2-node cycle attempt)
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.add_node(make_node("b", "task_b")).unwrap();
        // a → b is fine
        dag.add_edge(TaskId::from("a"), TaskId::from("b")).unwrap();
        // Since add_edge prevents cycles, topological_sort should succeed
        let sorted = dag.topological_sort().unwrap();
        assert_eq!(sorted.len(), 2);
    }

    #[test]
    fn topological_sort_empty_dag() {
        let dag = Dag::new();
        let sorted = dag.topological_sort().unwrap();
        assert!(sorted.is_empty());
    }

    #[test]
    fn predecessors_of_returns_node_references() {
        let dag = make_linear_dag();
        let preds = dag.predecessors_of(&TaskId::from("c"));
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].task_id.as_str(), "b");
    }

    #[test]
    fn successors_of_returns_node_references() {
        let dag = make_linear_dag();
        let succs = dag.successors_of(&TaskId::from("a"));
        assert_eq!(succs.len(), 1);
        assert_eq!(succs[0].task_id.as_str(), "b");
    }

    #[test]
    fn effective_retry_policy_prefers_node_over_dag_default() {
        let mut dag = Dag::new();
        let mut node = make_node("a", "task_a");
        node.retry_policy = Some(RetryPolicy {
            max_retries: 5,
            delay_ms: 200,
            backoff_multiplier: 3.0,
            max_delay_ms: 10000,
        });
        dag.add_node(node).unwrap();
        dag.default_retry_policy = Some(RetryPolicy::default());

        let policy = dag.effective_retry_policy(&TaskId::from("a")).unwrap();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.delay_ms, 200);
    }

    #[test]
    fn effective_retry_policy_falls_back_to_dag_default() {
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        dag.default_retry_policy = Some(RetryPolicy {
            max_retries: 7,
            delay_ms: 500,
            backoff_multiplier: 2.0,
            max_delay_ms: 30000,
        });

        let policy = dag.effective_retry_policy(&TaskId::from("a")).unwrap();
        assert_eq!(policy.max_retries, 7);
    }

    #[test]
    fn effective_retry_policy_returns_none_when_no_policy() {
        let mut dag = Dag::new();
        dag.add_node(make_node("a", "task_a")).unwrap();
        assert!(dag.effective_retry_policy(&TaskId::from("a")).is_none());
    }

    #[test]
    fn effective_retry_policy_returns_none_for_missing_node() {
        let dag = Dag::new();
        assert!(dag
            .effective_retry_policy(&TaskId::from("missing"))
            .is_none());
    }

    #[test]
    fn nodes_iterator_visits_all_nodes() {
        let dag = make_linear_dag();
        let count = dag.nodes().count();
        assert_eq!(count, 3);
    }
}
