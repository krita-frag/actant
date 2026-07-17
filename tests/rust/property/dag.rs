//! Property-based tests for DAG topology operations.
//!
//! 运行: `cargo test --test dag_property`
//! 验证 DAG 的核心不变量：拓扑排序合法性、环检测、roots/sinks 正确性。
//! 使用 `proptest` 生成随机 DAG（通过随机边序列），覆盖手写测试难以穷举的图结构。

use actant::common::TaskId;
use actant::runtime::workflow::{Dag, DagNode};
use proptest::prelude::*;

/// 生成随机 TaskId（基于索引，确保唯一）。
fn task_id(i: usize) -> TaskId {
    TaskId::new(format!("task-{i}"))
}

/// 生成随机 DagNode。
fn dag_node(i: usize) -> DagNode {
    DagNode {
        task_id: task_id(i),
        name: format!("Task {i}"),
        payload: vec![],
        retry_policy: None,
        timeout_ms: None,
        priority: 0,
        metadata: Default::default(),
    }
}

/// 随机 DAG 生成策略：先创建 n 个节点，再随机添加 m 条无环边。
///
/// 为保证无环，每条边 (from, to) 必须 from < to（按节点索引序）。
/// 这是最简单的无环图构造策略，足以验证拓扑排序等不变量。
/// 重复边会被自动去重（与 `Dag::add_edge` 的非去重行为对齐预期）。
fn random_dag(n: usize, edge_pairs: Vec<(usize, usize)>) -> Dag {
    let mut dag = Dag::new();
    for i in 0..n {
        dag.add_node(dag_node(i)).unwrap();
    }
    // 去重后添加，避免重复边影响 edge_count 测试预期
    let unique_edges: std::collections::HashSet<_> = edge_pairs.into_iter().collect();
    for (from, to) in unique_edges {
        if from < to && from < n && to < n {
            let _ = dag.add_edge(task_id(from), task_id(to));
        }
    }
    dag
}

proptest! {
    /// 拓扑排序结果应包含所有节点，且每个节点恰好出现一次。
    #[test]
    fn topological_sort_contains_all_nodes(
        n in 1usize..20,
        edges in prop::collection::vec((0usize..20, 0usize..20), 0..40)
    ) {
        let dag = random_dag(n, edges);
        let sorted = dag.topological_sort().unwrap();
        prop_assert_eq!(sorted.len(), n, "all nodes should appear in topo sort");
    }

    /// 拓扑排序中，每个节点的位置应在其所有后继之前。
    ///
    /// 即：若存在边 A→B，则 A 在排序中的索引 < B 的索引。
    #[test]
    fn topological_sort_respects_edges(
        n in 1usize..15,
        edges in prop::collection::vec((0usize..15, 0usize..15), 0..30)
    ) {
        let dag = random_dag(n, edges);
        let sorted = dag.topological_sort().unwrap();

        // 构造 node_id → position 映射
        let mut position = std::collections::HashMap::new();
        for (idx, node) in sorted.iter().enumerate() {
            position.insert(node.task_id.as_str().to_string(), idx);
        }

        // 对每条边验证 from 的 position < to 的 position
        for i in 0..n {
            let succs = dag.successor_ids(&task_id(i));
            let from_pos = position[task_id(i).as_str()];
            for succ in succs {
                let to_pos = position[succ.as_str()];
                prop_assert!(
                    from_pos < to_pos,
                    "edge {}→{} violates topo order: {} >= {}",
                    task_id(i).as_str(),
                    succ.as_str(),
                    from_pos,
                    to_pos
                );
            }
        }
    }

    /// roots() 返回入度为 0 的节点（无前驱）。
    #[test]
    fn roots_are_nodes_without_predecessors(
        n in 1usize..15,
        edges in prop::collection::vec((0usize..15, 0usize..15), 0..30)
    ) {
        let dag = random_dag(n, edges);
        let roots: Vec<_> = dag.roots().into_iter().map(|n| n.task_id.clone()).collect();

        for i in 0..n {
            let tid = task_id(i);
            let preds = dag.predecessor_count(&tid);
            if preds == 0 {
                prop_assert!(
                    roots.contains(&tid),
                    "node {} has no predecessors, should be a root",
                    tid.as_str()
                );
            } else {
                prop_assert!(
                    !roots.contains(&tid),
                    "node {} has {} predecessors, should not be a root",
                    tid.as_str(),
                    preds
                );
            }
        }
    }

    /// sinks() 返回出度为 0 的节点（无后继）。
    #[test]
    fn sinks_are_nodes_without_successors(
        n in 1usize..15,
        edges in prop::collection::vec((0usize..15, 0usize..15), 0..30)
    ) {
        let dag = random_dag(n, edges);
        let sinks: Vec<_> = dag.sinks().into_iter().map(|n| n.task_id.clone()).collect();

        for i in 0..n {
            let tid = task_id(i);
            let succs = dag.successor_ids(&tid);
            if succs.is_empty() {
                prop_assert!(
                    sinks.contains(&tid),
                    "node {} has no successors, should be a sink",
                    tid.as_str()
                );
            } else {
                prop_assert!(
                    !sinks.contains(&tid),
                    "node {} has {} successors, should not be a sink",
                    tid.as_str(),
                    succs.len()
                );
            }
        }
    }

    /// add_edge 拒绝自环（from == to）。
    #[test]
    fn add_edge_rejects_self_loop(
        i in 0usize..10
    ) {
        let mut dag = Dag::new();
        dag.add_node(dag_node(i)).unwrap();
        let result = dag.add_edge(task_id(i), task_id(i));
        prop_assert!(result.is_err(), "self-loop should be rejected");
    }

    /// add_edge 拒绝形成环的边。
    ///
    /// 若已存在 A→B 路径，添加 B→A 应失败。
    #[test]
    fn add_edge_rejects_cycle(
        n in 2usize..10
    ) {
        let mut dag = Dag::new();
        for i in 0..n {
            dag.add_node(dag_node(i)).unwrap();
        }
        // 创建链 0→1→2→...→n-1
        for i in 0..n-1 {
            dag.add_edge(task_id(i), task_id(i + 1)).unwrap();
        }
        // 添加 n-1→0 应形成环，被拒绝
        let result = dag.add_edge(task_id(n - 1), task_id(0));
        prop_assert!(result.is_err(), "cycle-creating edge should be rejected");
    }

    /// node_count + edge_count 反映图的结构。
    #[test]
    fn node_and_edge_count_consistent(
        n in 0usize..15,
        edges in prop::collection::vec((0usize..15, 0usize..15), 0..30)
    ) {
        // 先去重计算预期边数，再构造 DAG
        let expected_edges: usize = edges.iter()
            .filter(|(from, to)| from < to && *from < n && *to < n)
            .collect::<std::collections::HashSet<_>>()
            .len();
        let dag = random_dag(n, edges);
        prop_assert_eq!(dag.node_count(), n);
        prop_assert_eq!(dag.edge_count(), expected_edges);
    }

    /// predecessors_of + successors_of 对称：若 A 在 B 的前驱中，则 B 在 A 的后继中。
    #[test]
    fn predecessors_and_successors_symmetric(
        n in 1usize..12,
        edges in prop::collection::vec((0usize..12, 0usize..12), 0..25)
    ) {
        let dag = random_dag(n, edges);

        for i in 0..n {
            let tid_i = task_id(i);
            let succs = dag.successor_ids(&tid_i);
            for succ in &succs {
                // i 的后继 succ 的前驱应包含 i
                let preds_of_succ = dag.predecessors_of(succ);
                let pred_ids: Vec<_> = preds_of_succ.iter().map(|n| n.task_id.clone()).collect();
                prop_assert!(
                    pred_ids.contains(&tid_i),
                    "if {}→{} is an edge, {} should be in predecessors of {}",
                    tid_i.as_str(),
                    succ.as_str(),
                    tid_i.as_str(),
                    succ.as_str()
                );
            }
        }
    }
}
