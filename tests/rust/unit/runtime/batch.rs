//! Unit tests extracted from `src/runtime/batch.rs`.

use super::*;
use crate::common::{TaskCompletion, TaskId, WorkflowId};

fn tid(s: &str) -> TaskId {
    TaskId::from(s)
}

fn completed(task: &str) -> TaskCompletion {
    TaskCompletion::Completed {
        workflow_id: WorkflowId::from("wf"),
        task_id: tid(task),
        task_name: "n".into(),
        result: Vec::new(),
        target_node: None,
    }
}

#[test]
fn register_reports_total_and_pending() {
    let tracker = BatchTracker::new();
    let p = tracker.register("b-1", [tid("t-1"), tid("t-2")]);
    assert_eq!(p.total, 2);
    assert_eq!(p.pending, 2);
    assert!(!p.finished());
}

#[test]
fn terminal_counts_converge_to_finished() {
    let tracker = BatchTracker::new();
    tracker.register("b-1", [tid("t-1"), tid("t-2"), tid("t-3")]);

    tracker.record_terminal(&tid("t-1"), BatchItemState::Completed);
    tracker.record_terminal(&tid("t-2"), BatchItemState::Failed);
    let p = tracker
        .record_terminal(&tid("t-3"), BatchItemState::Cancelled)
        .unwrap();

    assert_eq!(p.completed, 1);
    assert_eq!(p.failed, 1);
    assert_eq!(p.cancelled, 1);
    assert_eq!(p.pending, 0);
    assert!(p.finished());
}

#[test]
fn duplicate_terminal_is_idempotent() {
    let tracker = BatchTracker::new();
    tracker.register("b-1", [tid("t-1"), tid("t-2")]);

    assert!(tracker
        .record_terminal(&tid("t-1"), BatchItemState::Completed)
        .is_some());
    assert!(tracker
        .record_terminal(&tid("t-1"), BatchItemState::Completed)
        .is_none());

    let p = tracker.progress("b-1").unwrap();
    assert_eq!(p.completed, 1);
    assert_eq!(p.pending, 1);
}

#[test]
fn non_member_terminal_is_ignored() {
    let tracker = BatchTracker::new();
    tracker.register("b-1", [tid("t-1")]);
    assert!(tracker
        .record_terminal(&tid("t-other"), BatchItemState::Failed)
        .is_none());
    assert_eq!(tracker.progress("b-1").unwrap().pending, 1);
}

#[test]
fn incremental_registration_accumulates_members() {
    let tracker = BatchTracker::new();
    tracker.register("b-1", [tid("t-1")]);
    let p = tracker.register("b-1", [tid("t-2"), tid("t-1")]);
    assert_eq!(p.total, 2);
    // len() 读出口已随 API 收缩移除：经 progress 验证批次仍单批次存在。
    assert_eq!(tracker.progress("b-1").unwrap().total, 2);
}

#[test]
fn task_cannot_belong_to_two_batches() {
    let tracker = BatchTracker::new();
    tracker.register("b-1", [tid("t-1")]);
    // t-1 已属于 b-1：b-2 只应计入 t-2，不得把 t-1 重复计入。
    let p = tracker.register("b-2", [tid("t-1"), tid("t-2")]);
    assert_eq!(p.total, 1);
    assert_eq!(tracker.progress("b-1").unwrap().total, 1);
}

#[test]
fn concurrent_registration_of_same_task_counts_once() {
    use std::sync::{Arc, Barrier};

    let tracker = Arc::new(BatchTracker::new());
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = ["b-1", "b-2"]
        .into_iter()
        .map(|batch_id| {
            let tracker = Arc::clone(&tracker);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                tracker.register(batch_id, [tid("t-1")]);
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("registration thread panicked");
    }

    // 无论哪个线程先插入，任务只归属一个 batch：总数之和恒为 1。
    let totals: u32 = ["b-1", "b-2"]
        .iter()
        .filter_map(|id| tracker.progress(id))
        .map(|p| p.total)
        .sum();
    assert_eq!(totals, 1);
}

#[test]
fn completion_state_mapping() {
    assert_eq!(
        BatchItemState::from_completion(&completed("t-1")),
        Some(BatchItemState::Completed)
    );
    let running_like = TaskCompletion::Failed {
        workflow_id: WorkflowId::from("wf"),
        task_id: tid("t-1"),
        task_name: "n".into(),
        error: "e".into(),
        target_node: None,
    };
    assert_eq!(
        BatchItemState::from_completion(&running_like),
        Some(BatchItemState::Failed)
    );
}

#[test]
fn remove_clears_batch_and_members() {
    let tracker = BatchTracker::new();
    tracker.register("b-1", [tid("t-1")]);
    assert!(tracker.remove("b-1"));
    assert!(tracker.progress("b-1").is_none());
    assert!(tracker
        .record_terminal(&tid("t-1"), BatchItemState::Completed)
        .is_none());
}

/// `register` 的「写 members → 建 batches」窗口内，终态不得被永久丢弃。
#[test]
fn terminal_during_registration_window_is_recoverable() {
    let tracker = BatchTracker::new();
    let task = tid("t-race");
    // 模拟并发窗口：members 已有该任务，但 batch 状态尚未建立。
    tracker.members.insert(task.clone(), "b-race".to_string());
    assert!(tracker.batches.get("b-race").is_none());
    assert!(
        tracker
            .record_terminal(&task, BatchItemState::Completed)
            .is_none(),
        "batch 状态缺失时本次结算可以不产出进度，但必须保留成员以便重试"
    );
    assert!(
        tracker.members.contains_key(&task),
        "终态结算未产出进度时，成员映射不得被删除（否则事件永久丢失）"
    );
    // 注册补齐 batch 状态后，迟到的终态仍能收敛到 finished。
    tracker.register("b-race", [task.clone()]);
    let progress = tracker
        .record_terminal(&task, BatchItemState::Completed)
        .expect("成员映射保留后，重试必须能结算");
    assert!(progress.finished(), "batch 进度必须能收敛：{progress:?}");
}
