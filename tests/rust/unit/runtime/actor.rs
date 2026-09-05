//! Unit tests extracted from `src/runtime/actor.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::mpsc;

use crate::common::{
    ActantError, ActorConfig, ActorId, ActorMessage, ActorMessageResult, ActorStatus, MessageId,
    NodeId, Result,
};
use crate::runtime::event_bus::EventBus;
use crate::runtime::state::{
    ActorSnapshot, CheckpointManager, HybridLogicalClock, LmdbStore, Store, WalWriter,
};

struct EchoActor {
    received: Arc<StdMutex<Vec<String>>>,
    fail_method: Option<String>,
    panic_method: Option<String>,
}

impl EchoActor {
    fn new() -> (Self, Arc<StdMutex<Vec<String>>>) {
        let received = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                received: received.clone(),
                fail_method: None,
                panic_method: None,
            },
            received,
        )
    }

    fn with_fail(method: &str) -> (Self, Arc<StdMutex<Vec<String>>>) {
        let (mut actor, received) = Self::new();
        actor.fail_method = Some(method.to_string());
        (actor, received)
    }

    fn with_panic(method: &str) -> (Self, Arc<StdMutex<Vec<String>>>) {
        let (mut actor, received) = Self::new();
        actor.panic_method = Some(method.to_string());
        (actor, received)
    }
}

#[async_trait]
impl Actor for EchoActor {
    fn actor_type(&self) -> &str {
        "echo"
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        self.received.lock().unwrap().push(msg.method.clone());

        if self.panic_method.as_deref() == Some(&msg.method) {
            panic!("test panic in handle_message");
        }
        if self.fail_method.as_deref() == Some(&msg.method) {
            return Err(ActantError::Actor("intentional failure".into()));
        }

        Ok(ActorMessageResult {
            message_id: msg.id,
            payload: msg.payload.clone(),
            error: None,
        })
    }
}

#[tokio::test]
async fn new_context_starts_in_created_state() {
    let ctx = ActorContext::new(ActorId("a1".into()));
    assert_eq!(ctx.status, ActorStatus::Created);
    assert_eq!(ctx.actor_id.0, "a1");
}

#[tokio::test]
async fn spawn_and_send_delivers_message_to_actor() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("echo-1");
    let (actor, received) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let msg = ActorMessage::new(actor_id.clone(), "ping".into(), b"data".to_vec());
    system.send(&actor_id, msg).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let received = received.lock().unwrap();
    assert_eq!(*received, vec!["ping".to_string()]);
}

#[tokio::test]
async fn spawn_duplicate_actor_returns_already_exists() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("dup");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let (actor2, _) = EchoActor::new();
    let err = system.spawn(actor_id.clone(), actor2).await.unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn send_to_unknown_actor_returns_error() {
    let system = ActorSystem::new();
    let target = ActorId::from("ghost");
    let msg = ActorMessage::new(target.clone(), "ping".into(), vec![]);
    let err = system.send(&target, msg).await.unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn call_returns_reply_from_actor() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("echo-2");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let result = system
        .call(&actor_id, "echo", b"hello".to_vec())
        .await
        .unwrap();

    assert_eq!(result.payload, b"hello");
    assert!(result.error.is_none());
}

#[tokio::test]
async fn call_returns_error_when_actor_fails() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("fail-1");
    let (actor, _) = EchoActor::with_fail("boom");
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let result = system.call(&actor_id, "boom", vec![]).await.unwrap();
    assert!(result.error.is_some());
    assert!(result
        .error
        .unwrap()
        .message
        .contains("intentional failure"));
}

#[tokio::test]
async fn call_returns_error_when_actor_panics() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("panic-1");
    let (actor, _) = EchoActor::with_panic("explode");
    system.spawn(actor_id.clone(), actor).await.unwrap();

    let result = system.call(&actor_id, "explode", vec![]).await.unwrap();
    assert!(result.error.is_some());
    assert!(result.error.unwrap().message.contains("panicked"));
}

#[tokio::test]
async fn actor_panic_does_not_crash_other_actors() {
    let system = ActorSystem::new();

    let panic_id = ActorId::from("panic-2");
    let (panic_actor, _) = EchoActor::with_panic("boom");
    system.spawn(panic_id.clone(), panic_actor).await.unwrap();

    let healthy_id = ActorId::from("healthy");
    let (healthy_actor, healthy_received) = EchoActor::new();
    system
        .spawn(healthy_id.clone(), healthy_actor)
        .await
        .unwrap();

    let _ = system.call(&panic_id, "boom", vec![]).await;

    let result = system
        .call(&healthy_id, "ping", b"data".to_vec())
        .await
        .unwrap();
    assert_eq!(result.payload, b"data");

    let received = healthy_received.lock().unwrap();
    assert!(*received == vec!["ping".to_string()]);
}

#[tokio::test]
async fn stop_terminates_actor_gracefully() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("stop-1");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();

    assert_eq!(system.actor_status(&actor_id), Some(ActorStatus::Running));

    system.stop(&actor_id).await.unwrap();

    let status = system.actor_status(&actor_id);
    assert!(
        status.is_none() || status == Some(ActorStatus::Stopped),
        "expected None or Stopped, got {:?}",
        status
    );

    let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
    assert!(system.send(&actor_id, msg).await.is_err());
}

#[tokio::test]
async fn kill_aborts_actor_immediately() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("kill-1");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();

    system.kill(&actor_id).unwrap();

    let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
    assert!(system.send(&actor_id, msg).await.is_err());
}

#[tokio::test]
async fn list_actors_returns_all_spawned_actors() {
    let system = ActorSystem::new();
    let id1 = ActorId::from("list-1");
    let id2 = ActorId::from("list-2");
    let (a1, _) = EchoActor::new();
    let (a2, _) = EchoActor::new();
    system.spawn(id1.clone(), a1).await.unwrap();
    system.spawn(id2.clone(), a2).await.unwrap();

    let mut actors = system.list_actors();
    actors.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(actors.len(), 2);
    assert_eq!(actors[0].as_str(), "list-1");
    assert_eq!(actors[1].as_str(), "list-2");
}

#[tokio::test]
async fn actor_status_none_for_unknown_actor() {
    let system = ActorSystem::new();
    assert_eq!(system.actor_status(&ActorId::from("ghost")), None);
}

#[tokio::test]
async fn stop_unknown_actor_is_noop() {
    let system = ActorSystem::new();
    system.stop(&ActorId::from("ghost")).await.unwrap();
}

#[tokio::test]
async fn multiple_actors_process_messages_concurrently() {
    let system = ActorSystem::new();

    let id1 = ActorId::from("conc-1");
    let id2 = ActorId::from("conc-2");
    let (a1, r1) = EchoActor::new();
    let (a2, r2) = EchoActor::new();
    system.spawn(id1.clone(), a1).await.unwrap();
    system.spawn(id2.clone(), a2).await.unwrap();

    let s1 = system.send(&id1, ActorMessage::new(id1.clone(), "m1".into(), vec![]));
    let s2 = system.send(&id2, ActorMessage::new(id2.clone(), "m2".into(), vec![]));
    let (send_r1, send_r2) = tokio::join!(s1, s2);
    send_r1.unwrap();
    send_r2.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(*r1.lock().unwrap(), vec!["m1".to_string()]);
    assert_eq!(*r2.lock().unwrap(), vec!["m2".to_string()]);
}

#[tokio::test]
async fn with_config_sets_mailbox_capacity() {
    let config = ActorConfig {
        mailbox_capacity: 16,
        ..Default::default()
    };
    let system = ActorSystem::new().with_config(config);
    assert_eq!(system.config.mailbox_capacity, 16);
}

#[tokio::test]
async fn with_node_id_stores_node_id() {
    let system = ActorSystem::new().with_node_id(NodeId::from("node-1"));
    assert_eq!(system.node_id().unwrap().as_str(), "node-1");
}

#[tokio::test]
async fn mailbox_send_to_unknown_actor_returns_error() {
    let registry = MailboxRegistry::new();
    let target = ActorId("ghost".into());
    let msg = ActorMessage::new(target.clone(), "ping".into(), b"payload".to_vec());
    let err = registry.send(&target, msg).await.unwrap_err();
    assert!(err.to_string().contains("not found in mailbox registry"));
}

#[tokio::test]
async fn replay_after_replays_all_wal_events_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal_writer = WalWriter::open(&wal_path).unwrap();
    let persistence = ActorPersistence::new().with_wal(wal_writer, store);

    let actor_id = ActorId::from("replay-actor");
    let other_id = ActorId::from("other-actor");

    // 手动写入一个较早的检查点，模拟 checkpoint 之后仍有 WAL 事件的场景。
    {
        let checkpoint = persistence.checkpoint.lock();
        let cm = checkpoint.as_ref().unwrap();
        cm.save(&ActorSnapshot {
            actor_id: actor_id.clone(),
            actor_type: "test".to_string(),
            state: b"state-1".to_vec(),
            timestamp: HybridLogicalClock::new().tick(),
            sequence: 1,
            wal_offset: 0,
        })
        .unwrap();
    }

    // 追加多个 WAL 事件，并穿插其他 Actor 的事件。
    persistence
        .persist(actor_id.clone(), "test".to_string(), b"state-2".to_vec())
        .await;
    persistence
        .persist(
            other_id.clone(),
            "test".to_string(),
            b"other-state".to_vec(),
        )
        .await;
    persistence
        .persist(actor_id.clone(), "test".to_string(), b"state-3".to_vec())
        .await;

    // 从检查点 offset 重放，应返回该 Actor 的最终状态。
    let replayed = persistence.replay_after(actor_id.clone(), 0).await.unwrap();
    assert_eq!(replayed, b"state-3");

    // 其他 Actor 的事件应独立返回其最终状态。
    let other_replayed = persistence.replay_after(other_id.clone(), 0).await.unwrap();
    assert_eq!(other_replayed, b"other-state");
}

// ───────────────────────── ActorSystem builder 方法测试 ─────────────────────────

#[tokio::test]
async fn with_event_bus_stores_bus() {
    let bus = EventBus::new();
    let system = ActorSystem::new().with_event_bus(bus);
    // 注入的 bus 应替换默认 bus：经 system.event_bus 发布的生命周期错误
    // 应能被该 bus 上的订阅者收到。
    let mut rx = system
        .event_bus
        .subscribe(crate::runtime::event_bus::Topic::ActorLifecycleError);
    system.emit_lifecycle_error(ActorId::from("lc-1"), "boom".into());
    let event = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
        .await
        .expect("should receive lifecycle error event")
        .expect("channel not closed");
    match event {
        crate::runtime::event_bus::BusEvent::ActorLifecycleError { actor_id, error } => {
            assert_eq!(actor_id.as_str(), "lc-1");
            assert_eq!(error, "boom");
        }
        other => panic!("expected ActorLifecycleError, got {:?}", other),
    }
}

#[tokio::test]
async fn with_checkpoint_sets_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let cm = CheckpointManager::new(store);
    let system = ActorSystem::new().with_checkpoint(cm);
    // 通过 spawn + stop 验证不 panic
    let (actor, _) = EchoActor::new();
    system
        .spawn(ActorId::from("ckpt-actor"), actor)
        .await
        .unwrap();
}

#[tokio::test]
async fn with_wal_sets_persistence_and_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let wal_path = dir.path().join("sys.wal");
    let wal_writer = WalWriter::open(&wal_path).unwrap();
    let system = ActorSystem::new().with_wal(wal_writer, store);
    // spawn + persist + stop
    let (actor, _) = EchoActor::new();
    let actor_id = ActorId::from("wal-actor");
    system.spawn(actor_id.clone(), actor).await.unwrap();
    system.stop(&actor_id).await.unwrap();
}

// ───────────────────────── stop_timeout 测试 ─────────────────────────

#[tokio::test]
async fn stop_timeout_terminates_actor() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("timeout-stop");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();

    system
        .stop_timeout(&actor_id, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    // stop 后 send 应失败
    let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
    assert!(system.send(&actor_id, msg).await.is_err());
}

#[tokio::test]
async fn stop_timeout_unknown_actor_is_noop() {
    let system = ActorSystem::new();
    system
        .stop_timeout(&ActorId::from("ghost"), std::time::Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn stop_timeout_aborts_on_timeout() {
    // 使用一个长时间运行的 actor 来触发超时
    struct SlowActor;
    #[async_trait]
    impl Actor for SlowActor {
        fn actor_type(&self) -> &str {
            "slow"
        }
        async fn handle_message(&mut self, _msg: ActorMessage) -> Result<ActorMessageResult> {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Ok(ActorMessageResult {
                message_id: MessageId::generate(),
                payload: vec![],
                error: None,
            })
        }
        async fn on_stop(&mut self) -> Result<()> {
            // on_stop 也阻塞，触发 stop_timeout 的 abort 路径
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Ok(())
        }
    }
    let system = ActorSystem::new();
    let actor_id = ActorId::from("slow-1");
    system.spawn(actor_id.clone(), SlowActor).await.unwrap();

    // 给 actor 一个消息让它进入处理状态
    system
        .send(
            &actor_id,
            ActorMessage::new(actor_id.clone(), "slow".into(), vec![]),
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // stop_timeout 用很短的超时
    let start = std::time::Instant::now();
    system
        .stop_timeout(&actor_id, std::time::Duration::from_millis(100))
        .await
        .unwrap();
    // 应该在合理时间内返回（即使 actor 还在 sleep）
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
}

// ───────────────────────── kill 测试 ─────────────────────────

#[tokio::test]
async fn kill_unknown_actor_is_noop() {
    let system = ActorSystem::new();
    system.kill(&ActorId::from("ghost")).unwrap();
}

#[tokio::test]
async fn kill_removes_from_list() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("kill-list");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();
    assert_eq!(system.list_actors().len(), 1);
    system.kill(&actor_id).unwrap();
    assert_eq!(system.list_actors().len(), 0);
}

// ───────────────────────── compaction task 测试 ─────────────────────────

#[tokio::test]
async fn start_and_stop_compaction_task() {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let wal_path = dir.path().join("comp.wal");
    let wal_writer = WalWriter::open(&wal_path).unwrap();
    let system = ActorSystem::new().with_wal(wal_writer, store);

    // 启动 compaction task 不 panic
    system.start_compaction_task();
    // 短暂等待让 task 开始
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // 停止 compaction task 不 panic
    system.stop_compaction_task();
}

#[tokio::test]
async fn stop_compaction_task_without_start_is_noop() {
    let system = ActorSystem::new();
    // 没有启动过 compaction task，stop 应是 noop
    system.stop_compaction_task();
}

// ───────────────────────── MailboxRegistry 扩展测试 ─────────────────────────

#[tokio::test]
async fn mailbox_register_and_unregister() {
    let registry = MailboxRegistry::new();
    let actor_id = ActorId::from("mb-1");
    let (tx, mut rx) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx);

    // 发送消息
    let msg = ActorMessage::new(actor_id.clone(), "test".into(), vec![]);
    registry.send(&actor_id, msg).await.unwrap();
    let received = rx.recv().await.unwrap();
    assert_eq!(received.method, "test");

    // 注销后 send 应失败
    registry.unregister(&actor_id);
    let msg = ActorMessage::new(actor_id.clone(), "test".into(), vec![]);
    assert!(registry.send(&actor_id, msg).await.is_err());
}

#[tokio::test]
async fn mailbox_with_store_enables_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let registry = MailboxRegistry::new().with_store(Store::new(store));

    let actor_id = ActorId::from("persist-1");
    let (tx, _rx) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx);

    let msg = ActorMessage::new(actor_id.clone(), "persisted".into(), b"data".to_vec());
    registry.send(&actor_id, msg).await.unwrap();
    // 持久化模式下 send 应成功
}

// ───────────────────────── ActorPersistence 扩展测试 ─────────────────────────

#[tokio::test]
async fn persistence_without_wal_save_state_returns_empty() {
    let persistence = ActorPersistence::new();
    // 无 WAL 时持久化相关方法不应 panic
    let actor_id = ActorId::from("no-wal");
    // persist 在无 WAL 时应是 noop（不 panic）
    persistence
        .persist(actor_id.clone(), "test".to_string(), b"state".to_vec())
        .await;
}

#[tokio::test]
async fn persistence_load_latest_returns_none_without_checkpoint() {
    let persistence = ActorPersistence::new();
    let result = persistence.load_latest(ActorId::from("no-ckpt")).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn persistence_replay_after_returns_none_without_wal() {
    let persistence = ActorPersistence::new();
    let result = persistence.replay_after(ActorId::from("no-wal"), 0).await;
    assert!(result.is_none());
}

// ───────────────────────── 默认实现测试 ─────────────────────────

#[tokio::test]
async fn actor_default_save_state_returns_empty_vec() {
    struct DefaultActor;
    #[async_trait]
    impl Actor for DefaultActor {
        fn actor_type(&self) -> &str {
            "default"
        }
        async fn handle_message(&mut self, _msg: ActorMessage) -> Result<ActorMessageResult> {
            Ok(ActorMessageResult {
                message_id: MessageId::generate(),
                payload: vec![],
                error: None,
            })
        }
    }
    let actor = DefaultActor;
    assert_eq!(actor.save_state().unwrap(), Vec::<u8>::new());
    assert!(!actor.supports_state_persistence());
    let mut actor = actor;
    assert!(actor.load_state(&[]).is_ok());
    assert!(actor.on_start().await.is_ok());
    assert!(actor.on_stop().await.is_ok());
}

#[tokio::test]
async fn boxed_actor_delegates_to_inner() {
    let (actor, _) = EchoActor::new();
    let mut boxed: Box<dyn Actor> = Box::new(actor);
    assert_eq!(boxed.actor_type(), "echo");
    assert_eq!(boxed.save_state().unwrap(), Vec::<u8>::new());
    assert!(boxed.load_state(&[]).is_ok());
    assert!(!boxed.supports_state_persistence());
    assert!(boxed.on_start().await.is_ok());
    assert!(boxed.on_stop().await.is_ok());
}

// ───────────────────────── ActorContext 测试 ─────────────────────────

#[tokio::test]
async fn actor_context_transition_to_running_succeeds() {
    let mut ctx = ActorContext::new(ActorId::from("ctx-1"));
    assert_eq!(ctx.status, ActorStatus::Created);
    ctx.transition(ActorStatus::Running).unwrap();
    assert_eq!(ctx.status, ActorStatus::Running);
}

#[tokio::test]
async fn actor_context_transition_invalid_returns_error() {
    let mut ctx = ActorContext::new(ActorId::from("ctx-2"));
    // Created -> Stopped 是非法转换
    assert!(ctx.transition(ActorStatus::Stopped).is_err());
    // Created -> Failed 也是非法
    assert!(ctx.transition(ActorStatus::Failed).is_err());
    // 合法: Created -> Running
    ctx.transition(ActorStatus::Running).unwrap();
    // Running -> Created 非法
    assert!(ctx.transition(ActorStatus::Created).is_err());
}

// ───────────────────────── actor_status 反映 task 完成状态 ─────────────────────────

#[tokio::test]
async fn actor_status_returns_stopped_after_task_finishes() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("status-1");
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();
    assert_eq!(system.actor_status(&actor_id), Some(ActorStatus::Running));

    // stop 后状态应为 Stopped 或 None
    system.stop(&actor_id).await.unwrap();
    let status = system.actor_status(&actor_id);
    assert!(
        status.is_none() || status == Some(ActorStatus::Stopped),
        "expected None or Stopped, got {:?}",
        status
    );
}

// ───────────────────────── 并发 spawn 测试 ─────────────────────────

#[tokio::test]
async fn spawn_many_actors_concurrently() {
    let system = Arc::new(ActorSystem::new());
    let mut handles = Vec::new();
    for i in 0..20 {
        let s = system.clone();
        handles.push(tokio::spawn(async move {
            let (actor, _) = EchoActor::new();
            s.spawn(ActorId::from(format!("conc-{}", i)), actor)
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(system.list_actors().len(), 20);
}

// ───────────────────────── 默认 ActorConfig 测试 ─────────────────────────

#[test]
fn actor_config_default_has_sane_values() {
    let c = ActorConfig::default();
    assert!(c.mailbox_capacity > 0);
    assert!(c.wal_compaction_interval_secs > 0);
    assert!(c.checkpoint_retention_count >= 1);
    assert!(c.stop_timeout_ms > 0);
}

#[test]
fn actor_system_default_equals_new() {
    let a = ActorSystem::default();
    let b = ActorSystem::new();
    // 两者应具有相同的默认配置
    assert_eq!(a.config.mailbox_capacity, b.config.mailbox_capacity);
}

// ───────────────────────── on_start 失败的幽灵注册清理 ─────────────────────────

struct OnStartFailActor;

#[async_trait]
impl Actor for OnStartFailActor {
    fn actor_type(&self) -> &str {
        "onstart-fail"
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        Ok(ActorMessageResult {
            message_id: msg.id,
            payload: vec![],
            error: None,
        })
    }

    async fn on_start(&mut self) -> Result<()> {
        Err(ActantError::Actor("intentional on_start failure".into()))
    }
}

#[tokio::test]
async fn spawn_on_start_failure_cleans_up_ghost_registrations() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("onstart-fail-1");
    let err = system
        .spawn(actor_id.clone(), OnStartFailActor)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("on_start failed"));

    // 邮箱注册已清理：send 报 not found 而非入队到无人消费的通道。
    let msg = ActorMessage::new(actor_id.clone(), "ping".into(), vec![]);
    assert!(system.send(&actor_id, msg).await.is_err());
    assert!(system.list_actors().is_empty());

    // 清理后同 id 可重新 spawn 成功。
    let (actor, _) = EchoActor::new();
    system.spawn(actor_id.clone(), actor).await.unwrap();
    assert!(system.list_actors().contains(&actor_id));
}

// ───────────────────────── 消息失败不退役 actor（指标单一扣减的行为不变量） ─────────────────────────

#[tokio::test]
async fn message_failure_does_not_retire_actor() {
    let system = ActorSystem::new();
    let actor_id = ActorId::from("still-active-1");
    let (actor, _) = EchoActor::with_fail("boom");
    system.spawn(actor_id.clone(), actor).await.unwrap();

    // 消息失败后 actor 仍处于 Running（active_actors 不在消息级失败路径扣减，
    // 其对应行为是：actor 生命周期未被消息失败终止）。
    let result = system.call(&actor_id, "boom", vec![]).await.unwrap();
    assert!(result.error.is_some());
    assert_eq!(system.actor_status(&actor_id), Some(ActorStatus::Running));

    // 后续消息仍被正常处理。
    let ok = system
        .call(&actor_id, "ping", b"data".to_vec())
        .await
        .unwrap();
    assert_eq!(ok.payload, b"data");
}

// ───────────────────────── pending 消息 at-least-once 语义 ─────────────────────────

#[tokio::test]
async fn pending_message_persists_until_ack() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(LmdbStore::open(dir.path()).unwrap());
    let registry = MailboxRegistry::new().with_store(store.clone());

    let actor_id = ActorId::from("ack-persist-1");
    let (tx, mut rx) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx);

    let msg = ActorMessage::new(actor_id.clone(), "work".into(), b"payload".to_vec());
    let msg_id = msg.id.clone();
    registry.send(&actor_id, msg).await.unwrap();
    let _delivered = rx.recv().await.unwrap();

    // 入队成功不删除 pending 记录（由 ack 删除）。
    let prefix = format!("pending:{}:", actor_id.0);
    let entries = store.scan_prefix(&prefix).await.unwrap();
    assert_eq!(entries.len(), 1, "pending record must survive enqueue");

    registry.ack_message(&actor_id, &msg_id).await.unwrap();
    let entries = store.scan_prefix(&prefix).await.unwrap();
    assert_eq!(entries.len(), 0, "ack_message must delete pending record");
}

#[tokio::test]
async fn recover_pending_redelivers_unacked_messages() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(LmdbStore::open(dir.path()).unwrap());
    let registry = MailboxRegistry::new().with_store(store.clone());

    let actor_id = ActorId::from("recover-1");
    let (tx, _rx) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx);

    let msg = ActorMessage::new(actor_id.clone(), "work".into(), b"payload".to_vec());
    let msg_id = msg.id.clone();
    registry.send(&actor_id, msg).await.unwrap();

    // 模拟重启：注销旧邮箱，注册新通道后恢复未确认消息。
    registry.unregister(&actor_id);
    let (tx2, mut rx2) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx2);

    let count = registry.recover_pending(&actor_id).await.unwrap();
    assert_eq!(count, 1, "unacked message should be redelivered");

    let redelivered = rx2.recv().await.unwrap();
    assert_eq!(redelivered.id, msg_id);
    assert_eq!(redelivered.method, "work");

    // 重投不删除 pending 记录——仅 ack_message（成功处理后）删除。
    let prefix = format!("pending:{}:", actor_id.0);
    let entries = store.scan_prefix(&prefix).await.unwrap();
    assert_eq!(entries.len(), 1, "redelivery must keep pending record");

    registry.ack_message(&actor_id, &msg_id).await.unwrap();
    let entries = store.scan_prefix(&prefix).await.unwrap();
    assert_eq!(entries.len(), 0);
}

#[tokio::test]
async fn failed_message_redelivered_after_actor_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbStore::open(dir.path()).unwrap();
    let wal_path = dir.path().join("redeliver.wal");
    let wal_writer = WalWriter::open(&wal_path).unwrap();
    let system = ActorSystem::new().with_wal(wal_writer, store);

    let actor_id = ActorId::from("redeliver-1");
    let (actor, received1) = EchoActor::with_fail("boom");
    system.spawn(actor_id.clone(), actor).await.unwrap();

    system
        .send(
            &actor_id,
            ActorMessage::new(actor_id.clone(), "boom".into(), vec![]),
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(*received1.lock().unwrap(), vec!["boom".to_string()]);

    // 失败不 ack：重启后 recover_pending 重投该消息。
    system.stop(&actor_id).await.unwrap();
    let (actor2, received2) = EchoActor::with_fail("boom");
    system.spawn(actor_id.clone(), actor2).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *received2.lock().unwrap(),
        vec!["boom".to_string()],
        "unacked message must be redelivered after restart"
    );

    // 成功处理的消息被 ack，再次重启不重投。
    system
        .send(
            &actor_id,
            ActorMessage::new(actor_id.clone(), "ping".into(), b"ok".to_vec()),
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(*received2.lock().unwrap(), vec!["boom", "ping"]);

    system.stop(&actor_id).await.unwrap();
    let (actor3, received3) = EchoActor::new();
    system.spawn(actor_id.clone(), actor3).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    // "ping" 已 ack 不重投；"boom" 未 ack 会重投并再次失败。
    assert_eq!(
        *received3.lock().unwrap(),
        vec!["boom".to_string()],
        "acked message must not be redelivered"
    );
}

// ───────────────────────── 毒消息 bounded-redelivery 测试 ─────────────────────────

/// 捕获 tracing 输出到内存的 writer，用于断言毒消息判定发出 error 日志。
#[derive(Clone)]
struct PoisonLogWriter {
    buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl PoisonLogWriter {
    fn new() -> Self {
        Self {
            buf: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
    fn captured(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for PoisonLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for PoisonLogWriter {
    type Writer = PoisonLogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn poison_test_registry(dir: &std::path::Path) -> (MailboxRegistry, LmdbStore) {
    let store = LmdbStore::open(dir).unwrap();
    let registry = MailboxRegistry::new().with_store(Store::new(store.clone()));
    (registry, store)
}

#[tokio::test]
async fn recover_pending_increments_delivery_count() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, store) = poison_test_registry(dir.path());
    let actor_id = ActorId::from("redeliver-1");
    let (tx, mut rx) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx);

    let msg = ActorMessage::new(actor_id.clone(), "job".into(), b"p".to_vec());
    let key = crate::runtime::actor::mailbox::pending_key(&actor_id, &msg.id);
    registry.send(&actor_id, msg).await.unwrap();

    let read_count = |store: &LmdbStore, key: &str| -> u32 {
        let raw = store
            .get(key)
            .unwrap()
            .expect("pending record should exist");
        postcard::from_bytes::<crate::runtime::actor::mailbox::PersistentMessage>(&raw)
            .unwrap()
            .delivery_count()
    };

    // 首次投递 delivery_count = 0
    assert_eq!(read_count(&store, &key), 0);

    // 每次 recover_pending 重投成功后计数递增并回写
    registry.recover_pending(&actor_id).await.unwrap();
    assert_eq!(
        read_count(&store, &key),
        1,
        "delivery count should increment on first redelivery"
    );
    registry.recover_pending(&actor_id).await.unwrap();
    assert_eq!(
        read_count(&store, &key),
        2,
        "delivery count should increment on second redelivery"
    );

    // 消息确实被重投进邮箱
    let redelivered = rx.recv().await.unwrap();
    assert_eq!(redelivered.method, "job");
}

#[tokio::test]
async fn poison_pending_message_dropped_and_logged_after_max_redeliveries() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, store) = poison_test_registry(dir.path());
    let actor_id = ActorId::from("poison-1");
    let (tx, mut rx) = mpsc::channel(32);
    registry.register(actor_id.clone(), tx);

    let msg = ActorMessage::new(actor_id.clone(), "doom".into(), b"p".to_vec());
    let key = crate::runtime::actor::mailbox::pending_key(&actor_id, &msg.id);
    registry.send(&actor_id, msg).await.unwrap();

    let writer = PoisonLogWriter::new();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer.clone())
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    let dispatch = tracing::dispatcher::Dispatch::new(subscriber);

    // recover_pending 不依赖 tokio 运行时设施（LMDB 同步 + try_send），
    // 可在 dispatcher 作用域内用 executor 驱动。
    tracing::dispatcher::with_default(&dispatch, || {
        // MAX_PENDING_REDELIVERIES = 5：前 5 次 recover 正常重投，第 6 次超限丢弃。
        for round in 1..=5 {
            let recovered =
                futures::executor::block_on(registry.recover_pending(&actor_id)).unwrap();
            assert_eq!(recovered, 1, "round {round}: message should be redelivered");
            assert!(
                store.get(&key).unwrap().is_some(),
                "round {round}: record must be kept before exceeding the limit"
            );
        }

        let recovered = futures::executor::block_on(registry.recover_pending(&actor_id)).unwrap();
        assert_eq!(recovered, 0, "poison message must not be redelivered");
    });

    // 记录已被删除，不会随下一次 spawn 再度重投。
    assert!(
        store.get(&key).unwrap().is_none(),
        "poison message record must be deleted after exceeding max redeliveries"
    );
    // 邮箱内应存在此前轮次重投的消息（未被消费），但不含第 6 条。
    assert!(
        rx.try_recv().is_ok(),
        "earlier redeliveries should remain in the mailbox"
    );

    let captured = writer.captured();
    assert!(
        captured.contains("ERROR") && captured.contains("poison message"),
        "dropping a poison message must log an error, got: {captured}"
    );
    assert!(
        captured.contains(&actor_id.0) && captured.contains(&msg_id_str(&key)),
        "error log must carry actor_id and msg_id, got: {captured}"
    );
}

/// 从 pending key 提取 msg_id（key 形如 `pending:{actor}:{msg}`）。
fn msg_id_str(key: &str) -> String {
    key.rsplit(':').next().unwrap_or("").to_string()
}

#[tokio::test]
async fn ack_message_deletes_pending_record_resetting_redelivery_state() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, store) = poison_test_registry(dir.path());
    let actor_id = ActorId::from("ack-1");
    let (tx, mut rx) = mpsc::channel(8);
    registry.register(actor_id.clone(), tx);

    let msg = ActorMessage::new(actor_id.clone(), "job".into(), b"p".to_vec());
    let key = crate::runtime::actor::mailbox::pending_key(&actor_id, &msg.id);
    registry.send(&actor_id, msg).await.unwrap();
    assert!(store.get(&key).unwrap().is_some());

    // 成功处理后的 ack：删除 pending 记录（重投计数随之归零消失）。
    let delivered = rx.recv().await.unwrap();
    registry
        .ack_message(&actor_id, &delivered.id)
        .await
        .unwrap();
    assert!(
        store.get(&key).unwrap().is_none(),
        "acked message must have its pending record deleted"
    );

    // ack 后 recover_pending 无可重投消息。
    let recovered = registry.recover_pending(&actor_id).await.unwrap();
    assert_eq!(recovered, 0, "acked message must not be redelivered");
}
