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

// ───────────────────────── MailboxRegistry 测试 ─────────────────────────

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

// ───────────────────────── 默认实现测试 ─────────────────────────

#[tokio::test]
async fn actor_default_lifecycle_hooks_are_noop() {
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
    let mut actor = DefaultActor;
    assert!(actor.on_start().await.is_ok());
    assert!(actor.on_stop().await.is_ok());
}

#[tokio::test]
async fn boxed_actor_delegates_to_inner() {
    let (actor, _) = EchoActor::new();
    let mut boxed: Box<dyn Actor> = Box::new(actor);
    assert_eq!(boxed.actor_type(), "echo");
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
