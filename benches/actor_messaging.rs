//! Actor 消息传递基准测试。
//!
//! 测量 Actor 系统的核心消息路径：
//! - `send`（fire-and-forget）吞吐量
//! - `call`（request-response）延迟
//! - 并发 `call` 吞吐量
//!
//! 这些是所有 capability dispatch、workflow 调度的底层基础路径。
//!
//! 运行：`cargo bench --bench actor_messaging`

use std::sync::Arc;

use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

use actant::common::{ActorId, ActorMessage, ActorMessageResult, ActantError};
use actant::runtime::actor::{Actor, ActorSystem};

/// Echo actor：将收到的 payload 原样返回。
struct EchoActor;

#[async_trait]
impl Actor for EchoActor {
    fn actor_type(&self) -> &str {
        "echo"
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult, ActantError> {
        Ok(ActorMessageResult {
            message_id: msg.id,
            payload: msg.payload,
            error: None,
        })
    }
}

/// Noop actor：不返回 payload，用于 fire-and-forget 吞吐测试。
struct NoopActor;

#[async_trait]
impl Actor for NoopActor {
    fn actor_type(&self) -> &str {
        "noop"
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult, ActantError> {
        Ok(ActorMessageResult {
            message_id: msg.id,
            payload: vec![],
            error: None,
        })
    }
}

/// 启动 EchoActor 并返回 (ActorSystem, ActorId)。
fn setup_echo(rt: &Runtime) -> (Arc<ActorSystem>, ActorId) {
    let system = Arc::new(ActorSystem::new());
    let id = ActorId::new("echo-bench".to_string());
    rt.block_on(system.spawn(id.clone(), EchoActor)).unwrap();
    (system, id)
}

/// 启动 NoopActor 并返回 (ActorSystem, ActorId)。
fn setup_noop(rt: &Runtime) -> (Arc<ActorSystem>, ActorId) {
    let system = Arc::new(ActorSystem::new());
    let id = ActorId::new("noop-bench".to_string());
    rt.block_on(system.spawn(id.clone(), NoopActor)).unwrap();
    (system, id)
}

/// `call`（request-response）延迟。
fn bench_call_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (system, actor_id) = setup_echo(&rt);
    let payload = vec![0u8; 64];

    let mut group = c.benchmark_group("actor/call_latency");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let result = system.call(&actor_id, "echo", payload.clone()).await;
                        black_box(result);
                    }
                });
            });
        });
    }
    group.finish();
}

/// `send`（fire-and-forget）吞吐量。
/// 使用 oneshot channel 模拟无回复消息。
fn bench_send_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (system, actor_id) = setup_noop(&rt);
    let payload = vec![0u8; 64];

    let mut group = c.benchmark_group("actor/send_throughput");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let msg = ActorMessage::new(actor_id.clone(), "noop".into(), payload.clone());
                        system.send(&actor_id, msg).await;
                    }
                });
            });
        });
    }
    group.finish();
}

/// 并发 `call` 吞吐量（模拟多任务并行调度场景）。
fn bench_concurrent_call(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (system, actor_id) = setup_echo(&rt);
    let payload = vec![0u8; 64];

    let mut group = c.benchmark_group("actor/concurrent_call");
    for &concurrency in &[16usize, 64, 128] {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &conc| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(conc);
                        for _ in 0..conc {
                            let system = system.clone();
                            let id = actor_id.clone();
                            let payload = payload.clone();
                            handles.push(tokio::spawn(async move {
                                system.call(&id, "echo", payload).await;
                            }));
                        }
                        for h in handles {
                            h.await;
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    actor_messaging_benches,
    bench_call_latency,
    bench_send_throughput,
    bench_concurrent_call,
);
criterion_main!(actor_messaging_benches);
