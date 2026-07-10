//! Capability 分发基准测试。
//!
//! 测量 ERH 架构的核心分发路径：
//! - `perform` 延迟（含编解码 + Actor 消息往返）
//! - `ask` 延迟（含编解码 + Actor 消息往返）
//! - `emit` 吞吐量
//!
//! 这些路径是用户层 `ask`/`perform`/`emit` effect 的底层实现，
//! 其延迟直接决定任务编排的整体响应时间。
//!
//! 运行：`cargo bench --bench capability_dispatch`

use std::sync::Arc;

use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use actant::runtime::actor::ActorSystem;
use actant::runtime::capability::{
    Capability, CapabilityMeta, CapabilityRuntime, EffectKind, Handler, Layer,
};

// ── 测试用 Capability 定义 ──────────────────────────────────────────

/// Echo capability：perform 返回原始 payload，ask 返回 payload 长度。
struct EchoCap;

impl Capability for EchoCap {
    type Request = EchoReq;
    type Response = EchoResp;
}

impl EchoCap {
    fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("EchoBench", EffectKind::Perform)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoReq {
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoResp {
    payload: Vec<u8>,
}

/// Echo handler：perform 和 ask 都返回原始 payload。
struct EchoHandler;

#[async_trait]
impl Handler<EchoCap> for EchoHandler {
    async fn handle(&self, req: EchoReq) -> Option<EchoResp> {
        Some(EchoResp {
            payload: req.payload,
        })
    }
}

/// Emit-only capability：用于 emit 吞吐测试。
struct EmitCap;

impl Capability for EmitCap {
    type Request = EmitReq;
    type Response = ();
}

impl EmitCap {
    fn meta() -> CapabilityMeta {
        CapabilityMeta::new::<Self>("EmitBench", EffectKind::Emit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmitReq {
    event: String,
    data: Vec<u8>,
}

struct EmitHandler;

#[async_trait]
impl Handler<EmitCap> for EmitHandler {
    async fn handle(&self, _req: EmitReq) -> Option<()> {
        None
    }
}

// ── 设置 ──────────────────────────────────────────────────────────

/// 创建绑定到 ActorSystem 的 CapabilityRuntime，注册 EchoCap handler。
fn setup_echo_runtime(rt: &Runtime) -> Arc<CapabilityRuntime> {
    let cap_rt = Arc::new(CapabilityRuntime::new());
    cap_rt.register_codec::<EchoCap>();
    let layer = Layer::<EchoCap>::new(EchoCap::meta()).chain(EchoHandler);
    cap_rt.register(layer).unwrap();

    let actor_system = Arc::new(ActorSystem::new());
    rt.block_on(async {
        cap_rt.clone().bind_actor_system(actor_system).await;
    });
    cap_rt
}

/// 创建绑定到 ActorSystem 的 CapabilityRuntime，注册 EmitCap handler。
fn setup_emit_runtime(rt: &Runtime) -> Arc<CapabilityRuntime> {
    let cap_rt = Arc::new(CapabilityRuntime::new());
    cap_rt.register_codec::<EmitCap>();
    let layer = Layer::<EmitCap>::new(EmitCap::meta()).chain(EmitHandler);
    cap_rt.register(layer).unwrap();

    let actor_system = Arc::new(ActorSystem::new());
    rt.block_on(async {
        cap_rt.clone().bind_actor_system(actor_system).await;
    });
    cap_rt
}

// ── 基准测试 ──────────────────────────────────────────────────────

/// `perform` 延迟：编解码 + Actor 消息往返。
fn bench_perform_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cap_rt = setup_echo_runtime(&rt);
    let req = EchoReq {
        payload: vec![0u8; 64],
    };

    let mut group = c.benchmark_group("capability/perform_latency");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let resp = cap_rt.perform::<EchoCap>(req.clone()).await;
                        match resp {
                            Ok(r) => black_box(r),
                            Err(e) => panic!("perform failed: {:?}", e),
                        };
                    }
                });
            });
        });
    }
    group.finish();
}

/// `ask` 延迟：编解码 + Actor 消息往返 + handler 返回值。
fn bench_ask_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cap_rt = setup_echo_runtime(&rt);
    let req = EchoReq {
        payload: vec![0u8; 64],
    };

    let mut group = c.benchmark_group("capability/ask_latency");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let resp = cap_rt.ask::<EchoCap>(req.clone()).await;
                        match resp {
                            Ok(r) => black_box(r),
                            Err(e) => panic!("ask failed: {:?}", e),
                        };
                    }
                });
            });
        });
    }
    group.finish();
}

/// `emit` 吞吐量：编解码 + Actor 投递（无回复）。
fn bench_emit_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cap_rt = setup_emit_runtime(&rt);
    let req = EmitReq {
        event: "bench".into(),
        data: vec![0u8; 64],
    };

    let mut group = c.benchmark_group("capability/emit_throughput");
    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..n {
                        let _ = cap_rt.emit::<EmitCap>(req.clone()).await;
                    }
                });
            });
        });
    }
    group.finish();
}

/// 并发 `perform` 吞吐量（模拟多任务并行调度场景）。
fn bench_concurrent_perform(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cap_rt = setup_echo_runtime(&rt);
    let req = EchoReq {
        payload: vec![0u8; 64],
    };

    let mut group = c.benchmark_group("capability/concurrent_perform");
    for &concurrency in &[16usize, 64, 128] {
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &conc| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(conc);
                        for _ in 0..conc {
                            let cap = cap_rt.clone();
                            let req = req.clone();
                            handles.push(tokio::spawn(async move {
                                let _ = cap.perform::<EchoCap>(req).await;
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    capability_benches,
    bench_perform_latency,
    bench_ask_latency,
    bench_emit_throughput,
    bench_concurrent_perform,
);
criterion_main!(capability_benches);
