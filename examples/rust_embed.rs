//! 纯 Rust 嵌入示例（X2）：**框架化验收实验**。
//!
//! 目标：在不依赖 PyO3 / Python 解释器的前提下，用 Actant 核心 API 跑通
//! 「提交 → DAG 依赖 → 任务执行 → 结果聚合」全链路：
//!
//! 1. 自定义 [`TaskDispatcher`]（编译期注册的 Rust 闭包，进程内执行，µs 级
//!    延迟路径——不经 worker 子进程 IPC）经 `RuntimeBuilder::with_task_dispatcher`
//!    注入；
//! 2. 构造线性 DAG（a → b → c），提交给 orchestrator；
//! 3. 自定义 dispatcher 的执行体根据任务名返回结果字节（模拟任意语言的
//!    worker 程序——stdio 帧协议的另一端可以是任何实现）；
//! 4. 轮询 `get_results` 直到 DAG 全部完成，断言三个任务的执行顺序满足依赖。
//!
//! 运行：`cargo run --no-default-features --example rust_embed`
//! （本示例不触碰 py 模块，在默认 feature 下同样可编译运行；但验收门禁以
//! no-default-features 构建为准——那是"核心不依赖 PyO3"的证明）。
//!
//! 契约文档见 `docs/FRAMEWORK.md`。

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actant::common::{sign, verify, ActantConfig, TaskId, WorkflowId};
use actant::runtime::builder::RuntimeBuilder;
use actant::runtime::dispatcher::TaskDispatcher;
use actant::runtime::workflow::{Dag, DagNode};

/// 轮询 worker 就绪标志（Python 绑定层 serve() 的就绪等待等价物）。
fn worker_ready(runtime: &actant::runtime::Runtime) -> bool {
    runtime.worker().map(|w| w.is_ready()).unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 便携诊断：ACTANT_TRACING=1 时安装 subscriber（嵌入方自选观测栈，核心不强装）。
    actant::observability::init();

    let signing_key = b"rust-embed-demo-key".to_vec();

    // ── 1. 自定义任务分发器：进程内 Rust 闭包执行 ─────────────────────
    //
    // 这是「换执行语言」扩展缝的 Rust 侧形态：dispatch(name, payload, ...)
    // 的 name 是任务名，payload 是不透明字节。这里按任务名执行编译期逻辑；
    // 生产中同样的缝可以接 shell / Lua / 任何语言的 worker（实现同一
    // stdio 帧协议）。
    #[derive(Clone)]
    struct ClosureDispatcher {
        signing_key: Vec<u8>,
        /// 记录实际执行顺序，供依赖断言。
        executed: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl TaskDispatcher for ClosureDispatcher {
        async fn dispatch(
            &self,
            name: &str,
            payload_bytes: Vec<u8>,
            _cancel_flag: actant::runtime::dispatcher::CancelFlag,
            _timeout: Duration,
        ) -> actant::common::Result<Vec<u8>> {
            // 核心提交路径对 payload 签名；进程内执行同样先验签——
            // 完整走一遍「不透明字节」契约。
            let body = verify(&self.signing_key, &payload_bytes)
                .map_err(actant::common::ActantError::Internal)?;

            self.executed.lock().unwrap().push(name.to_string());

            // 任务体：把输入字节翻转为「<name>:<len>」的结果帧。
            Ok(format!("{name}:ok({} bytes)", body.len()).into_bytes())
        }
    }

    let executed = Arc::new(Mutex::new(Vec::new()));
    let dispatcher: Arc<dyn TaskDispatcher> = Arc::new(ClosureDispatcher {
        signing_key: signing_key.clone(),
        executed: Arc::clone(&executed),
    });

    // ── 2. 组装 Runtime（无 data_dir：纯内存；discovery none：单机） ──
    let mut config = ActantConfig::default();
    config.network.discovery_mode = actant::common::DiscoveryMode::new_unchecked("none");
    config.payload_signing_key = signing_key.clone();

    let dir = tempfile::tempdir()?;
    let runtime = RuntimeBuilder::new("rust-embed-node".into(), config)
        .with_data_dir(dir.path().to_str().unwrap().to_string())
        .with_task_dispatcher(dispatcher)
        .build()
        .await?;

    // ── 3. 构造并提交线性 DAG：a → b → c ─────────────────────────────
    let workflow_id = WorkflowId::from("rust-embed-demo-wf".to_string());
    let mut dag = Dag::new();
    for (id, name) in [("a", "step-a"), ("b", "step-b"), ("c", "step-c")] {
        dag.add_node(DagNode {
            task_id: TaskId::from(id.to_string()),
            name: name.to_string(),
            // 节点载荷同样要按提交契约签名（orchestrator 派发前不再签节点载荷，
            // 签名发生在任务定义构造处——与 Python 层提交路径一致）。
            payload: sign(&signing_key, format!("payload-{id}").as_bytes())?,
            retry_policy: None,
            timeout_ms: None,
            priority: 0,
            metadata: Default::default(),
        })?;
    }
    dag.add_edge(TaskId::from("a".to_string()), TaskId::from("b".to_string()))?;
    dag.add_edge(TaskId::from("b".to_string()), TaskId::from("c".to_string()))?;

    // 提交 + 启动 + 派发：Rust 嵌入者的完整装配序列——
    //   ① orchestrator.submit：登记 DAG（Pending）；
    //   ② orchestrator.start：置 Running 并返回 roots 的 TaskDefinition；
    //   ③ scheduler.enqueue_batch：roots 进调度器，Worker 主循环派发给
    //      注入的 dispatcher；后继任务在前驱完成回调中自动就绪并入队。
    // （Python 绑定层走 add_workflow_node 增量路径，职责等价。）
    let orchestrator = runtime
        .orchestrator_handle()
        .expect("orchestrator must be set by builder")
        .clone();
    let scheduler = runtime
        .worker()
        .expect("worker must be set by builder")
        .scheduler_clone();
    // worker.run() 即 Rust 侧的 serve（Python 绑定层在 serve() 里做同样的事）：
    // spawn 守护循环，驱动任务执行 / 结果回灌 / 网络事件路由。**必须先于
    // enqueue 启动**——TaskEnqueued Notify 无队列，先 enqueue 后 run 会错过
    // 唤醒信号；结果回灌泵同样由 run() 驱动。
    let worker = runtime
        .worker()
        .expect("worker must be set by builder")
        .clone();
    let worker_task = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            tracing::error!(error = %e, "worker daemon exited with error");
        }
    });
    // 就绪门：run() 进入执行循环前置 ready 标志，避免与 prefetch 的竞态。
    while !worker_ready(&runtime) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    orchestrator.submit(workflow_id.clone(), dag).await?;
    let roots = orchestrator.start(&workflow_id)?;
    scheduler.enqueue_batch(roots).await?;

    // ── 4. 等待 DAG 完成，断言依赖顺序 ────────────────────────────────
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let results = loop {
        if let Some(results) = orchestrator.get_results(&workflow_id).await {
            break results;
        }
        if std::time::Instant::now() > deadline {
            panic!("workflow did not complete within 30s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    let order = executed.lock().unwrap().clone();
    println!("executed order: {order:?}");
    println!("results: {} tasks", results.len());
    for (i, r) in results.iter().enumerate() {
        println!("  result[{i}] = {}", String::from_utf8_lossy(r));
    }

    assert_eq!(
        order,
        vec!["step-a", "step-b", "step-c"],
        "DAG 依赖顺序必须被遵守"
    );
    assert_eq!(results.len(), 3);

    worker_task.abort();
    runtime.shutdown().await?;
    println!("X2 验收通过：纯 Rust 引擎跑通 提交 → DAG → 执行 → 聚合 全链路");
    Ok(())
}
