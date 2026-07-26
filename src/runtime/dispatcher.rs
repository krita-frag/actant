//! 本模块负责将任务 payload 分发给注册的 handler 并管理执行线程池。
//! 与工作流编排无直接耦合，属于通用执行基础设施。

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dashmap::DashMap;

use crate::common::ActantError;

/// 取消标志，用于协调任务分发器和任务处理程序之间的取消操作。
///
/// 分发器为每次分发创建一个 `Arc<AtomicBool>`，并将其克隆传递给处理程序。
/// 超时时，分发器将 `true` 存入该标志；处理程序（或通过 `PyCancelToken` 的 Python）
/// 轮询该标志以协作退出长时间运行的操作。
pub type CancelFlag = Arc<AtomicBool>;

/// 创建一个新鲜的取消标志（初始值为 `false`）。
pub fn new_cancel_flag() -> CancelFlag {
    Arc::new(AtomicBool::new(false))
}

pub type TaskHandler =
    Arc<dyn Fn(Vec<u8>, CancelFlag) -> crate::common::Result<Vec<u8>> + Send + Sync + 'static>;

/// 有界线程池，用于任务执行。
///
/// 使用固定数量的工作线程从共享队列中拉取任务。
/// 这样可以避免无限制的线程创建（每个任务一个线程），同时将任务执行与 Tokio 的运行时线程隔离。
struct TaskThreadPool {
    sender: crossbeam_channel::Sender<Job>,
    /// 工作线程句柄，用于 shutdown 时 join。
    workers: Vec<std::thread::JoinHandle<()>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

impl TaskThreadPool {
    fn new(worker_count: usize, channel_capacity: usize) -> Result<Self, ActantError> {
        if worker_count == 0 {
            return Err(ActantError::Config(
                "task thread pool worker_count must be > 0".into(),
            ));
        }
        let (sender, receiver) = crossbeam_channel::bounded::<Job>(channel_capacity);
        let mut workers = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let rx = receiver.clone();
            let handle = std::thread::Builder::new()
                .name(format!("actant-task-worker-{i}"))
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        job();
                    }
                })
                .map_err(|e| {
                    ActantError::Worker(format!("failed to spawn task worker thread: {e}"))
                })?;
            workers.push(handle);
        }
        // 注：工作线程非 daemon。shutdown_and_wait 通过关闭 channel 让
        // 空闲线程退出并 join；正在执行不可中断任务的线程会阻塞 join
        // 直到任务完成（由 drain_timeout_secs 限制总时长）。
        Ok(Self { sender, workers })
    }

    /// 关闭线程池并等待所有工作线程退出。
    ///
    /// 通过替换 sender 为一个无关的、已关闭的 channel，使原 channel 的所有 sender
    /// 都被 drop，工作线程的 `rx.recv()` 返回 `Err` 并退出。
    /// 不会中断正在执行的任务。
    ///
    /// `timeout` 限制总等待时长。超时后尚未退出的工作线程被放弃 join（线程为
    /// 非 daemon，进程退出时仍会等待，但调用方不会被无限阻塞）。
    fn shutdown_and_wait(&mut self, timeout: std::time::Duration) {
        // 替换 sender 为无关的新 channel，原 sender 被 drop。
        // mem::replace 总是返回旧值，此处明确丢弃旧 sender 以触发 worker
        // 退出路径（drop sender → channel 关闭 → recv 返回 Err）。
        let (dummy_tx, _) = crossbeam_channel::bounded::<Job>(0);
        let _ = std::mem::replace(&mut self.sender, dummy_tx);
        let workers = std::mem::take(&mut self.workers);
        if workers.is_empty() {
            return;
        }
        // 在独立线程中 join 所有 worker，主线程通过 channel 超时等待。
        // 在 workers 被 move 进 spawn 闭包前捕获数量，供超时日志使用。
        let worker_count = workers.len();
        let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);
        // spawn 失败仅发生在 OS 资源耗尽（OOM/thread limit），此时 worker
        // 线程仍会自行退出（sender 已 drop）；done_tx 不会发送，主线程将
        // 走 timeout 路径。失败信息由 OS 通过其他渠道报错。
        let _ = std::thread::Builder::new()
            .name("actant-task-pool-shutdown".into())
            .spawn(move || {
                for handle in workers {
                    if let Err(e) = handle.join() {
                        tracing::warn!("task worker thread join failed: {:?}", e);
                    }
                }
                // done_tx.send 失败仅当 done_rx 被 drop（主线程已 timeout 退出），
                // 属正常竞争路径，无需处理。
                let _ = done_tx.send(());
            });
        match done_rx.recv_timeout(timeout) {
            Ok(()) => {}
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                tracing::warn!(
                    "task thread pool shutdown timed out after {:?}; \
                     abandoning {} worker thread(s) still running",
                    timeout,
                    worker_count,
                );
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                // join 线程已退出（所有 worker 均已 join 完成）
            }
        }
    }

    fn submit<F>(&self, f: F) -> Result<(), ActantError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.try_send(Box::new(f)).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(_) => {
                ActantError::Internal("task thread pool is at capacity".into())
            }
            crossbeam_channel::TrySendError::Disconnected(_) => {
                ActantError::Internal("task thread pool is shut down".into())
            }
        })
    }
}

/// 任务分发器抽象。
///
/// 此 trait 将 worker 运行时与具体执行后端解耦。默认实现 [`TaskRegistry`]
/// 使用固定大小线程池 + `DashMap` handler 注册表，支持协作式取消。
///
/// # 公共扩展点
///
/// 此 trait 是 Rust 核心的公共扩展点。外部 Rust 用户可实现此 trait 以替换执行后端
/// （例如进程池、GPU 任务队列、远程 RPC 调用等）。实现只需满足 `Send + Sync`。
///
/// `cancel_flag` 由调用方创建并传入；实现应在超时或取消时将其置为 `true`，
/// 处理程序通过轮询该标志协作退出长时间运行的操作。
#[async_trait::async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// 将任务分发给其处理程序。
    ///
    /// `cancel_flag` 是分发器和处理程序之间的共享标志。
    /// 超时时，分发器将其设置为 `true`；处理程序应轮询它以协作取消长时间运行的工作。
    async fn dispatch(
        &self,
        name: &str,
        payload: Vec<u8>,
        cancel_flag: CancelFlag,
    ) -> crate::common::Result<Vec<u8>>;

    /// 注册一个任务处理程序。
    ///
    /// 默认实现返回错误，表示此 dispatcher 不支持动态注册。
    /// `TaskRegistry` 覆盖此方法以支持运行时注册。
    fn register_handler(&self, _name: &str, _handler: TaskHandler) -> crate::common::Result<()> {
        Err(ActantError::Internal(
            "this dispatcher does not support register_handler".into(),
        ))
    }

    /// 关闭 dispatcher 并等待所有执行中的任务完成。
    ///
    /// 默认实现为空操作；`TaskRegistry` 覆盖以关闭线程池。
    /// 调用方应在 Worker drain 阶段调用此方法，确保在 Runtime 关闭前
    /// 所有任务线程已退出，避免访问已释放的资源。
    fn shutdown(&self) {}
}

pub struct TaskRegistry {
    handlers: DashMap<String, TaskHandler>,
    pool: parking_lot::Mutex<TaskThreadPool>,
    /// Payload 签名密钥。非空时 dispatch 会验证 payload MAC。
    payload_signing_key: Vec<u8>,
    /// shutdown 时等待工作线程退出的总超时。
    drain_timeout: std::time::Duration,
}

impl TaskRegistry {
    pub fn new(
        pool_workers: usize,
        pool_channel_capacity: usize,
        payload_signing_key: Vec<u8>,
    ) -> crate::common::Result<Self> {
        Self::with_drain_timeout(
            pool_workers,
            pool_channel_capacity,
            payload_signing_key,
            std::time::Duration::from_secs(30),
        )
    }

    /// 创建 `TaskRegistry` 并指定 shutdown 超时。
    ///
    /// `drain_timeout` 限制 `shutdown` 时等待工作线程退出的总时长，超时后
    /// 放弃 join 尚未退出的线程。默认 30s（对应 `WorkerConfig::drain_timeout_secs`）。
    pub fn with_drain_timeout(
        pool_workers: usize,
        pool_channel_capacity: usize,
        payload_signing_key: Vec<u8>,
        drain_timeout: std::time::Duration,
    ) -> crate::common::Result<Self> {
        Ok(Self {
            handlers: DashMap::new(),
            pool: parking_lot::Mutex::new(TaskThreadPool::new(
                pool_workers,
                pool_channel_capacity,
            )?),
            payload_signing_key,
            drain_timeout,
        })
    }

    pub fn register<F>(&self, name: &str, handler: F)
    where
        F: Fn(Vec<u8>, CancelFlag) -> crate::common::Result<Vec<u8>> + Send + Sync + 'static,
    {
        self.handlers.insert(name.to_string(), Arc::new(handler));
    }

    pub fn into_dispatcher(self) -> Arc<dyn TaskDispatcher> {
        Arc::new(self)
    }
}

/// 通用计算节点 fallback handler 名。
///
/// 当任务名未在 `TaskRegistry::handlers` 中注册时,`dispatch` 会回退到此名。
/// Python 端 worker 启动时总是注册此 handler,用于反序列化内联 callable payload,
/// 让无业务模块依赖的 worker 也能执行任意 cloudpickle 任务。
pub const GENERIC_DISPATCH_NAME: &str = "__actant_generic__";

#[async_trait::async_trait]
impl TaskDispatcher for TaskRegistry {
    async fn dispatch(
        &self,
        name: &str,
        payload: Vec<u8>,
        cancel_flag: CancelFlag,
    ) -> crate::common::Result<Vec<u8>> {
        let handler = {
            // 先按 name 精确查找(快路径:worker 预加载业务模块的场景)。
            // 未命中则回退到 __actant_generic__ handler,执行内联 payload 中的 callable。
            // 避免对 "__actant_generic__" 本身递归回退造成无限循环。
            let h = if name == GENERIC_DISPATCH_NAME {
                None
            } else {
                self.handlers.get(name)
            };
            let h = match h {
                Some(h) => h,
                None => self.handlers.get(GENERIC_DISPATCH_NAME).ok_or_else(|| {
                    ActantError::Internal(format!(
                        "no handler registered for task '{}' and no generic fallback available",
                        name
                    ))
                })?,
            };
            h.clone()
        };
        tracing::debug!(%name, "TaskRegistry::dispatch");

        // 验证 payload MAC
        let verified_payload = crate::common::payload::verify(&self.payload_signing_key, &payload)
            .map_err(|e| ActantError::Internal(format!("payload verification: {}", e)))?;

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.pool.lock().submit(move || {
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (handler)(verified_payload, cancel_flag)
            }));
            match res {
                Ok(Ok(value)) => {
                    if tx.send(Ok(value)).is_err() {
                        tracing::warn!("dispatcher: result channel closed before sending Ok");
                    }
                }
                Ok(Err(e)) => {
                    if tx.send(Err(e)).is_err() {
                        tracing::warn!("dispatcher: result channel closed before sending Err");
                    }
                }
                Err(panic_payload) => {
                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    if tx
                        .send(Err(ActantError::Internal(format!(
                            "task handler panicked: {}",
                            msg
                        ))))
                        .is_err()
                    {
                        tracing::warn!("dispatcher: result channel closed before sending panic");
                    }
                }
            }
        })?;

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ActantError::Internal("task handler panicked".into())),
        }
    }

    fn register_handler(&self, name: &str, handler: TaskHandler) -> crate::common::Result<()> {
        self.handlers.insert(name.to_string(), handler);
        Ok(())
    }

    fn shutdown(&self) {
        self.pool.lock().shutdown_and_wait(self.drain_timeout);
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/dispatcher.rs"]
mod tests;
