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
        for i in 0..worker_count {
            let rx = receiver.clone();
            std::thread::Builder::new()
                .name(format!("actant-task-worker-{i}"))
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        job();
                    }
                })
                .map_err(|e| {
                    ActantError::Worker(format!("failed to spawn task worker thread: {e}"))
                })?;
        }
        Ok(Self { sender })
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
}

pub struct TaskRegistry {
    handlers: DashMap<String, TaskHandler>,
    pool: TaskThreadPool,
    /// Payload 签名密钥。非空时 dispatch 会验证 payload MAC。
    payload_signing_key: Vec<u8>,
}

impl TaskRegistry {
    pub fn new(
        pool_workers: usize,
        pool_channel_capacity: usize,
        payload_signing_key: Vec<u8>,
    ) -> crate::common::Result<Self> {
        Ok(Self {
            handlers: DashMap::new(),
            pool: TaskThreadPool::new(pool_workers, pool_channel_capacity)?,
            payload_signing_key,
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

        self.pool.submit(move || {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: &[u8] = b"test-key";

    fn signed(payload: &[u8]) -> Vec<u8> {
        crate::common::payload::sign(TEST_KEY, payload).unwrap()
    }

    #[tokio::test]
    async fn dispatch_executes_registered_handler() {
        let registry = TaskRegistry::new(2, 16, TEST_KEY.to_vec()).unwrap();
        registry.register("echo", |_payload, _flag| Ok(b"echo-response".to_vec()));

        let result = registry
            .dispatch("echo", signed(b"input"), new_cancel_flag())
            .await
            .unwrap();
        assert_eq!(result, b"echo-response");
    }

    #[tokio::test]
    async fn dispatch_returns_error_for_unknown_handler_without_generic() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        let err = registry
            .dispatch("nonexistent", signed(b""), new_cancel_flag())
            .await
            .unwrap_err();
        assert!(matches!(err, ActantError::Internal(_)));
    }

    #[tokio::test]
    async fn dispatch_falls_back_to_generic_handler() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        registry.register(GENERIC_DISPATCH_NAME, |payload, _flag| {
            Ok([b"generic:", &payload[..]].concat())
        });

        let result = registry
            .dispatch("custom-task", signed(b"data"), new_cancel_flag())
            .await
            .unwrap();
        assert_eq!(result, b"generic:data");
    }

    #[tokio::test]
    async fn dispatch_propagates_handler_error() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        registry.register("failing", |_payload, _flag| {
            Err(ActantError::Worker("handler error".into()))
        });

        let err = registry
            .dispatch("failing", signed(b""), new_cancel_flag())
            .await
            .unwrap_err();
        assert!(matches!(err, ActantError::Worker(_)));
    }

    #[tokio::test]
    async fn dispatch_isolates_handler_panic() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        registry.register("panicker", |_payload, _flag| {
            panic!("intentional test panic");
        });

        let err = registry
            .dispatch("panicker", signed(b""), new_cancel_flag())
            .await
            .unwrap_err();
        assert!(matches!(err, ActantError::Internal(ref m) if m.contains("panicked")));
    }

    #[tokio::test]
    async fn dispatch_passes_cancel_flag_to_handler() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        registry.register("cancel-aware", |_payload, flag| {
            if flag.load(std::sync::atomic::Ordering::Relaxed) {
                Err(ActantError::Worker("cancelled".into()))
            } else {
                Ok(b"completed".to_vec())
            }
        });

        // Not cancelled — should complete
        let flag = new_cancel_flag();
        let result = registry
            .dispatch("cancel-aware", signed(b""), flag.clone())
            .await
            .unwrap();
        assert_eq!(result, b"completed");

        // Cancelled — should return error
        let flag2 = new_cancel_flag();
        flag2.store(true, std::sync::atomic::Ordering::Relaxed);
        let err = registry
            .dispatch("cancel-aware", signed(b""), flag2)
            .await
            .unwrap_err();
        assert!(matches!(err, ActantError::Worker(_)));
    }

    #[tokio::test]
    async fn dispatch_concurrent_tasks_run_in_parallel() {
        let registry = TaskRegistry::new(4, 32, TEST_KEY.to_vec()).unwrap();
        registry.register("slow", |_payload, _flag| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            Ok(b"done".to_vec())
        });

        // TaskDispatcher::dispatch takes &self, so Arc<TaskRegistry> allows
        // concurrent dispatch calls from multiple tasks.
        let registry = Arc::new(registry);
        let r1 = registry.clone();
        let r2 = registry.clone();

        let start = std::time::Instant::now();

        let h1 = tokio::spawn(async move {
            TaskDispatcher::dispatch(&*r1, "slow", signed(b""), new_cancel_flag()).await
        });
        let h2 = tokio::spawn(async move {
            TaskDispatcher::dispatch(&*r2, "slow", signed(b""), new_cancel_flag()).await
        });

        let (res1, res2) = tokio::join!(h1, h2);
        let elapsed = start.elapsed();

        assert!(res1.unwrap().is_ok());
        assert!(res2.unwrap().is_ok());
        assert!(
            elapsed < std::time::Duration::from_millis(350),
            "dispatch took too long: {elapsed:?}"
        );
    }

    #[test]
    fn new_cancel_flag_starts_false() {
        let flag = new_cancel_flag();
        assert!(!flag.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn task_registry_new_with_invalid_workers_returns_error() {
        let registry = TaskRegistry::new(0, 8, TEST_KEY.to_vec());
        assert!(registry.is_err());
    }

    #[tokio::test]
    async fn dispatch_rejects_unsigned_payload() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        registry.register("echo", |_payload, _flag| Ok(b"ok".to_vec()));

        let err = registry
            .dispatch("echo", b"raw-unsigned".to_vec(), new_cancel_flag())
            .await
            .unwrap_err();
        assert!(
            matches!(err, ActantError::Internal(ref m) if m.contains("payload verification")),
            "expected payload verification error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn dispatch_rejects_tampered_payload() {
        let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
        registry.register("echo", |_payload, _flag| Ok(b"ok".to_vec()));

        let mut tampered = signed(b"original");
        let last = tampered.len() - 1;
        tampered[last] ^= 0xFF;

        let err = registry
            .dispatch("echo", tampered, new_cancel_flag())
            .await
            .unwrap_err();
        assert!(
            matches!(err, ActantError::Internal(ref m) if m.contains("signature mismatch")),
            "expected signature mismatch error, got: {:?}",
            err
        );
    }
}
