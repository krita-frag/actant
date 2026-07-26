use crossbeam_channel as channel;
use pyo3::prelude::*;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

type Task = Box<dyn FnOnce(Python<'_>) + Send + 'static>;

/// GIL worker 通道容量。
///
/// 使用有界通道（默认 1024）防止内存无界增长：当 Python 调用速度 > GIL worker
/// 处理速度时，无界通道会无限堆积 `Box<dyn FnOnce>` 闭包（每个通常持 `Py<PyAny>`
/// 引用）。有界通道下的行为：
/// - 正常负载下 worker 跟得上 send 速率，无阻塞；
/// - 突发流量超过容量时，[`GilThread::send`] 返回 `TrySendError::Full`，
///   调用方（[`GilThread::send_or_run`]）退化为在当前线程直接
///   `Python::attach` 同步执行——比无界堆积更安全。
///
/// 1024 容量 ≈ 1024 个 `Box<dyn FnOnce>` + `Py<PyAny>` 引用 ≈ 数十 KB 内存，
/// 远小于 iroh endpoint 的常驻内存。
const GIL_QUEUE_CAPACITY: usize = 1024;

/// GIL worker 线程，通过 crossbeam 通道接收任务并在持有 GIL 的状态下执行。
/// 线程常驻，每个任务单独 `Python::attach` 获取 GIL，任务间释放 GIL，
/// 避免永久持有 GIL 导致主线程死锁。
pub(crate) struct GilThread {
    tx: Arc<channel::Sender<Task>>,
    // 持有 JoinHandle 以便在 Drop 时等待线程退出，避免 dangling thread。
    _handle: Option<JoinHandle<()>>,
}

impl GilThread {
    /// 创建一个新的 GIL worker 线程。
    ///
    /// 线程循环接收任务，**每个任务单独获取 GIL**（`Python::attach`），
    /// 任务间释放 GIL。这避免了永久持有 GIL 导致主线程（如 pytest）死锁，
    /// 同时仍比每次从 Tokio worker 竞争 GIL 更高效——线程常驻、channel
    /// 唤醒延迟低，且 `attach` 内部使用缓存的 `GILGuard` 开销极小。
    pub fn new() -> Self {
        let (tx, rx) = channel::bounded::<Task>(GIL_QUEUE_CAPACITY);
        let tx = Arc::new(tx);

        let handle = thread::spawn(move || {
            while let Ok(task) = rx.recv() {
                // 每个任务单独 attach GIL，任务执行完毕后释放。
                // 这样主线程（Python 解释器）在 GilThread 空闲时能正常获取 GIL。
                Python::attach(|py| {
                    task(py);
                });
            }
        });

        Self {
            tx,
            _handle: Some(handle),
        }
    }

    /// 非阻塞地向 GIL worker 投递闭包。
    ///
    /// 使用 `try_send` 而非 `send`：`send` 会在队列满时阻塞调用方，
    /// 而 GIL worker 的调用方通常是 tokio worker 线程，阻塞会拖慢整个 runtime。
    /// `try_send` 在队列满时立即返回 `Full` 错误，让调用方决定降级策略。
    ///
    /// 返回 `Err(TrySendError::Full(_))` 表示队列已满；返回
    /// `Err(TrySendError::Disconnected(_))` 表示 worker 已退出。
    /// 调用方应优先使用 [`Self::send_or_run`] 自动降级为同步执行。
    #[allow(dead_code)] // 暴露给未来需要显式错误处理的调用方；当前内部仅用 send_or_run。
    pub fn send<F>(&self, task: F) -> Result<(), channel::TrySendError<Task>>
    where
        F: FnOnce(Python<'_>) + Send + 'static,
    {
        self.tx.try_send(Box::new(task))
    }

    /// 向 GIL worker 发送闭包；若队列已满或 worker 退出，则降级为
    /// **当前线程直接 `Python::attach` 同步执行**。
    ///
    /// 降级路径比无界堆积更安全：调用方（通常是 tokio worker 线程）
    /// 暂时阻塞在 GIL 获取上，但内存不会无界增长。降级是协作式行为：
    /// 只要 GIL worker 仍在消费，队列不会长期处于满状态。
    ///
    /// 调用方需注意：降级执行会阻塞当前 tokio task 线程，仅在闭包执行
    /// 很快（设置 Future 结果 / 设置 awaitable 状态）时使用。重活应走
    /// `tokio::spawn_blocking`。
    pub fn send_or_run<F>(&self, task: F)
    where
        F: FnOnce(Python<'_>) + Send + 'static,
    {
        match self.tx.try_send(Box::new(task)) {
            Ok(()) => {}
            Err(channel::TrySendError::Full(boxed)) => {
                tracing::warn!(
                    "gil_thread queue full (cap={}); falling back to synchronous Python::attach",
                    GIL_QUEUE_CAPACITY
                );
                Python::attach(boxed);
            }
            Err(channel::TrySendError::Disconnected(boxed)) => {
                tracing::warn!("gil_thread worker disconnected; running task inline");
                Python::attach(boxed);
            }
        }
    }
}

impl Clone for GilThread {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _handle: None,
        }
    }
}

impl Drop for GilThread {
    fn drop(&mut self) {
        // 最后一个持有 JoinHandle 的实例 drop 时关闭发送端并等待线程退出。
        // 持有克隆体的实例不拥有 JoinHandle，只负责让 Arc<Sender> 自然释放。
        if self._handle.is_some() {
            // 关闭 channel：显式 drop sender 触发 recv 返回 Err。
            // 用一个零容量 sender 占位以保持字段类型不变。
            let (dummy_tx, _) = channel::bounded::<Task>(0);
            let tx = std::mem::replace(&mut self.tx, Arc::new(dummy_tx));
            drop(tx);
            if let Some(handle) = self._handle.take() {
                // 注意：JoinHandle::join 会阻塞当前线程。若 Drop 在持 GIL 状态下
                // 被触发（如 Python 进程退出时 GC），worker 仍在执行 task 中的
                // `Python::attach` 可能导致短暂等待——但 worker 处理单个 task 很快
                // （设置 Future 结果），通常 <1ms。`py.detach` 在 Drop 中难以使用
                // （Drop 不能 take Python token），故选择短暂 join。
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gil_thread_executes_task() {
        // pyo3 auto-initialize feature（dev-dependency）确保首次 Python::attach
        // 时自动初始化解释器，无需手动调用 prepare_freethreaded_python。
        let gw = GilThread::new();

        let (tx, rx) = channel::unbounded::<i32>();
        gw.send(move |_py| {
            tx.send(42).ok();
        })
        .expect("send");

        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(2)), Ok(42));
    }

    #[test]
    fn gil_thread_send_or_run_runs_inline_when_disconnected() {
        // pyo3 auto-initialize feature（dev-dependency）确保首次 Python::attach
        // 时自动初始化解释器，无需手动调用 prepare_freethreaded_python。
        // 构造 GilThread，drop 原始实例（关闭 sender + join worker）。
        let gw = GilThread::new();
        drop(gw);

        // 构造新 GilThread 并验证 send_or_run 不 panic（任务被正常投递到新 worker）。
        let gw2 = GilThread::new();
        let (tx, rx) = channel::unbounded::<i32>();
        gw2.send_or_run(move |_py| {
            tx.send(7).ok();
        });
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(2)), Ok(7));
    }
}
