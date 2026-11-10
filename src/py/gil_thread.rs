use crossbeam_channel as channel;
use pyo3::prelude::*;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
    /// 通道唯一的 Sender 放在共享 `Option` 里。
    ///
    /// 关闭通道只需 `take()` 一次：即使还有其他 `GilThread` 克隆（各自持同一
    /// `Arc`）存活，Sender 本身也已被释放，`recv()` 会立即返回 `Err`。若像
    /// 早先那样在字段里存 `Arc<Sender>`，克隆存活时 drop 只减少引用计数，
    /// 通道不关闭、worker 不退出，Drop 里的 `join()` 就永久阻塞。
    tx: Arc<parking_lot::Mutex<Option<channel::Sender<Task>>>>,
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
        let tx = Arc::new(parking_lot::Mutex::new(Some(tx)));

        let handle = thread::spawn(move || {
            while let Ok(task) = rx.recv() {
                // 每个任务单独 attach GIL，任务执行完毕后释放。
                // 这样主线程（Python 解释器）在 GilThread 空闲时能正常获取 GIL。
                //
                // 用 `try_attach` 而非 `attach`：Drop 超时会把本线程 detach，
                // 若解释器已在收尾，`attach` 会直接 abort 进程，`try_attach`
                // 只返回 `None`，据此退出循环即可。
                if Python::try_attach(task).is_none() {
                    tracing::warn!("gil_thread: interpreter unavailable; worker exiting");
                    break;
                }
            }
        });

        Self {
            tx,
            _handle: Some(handle),
        }
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
        let task: Task = Box::new(task);
        let Some(sender) = self.tx.lock().clone() else {
            // 通道已被 drop 方关闭（Runtime 正在收尾）：就地执行，保证结果
            // 不会因为「没人接收」而丢失。
            tracing::warn!("gil_thread channel closed; running task inline");
            Python::attach(task);
            return;
        };
        match sender.try_send(task) {
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

/// `Drop` 里等待 worker 退出的上限。
///
/// worker 单个任务只做「设置 Future 结果」这类微秒级操作，正常远快于此值；
/// 超时说明它正卡在 `Python::attach` 上等 GIL，而当前线程恰好持着 GIL
/// （GC / 解释器退出时就是这种情况）——继续等就是死锁。
const WORKER_JOIN_TIMEOUT: Duration = Duration::from_millis(500);

impl Drop for GilThread {
    fn drop(&mut self) {
        // 只有持有 JoinHandle 的实例负责关闭通道；克隆体不参与。
        if self._handle.is_none() {
            return;
        }
        // 关闭 channel：取走唯一 Sender 并释放，`recv()` 立即返回 Err。
        drop(self.tx.lock().take());

        let Some(handle) = self._handle.take() else {
            return;
        };
        // 带超时地等待线程退出。`Drop` 可能在持 GIL 时触发，而无条件
        // `handle.join()` 会与「worker 正阻塞在 attach」互等死锁，因此不能直接
        // join：把 join 挪到旁路线程，主线程只等一个有界时间。
        let (done_tx, done_rx) = channel::bounded::<()>(1);
        let joiner = thread::spawn(move || {
            let _ = handle.join();
            let _ = done_tx.send(());
        });
        if done_rx.recv_timeout(WORKER_JOIN_TIMEOUT).is_ok() {
            let _ = joiner.join();
        } else {
            tracing::warn!(
                timeout_ms = WORKER_JOIN_TIMEOUT.as_millis() as u64,
                "gil_thread worker did not exit in time; detaching it"
            );
            // 旁路线程仍在等那个 worker；丢弃句柄避免在此处再阻塞一次。
            std::mem::forget(joiner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 显式初始化解释器。
    ///
    /// worker 循环用 `try_attach`（不在解释器收尾时 abort 进程），它**不会**
    /// 像 `attach` 那样借 auto-initialize 自动拉起解释器；测试进程里必须先
    /// 自己初始化，否则 worker 首次取 GIL 就会看到 None。
    fn ensure_python() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(pyo3::Python::initialize);
    }

    #[test]
    fn gil_thread_executes_task() {
        ensure_python();
        let gw = GilThread::new();

        let (tx, rx) = channel::unbounded::<i32>();
        gw.send_or_run(move |_py| {
            tx.send(42).ok();
        });

        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(2)), Ok(42));
    }

    #[test]
    fn gil_thread_send_or_run_runs_inline_when_disconnected() {
        ensure_python();
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
