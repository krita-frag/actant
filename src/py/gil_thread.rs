use crossbeam_channel as channel;
use pyo3::prelude::*;
use std::sync::Arc;

type Task = Box<dyn FnOnce(Python<'_>) + Send + 'static>;

/// 永久持有 Python GIL 的后台线程，处理通过 crossbeam 通道发送的任务。
/// 消除热路径上的 `Python::attach` 开销 — 任务到达时线程已持有 GIL，
/// 因此 `call_soon_threadsafe` 和 Python 对象创建无需 GIL 竞争。
#[derive(Clone)]
pub(crate) struct GilThread {
    tx: Arc<channel::Sender<Task>>,
}

impl GilThread {
    /// 派生新的 GIL worker 线程并返回句柄。
    pub fn spawn() -> Self {
        let (tx, rx) = channel::unbounded::<Task>();
        std::thread::spawn(move || {
            Python::attach(|py| {
                // py.detach 在 recv() 阻塞时释放 GIL，
                // 任务可用时重新获取。
                while let Ok(task) = py.detach(|| rx.recv()) {
                    let t0 = std::time::Instant::now();
                    task(py);
                    let elapsed = t0.elapsed();
                    if elapsed > std::time::Duration::from_millis(1) {
                        tracing::trace!("gil_thread: task executed in {:?} (>1ms, slow)", elapsed);
                    } else {
                        tracing::trace!("gil_thread: task executed in {:?}", elapsed);
                    }
                }
                tracing::debug!("gil_thread: channel closed, exiting");
            });
        });
        Self { tx: Arc::new(tx) }
    }

    /// 向 GIL worker 发送闭包。worker 在已持有 GIL 的状态下调用它。
    pub fn send<F>(&self, task: F) -> Result<(), channel::SendError<Task>>
    where
        F: FnOnce(Python<'_>) + Send + 'static,
    {
        self.tx.send(Box::new(task))
    }
}

impl Drop for GilThread {
    fn drop(&mut self) {
        // 最后一个 Arc<Sender> drop 时，通道关闭，
        // worker 线程的 recv() 返回错误并干净退出。
    }
}
