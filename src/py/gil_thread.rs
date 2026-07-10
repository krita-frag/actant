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
