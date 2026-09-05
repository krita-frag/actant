//! 本模块负责将任务 payload 分发给 worker 子进程并管理进程池。
//!
//! 进程池是唯一的任务执行后端：每个任务由独立的 Python worker 子进程
//! （`actant.task._worker`）执行，杀进程即精确终止一个任务，实现严格的
//! 进程级隔离。与工作流编排无直接耦合，属于通用执行基础设施。
//!
//! # 通信协议
//!
//! 与 `actant/task/_worker.py` 保持一致的长度前缀二进制帧；传输层为 stdio pipe：
//! 每帧为 ``[4 字节小端长度][1 字节类型][正文]``，帧头与正文连续写入同一管道：
//!
//! ```text
//! pipe 上 [4 字节长度][1 字节类型][正文]
//! ```
//!
//! - 父 → 子：`Dispatch`(0x01) 正文 = **v2 载荷**（紧凑控制头部 + cloudpickle 编码的
//!   `(func, args, kwargs)`，头部承载 retries/retry_delay_ms/task_id/workflow_id，
//!   见 `actant/task/_helpers.py::_build_v2_envelope`）；`Cancel`(0x02) 为空正文。
//!   关闭时由 `shutdown` 直接强杀空闲 worker（不发送 `Shutdown` 帧）。
//! - 子 → 父：`Result`(0x02) 正文 = cloudpickle 编码的 `(success, payload)`，
//!   Python 封装层按此约定解包。
//!
//! 本模块对正文格式不感知——payload 对 Rust 不透明，仅作字节校验与搬运。

use std::collections::VecDeque;
use std::io::IoSlice;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::Semaphore;

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};

use crate::common::ActantError;

/// 取消标志，用于协调任务分发器和跨进程取消操作。
///
/// 分发器为每次分发创建一个 `Arc<AtomicBool>`，并将其克隆传递给调用方。
/// 超时或取消时，分发器将其置为 `true` 并向 worker 发送 `Cancel` 帧；
/// worker 子进程内的协作代码轮询其线程取消事件以干净退出。
pub type CancelFlag = Arc<AtomicBool>;

/// 创建一个新鲜的取消标志（初始值为 `false`）。
pub fn new_cancel_flag() -> CancelFlag {
    Arc::new(AtomicBool::new(false))
}

// 帧类型常量（与 `actant/task/_worker.py` 保持一致）。
const FRAME_DISPATCH: u8 = 0x01;
const FRAME_CANCEL: u8 = 0x02;
// 子进程回传的结果帧类型。
const FRAME_RESULT: u8 = 0x02;

/// `Cancel` 帧的完整字节（4 字节长度 + 1 字节类型 + 空正文）。
const CANCEL_FRAME_BYTES: [u8; 5] = [1, 0, 0, 0, FRAME_CANCEL];

/// 帧头长度：4 字节小端长度。
const FRAME_HEADER_LEN: usize = 4;

/// 一个 worker 子进程：同一时刻恰好执行一个任务。
struct WorkerProc {
    child: Child,
    /// worker 写端（hot path）：派发独占持有，**无 Mutex**。
    ///
    /// 取消轮询不与派发抢同一把锁——取消通过独立 dup 出来的 `cancel_stdin` 写入。
    /// Cancel 帧固定 5 字节（< PIPE_BUF 512），内核保证原子写入不会与 hot-path
    /// 写交错（任何 < PIPE_BUF 单次 write 到 pipe 均为原子）。
    stdin: ChildStdin,
    /// 取消帧的独立写端（dup(2) of stdin），仅供 `spawn_cancel_poller` 使用。
    cancel_stdin: Option<tokio::fs::File>,
    /// stdout 由当前在途的分发独占读取；空闲时缓存于此供复用。
    stdout: ChildStdout,
}

/// 把一帧（``[4B 长度][1B 类型][正文]``）完整写入 `writer`。
///
/// `write_vectored` 是**单次写入尝试**：pipe 容量（默认 64KB）小于帧大小时
/// 只写入前缀就返回短写计数，剩余字节必须按已写偏移显式推进——否则 worker
/// 的 `read_exact` 会永久等待一条被截断的帧（大载荷 Dispatch 挂死的根源）。
/// 首次 `write_vectored` 保住小帧（≤ pipe 容量）的单 syscall 快路径；短写后
/// 剩余正文经 `write_all` 循环推进。
///
/// 独立为自由函数以便注入任意 `AsyncWrite`（tokio duplex 等）做封闭测试。
async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg_type: u8,
    body: &[u8],
) -> Result<(), ActantError> {
    let mut header = [0u8; FRAME_HEADER_LEN + 1];
    let frame_len: u32 = (1 + body.len()) as u32;
    header[..FRAME_HEADER_LEN].copy_from_slice(&frame_len.to_le_bytes());
    header[FRAME_HEADER_LEN] = msg_type;
    let total = header.len() + body.len();
    let iov = [IoSlice::new(&header), IoSlice::new(body)];
    let mut written = writer
        .write_vectored(&iov)
        .await
        .map_err(map_io("worker stdin"))?;
    if written < total {
        // 短写推进：先补齐帧头剩余字节（仅当 writev 连帧头都未写完），再
        // `write_all` 剩余正文（write_all 内部循环处理后续短写）。
        if written < header.len() {
            writer
                .write_all(&header[written..])
                .await
                .map_err(map_io("worker stdin"))?;
            written = header.len();
        }
        writer
            .write_all(&body[written - header.len()..])
            .await
            .map_err(map_io("worker stdin"))?;
    }
    writer.flush().await.map_err(map_io("worker stdin"))?;
    Ok(())
}

impl WorkerProc {
    /// 向子进程发送一帧：``[4B 长度][1B 类型][正文]`` 连续写入 pipe。
    async fn send_frame(&mut self, msg_type: u8, body: &[u8]) -> Result<(), ActantError> {
        write_frame(&mut self.stdin, msg_type, body).await
    }

    /// 异步地从 stdout 读结果帧正文。
    ///
    /// 先读 5 字节帧头（4B 长度 + 1B 类型），再按帧头长度读完整正文。
    /// 到达 EOF（worker 崩溃或正常退出）返回 `Ok(None)`。
    async fn read_result_frame(&mut self) -> Result<Option<Vec<u8>>, ActantError> {
        let mut header = [0u8; FRAME_HEADER_LEN + 1];
        match self.stdout.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(map_io("worker stdout")(e)),
        }
        let mut len_bytes = [0u8; FRAME_HEADER_LEN];
        len_bytes.copy_from_slice(&header[..FRAME_HEADER_LEN]);
        let len = u32::from_le_bytes(len_bytes) as usize;
        if len < 1 {
            return Err(ActantError::Worker(
                "worker returned invalid frame length".into(),
            ));
        }
        let msg_type = header[FRAME_HEADER_LEN];
        let body_len = len - 1;
        let mut buf = vec![0u8; body_len];
        self.stdout
            .read_exact(&mut buf)
            .await
            .map_err(map_io("worker stdout"))?;
        if msg_type != FRAME_RESULT {
            tracing::warn!(
                frame_type = msg_type,
                "worker returned unexpected frame type (expected Result)"
            );
        }
        Ok(Some(buf))
    }
}

fn map_io(ctx: &'static str) -> impl Fn(std::io::Error) -> ActantError + 'static {
    move |e| ActantError::Worker(format!("{ctx}: {e}"))
}

/// 进程池任务分发器。
///
/// # 生命周期
///
/// 构造时按 `num_workers` 一次性拉起 worker 子进程。`dispatch` 通过有界信号量
/// 领用一个空闲 worker 发送 `Dispatch` 帧；超时**即时强杀**并替补一个新 worker
/// （不等待取消宽限期，资源即刻释放）；取消发送 `Cancel` 帧协作退出，宽限期
/// （`worker_cancel_grace_ms`）内未收到结果帧才强杀兜底；崩溃（EOF / 写失败）
/// 同样触发替补，保持进程池容量恒定。
#[async_trait::async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// 将任务分发给 worker 子进程执行。
    ///
    /// `timeout` 是硬超时：超时后 dispatcher **立即强杀** worker 进程，释放计算
    /// 资源并回收并发槽位（不等待取消宽限期，任务已失联视为无需清理）。
    /// `cancel_flag` 置位时发送 `Cancel` 帧触发协作取消，宽限期
    /// （`worker_cancel_grace_ms`）内未收到结果帧才强杀兜底。两者均保持进程池
    /// 容量恒定（杀一个补一个）。
    async fn dispatch(
        &self,
        name: &str,
        payload: Vec<u8>,
        cancel_flag: CancelFlag,
        timeout: Duration,
    ) -> crate::common::Result<Vec<u8>>;

    /// 关闭 dispatcher 并终止所有 worker 进程。
    fn shutdown(&self) {}
}

pub struct ProcessTaskDispatcher {
    /// worker 启动 argv（生产：`[python, -m, actant.task._worker]`）。
    worker_argv: Vec<String>,
    /// worker 子进程的 `PYTHONPATH`，透传给 spawn 的 Command 环境。
    python_path: Option<String>,
    /// 空闲 worker 队列。并发度由 `slots` 信号量保证，队列长度与之一致。
    free_workers: Mutex<VecDeque<WorkerProc>>,
    /// 并发槽位：容量 = 进程池大小，dispatch 据此公平领用空闲 worker。
    slots: Arc<Semaphore>,
    /// Payload 签名密钥。非空时 dispatch 会验证 payload MAC。
    signing_key: Vec<u8>,
    /// 取消后等待 worker 协作退出的宽限期，超时则强杀。
    cancel_grace: Duration,
    /// 关闭时终止空闲 worker。
    shutting_down: AtomicBool,
}

impl ProcessTaskDispatcher {
    /// 创建进程池任务分发器。
    ///
    /// `worker_program` 为 worker 解释器路径（如 `sys.executable`），进程池
    /// 以 `[worker_program, -m, actant.task._worker]` 拉起 worker 子进程。
    /// `python_path` 若非空，透传给 worker 子进程作为 `PYTHONPATH`，保证
    /// 模块级任务函数在子进程内可被 by-reference 再导入。
    pub fn new(
        num_workers: usize,
        worker_program: String,
        worker_cancel_grace_ms: u64,
        signing_key: Vec<u8>,
        python_path: Vec<String>,
    ) -> crate::common::Result<Self> {
        if worker_program.trim().is_empty() {
            return Err(ActantError::Config(
                "worker_program must be a non-empty interpreter path".into(),
            ));
        }
        let argv = vec![
            worker_program,
            "-m".to_string(),
            "actant.task._worker".to_string(),
        ];
        // PYTHONPATH 条目须以平台路径列表分隔符（unix `:`）拼接，而非路径内分隔符。
        let python_env = if python_path.is_empty() {
            None
        } else {
            std::env::join_paths(python_path.iter())
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        };
        let cancel_grace = Duration::from_millis(worker_cancel_grace_ms.max(1));
        let dispatcher = Self {
            worker_argv: argv.clone(),
            python_path: python_env,
            free_workers: Mutex::new(VecDeque::with_capacity(num_workers.max(0))),
            slots: Arc::new(Semaphore::new(num_workers)),
            signing_key,
            cancel_grace,
            shutting_down: AtomicBool::new(false),
        };
        for _ in 0..num_workers {
            let worker = ProcessTaskDispatcher::spawn_one(
                &dispatcher.worker_argv,
                dispatcher.python_path.as_deref(),
                &dispatcher,
            )?;
            dispatcher.free_workers.lock().push_back(worker);
        }
        Ok(dispatcher)
    }

    /// 测试专用：完全不拉取 worker 子进程。
    ///
    /// `dispatch` 在 `verify`（签名校验失败即提前返回）之前不需要 worker，
    /// 因此校验路径测试无需实际子进程，可纯 Rust 构建。
    #[cfg(test)]
    fn without_workers(signing_key: Vec<u8>) -> Self {
        Self {
            worker_argv: Vec::new(),
            python_path: None,
            free_workers: Mutex::new(VecDeque::new()),
            slots: Arc::new(Semaphore::new(0)),
            signing_key,
            cancel_grace: Duration::from_millis(10),
            shutting_down: AtomicBool::new(false),
        }
    }
    /// 在 Unix 下从 `ChildStdin` dup 原始 fd 封装回 tokio `File`，作为取消帧独立写端。
    ///
    /// Cancel 帧固定 5 字节（< PIPE_BUF 512），dup 后并发写入不会与 hot-path
    /// 写交错（POSIX 保证任何 < PIPE_BUF 的单次 write 到 pipe 为原子）。
    /// 非 Unix 返回 `None`，降级为 dispatcher 自身发送 Cancel（语义等价）。
    #[cfg(unix)]
    fn try_clone_cancel_stdin(stdin: &ChildStdin) -> Option<tokio::fs::File> {
        // 变参符号必须经 libc crate 调用：手工声明的变参 extern 在当前工具链上
        // 会丢失第三个实参（F_SETFL 静默失效，O_NONBLOCK 落不到 fd 上）。
        let raw = stdin.as_raw_fd();
        // SAFETY: `dup`/`fcntl`/`close` 是纯系统调用，`from_raw_fd` 仅在 dup 返回 ≥0 时执行。
        let dup_raw = unsafe { libc::dup(raw) };
        if dup_raw < 0 {
            return None;
        }
        // tokio `from_std` 要求底层 fd 已设置 nonblocking（否则写入会 block tokio
        // worker thread）。fcntl(F_SETFL, old | O_NONBLOCK) 完成，不依赖额外 trait。
        let flags = unsafe { libc::fcntl(dup_raw, libc::F_GETFL) };
        if flags < 0 {
            // 拿不到 flags（罕见）→ 就地关闭 dup fd（防泄漏），退回 fallback 路径
            // （poller 不写 Cancel，dispatcher 自己 terminate_and_replace 兜底发 Cancel）。
            unsafe { libc::close(dup_raw) };
            return None;
        }
        if unsafe { libc::fcntl(dup_raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            unsafe { libc::close(dup_raw) };
            return None;
        }
        let std_file = unsafe { std::fs::File::from_raw_fd(dup_raw) };
        Some(tokio::fs::File::from_std(std_file))
    }

    #[cfg(not(unix))]
    fn try_clone_cancel_stdin(_stdin: &ChildStdin) -> Option<tokio::fs::File> {
        None
    }

    /// 拉起单个 worker 子进程并把 stderr 转发到 tracing。
    fn spawn_one(
        argv: &[String],
        python_path: Option<&str>,
        _ctx: &ProcessTaskDispatcher,
    ) -> Result<WorkerProc, ActantError> {
        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(pp) = python_path {
            cmd.env("PYTHONPATH", pp);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| ActantError::Worker(format!("failed to spawn worker: {e}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ActantError::Worker("worker stdin not available".into()))?;
        let cancel_stdin = Self::try_clone_cancel_stdin(&stdin);
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ActantError::Worker("worker stdout not available".into()))?;
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(drain_stderr(stderr));
        }
        Ok(WorkerProc {
            child,
            stdin,
            cancel_stdin,
            stdout,
        })
    }

    /// 领用一个空闲 worker。调用方须先持有 `slots` 信号量许可，
    /// 保证空闲队列非空；队列为空时等待（仅发生在极端竞态下）。
    async fn pop_worker(&self) -> Result<WorkerProc, ActantError> {
        loop {
            if let Some(w) = self.free_workers.lock().pop_front() {
                return Ok(w);
            }
            if self.shutting_down.load(Ordering::Acquire) {
                return Err(ActantError::Internal("worker pool shut down".into()));
            }
            // 信号量许可保证队列应有 worker；此处兜底等待以免忙轮询。
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn release_worker(&self, worker: WorkerProc) {
        // shutdown 后不再回收入池：在途任务结束（或强杀）释放的 worker 直接终止
        // 回收，否则该 worker 塞回队列后无人消费、也无人强杀，父进程存活期间
        // 泄漏一个常驻 Python 子进程。
        if self.shutting_down.load(Ordering::Acquire) {
            let mut worker = worker;
            if let Err(e) = worker.child.start_kill() {
                tracing::warn!(error = %e, "failed to kill worker released after shutdown");
            }
            tokio::spawn(async move {
                if let Err(e) = worker.child.wait().await {
                    tracing::warn!(error = %e, "failed to reap worker released after shutdown");
                }
            });
            return;
        }
        self.free_workers.lock().push_back(worker);
    }

    /// 硬超时强杀：不发送 `Cancel`、不等待宽限期，立即终止并替补。
    ///
    /// 硬超时意味着任务已失联（未在时限内返回），无需再给协作清理机会；
    /// 立即强杀保证计算资源即时释放、槽位即时回收（与取消的宽限语义区分）。
    async fn kill_and_replace(&self, mut worker: WorkerProc) {
        Self::force_kill(&mut worker.child).await;
        self.ensure_replacement();
        let _ = worker;
    }

    /// 等待取消标志置位后的宽限窗口结束（15ms 轮询，与 `spawn_cancel_poller` 一致）。
    ///
    /// 取消置位后先进入 `grace` 宽限窗口；协作 worker 会在窗口内以结果帧响应，
    /// 由 `dispatch` 的**结果臂**（select 中列在前，优先）赢得并发而保留 worker 复用；
    /// 仅当窗口耗尽仍未收到结果帧（不可协作 worker）才返回，触发本臂强杀兜底。
    /// `active` 为真时才轮询，避免分发已结束（worker 释放复用后）的残留取消信号
    /// 误触发取消分支。
    async fn wait_for_cancel(flag: &CancelFlag, active: &AtomicBool, grace: Duration) {
        loop {
            if !active.load(Ordering::Relaxed) {
                return;
            }
            if flag.load(Ordering::Relaxed) {
                tokio::time::sleep(grace).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    /// 取消强杀：向 worker 发送 `Cancel` 帧 → 宽限等待协作退出 → 未退出则强杀，
    /// 并替补新 worker 以保持进程池容量恒定。调用方持有该 worker 的槽位许可。
    async fn terminate_and_replace(&self, mut worker: WorkerProc) {
        let _ = worker.send_frame(FRAME_CANCEL, &[]).await;
        let grace = tokio::time::sleep(self.cancel_grace);
        tokio::pin!(grace);
        let child = &mut worker.child;
        tokio::select! {
            _ = &mut grace => {
                // 宽限期耗尽仍未退出，强杀。
                Self::force_kill(child).await;
            }
            _status = child.wait() => {
                // worker 在宽限期内协作退出。
            }
        }
        self.ensure_replacement();
        let _ = worker; // 丢弃已终止的 worker
    }

    /// 强制终止子进程并等待其回收，避免僵尸进程。
    async fn force_kill(child: &mut Child) {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    /// 替补一个已崩溃/被杀的 worker。
    fn ensure_replacement(&self) {
        // shutdown 后不再替补：在途任务的终止路径（超时强杀/崩溃回收）会走到这里，
        // 新 spawn 的 worker 在 shutdown 语义下无人消费也无人回收，属纯泄漏。
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        match ProcessTaskDispatcher::spawn_one(&self.worker_argv, self.python_path.as_deref(), self)
        {
            Ok(worker) => self.release_worker(worker),
            Err(e) => tracing::warn!(error = %e, "failed to respawn worker; pool shrunk"),
        }
    }

    /// `dispatch` 内派发帧写失败 / 读到 EOF 时的崩溃回收路径。
    async fn recover_crash(&self, mut worker: WorkerProc) {
        Self::force_kill(&mut worker.child).await;
        self.ensure_replacement();
        let _ = worker;
    }

    /// 后台任务：轮询 `cancel_flag`，置位时向 worker 发送 `Cancel` 帧。
    /// `active` 标志防止 worker 被释放复用后残留的轮询误发帧。
    ///
    /// 使用独立 dup 出的 `cancel_stdin`：hot-path dispatch 不再与 poller 竞争同一把
    /// Mutex。Cancel 帧固定 5 字节（< PIPE_BUF），Unix 内核保证 write 原子，不与
    /// dispatch 的并发写交错。若 `cancel_stdin` 不可用（非 Unix / dup 失败），
    /// poller 不写帧——dispatcher 侧 `terminate_and_replace` 兜底自行发 Cancel，
    /// 语义等价，仅延迟 ≤ 15ms（一轮 poll interval）。
    ///
    /// `writing` 由 dispatch 与 poller 共享：poller 进入写帧路径前置位，dispatch
    /// 在 poller 结束后据此判断 worker stdin 是否可能残留 Cancel 帧
    ///（见 [`Self::stop_cancel_poller`]）。
    fn spawn_cancel_poller(
        cancel_stdin: Option<tokio::fs::File>,
        cancel_flag: CancelFlag,
        active: Arc<AtomicBool>,
        writing: Arc<AtomicBool>,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Receiver<Option<tokio::fs::File>>,
    ) {
        // 向 poller task 传入 cancel_stdin 所有权；结束后通过 oneshot 归还给 dispatch。
        // 这样 poller 活在自己的 task 里，dispatch 与 poller 之间无跨 task Arc<Mutex<…>>。
        let (ret_tx, ret_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut cs = cancel_stdin;
            loop {
                if cancel_flag.load(Ordering::Relaxed) {
                    if active.load(Ordering::Relaxed) {
                        // 先置写帧标记再进入写路径：即使随后被 abort（写帧可能
                        // 进行到一半），dispatch 也能从标记得知「不能复用」。
                        writing.store(true, Ordering::Release);
                        if let Some(c) = cs.as_mut() {
                            let _ = c.write_all(&CANCEL_FRAME_BYTES).await;
                            let _ = c.flush().await;
                        }
                    }
                    // 归还 cancel_stdin；接收端已 abort 时发送失败，File 随之
                    // Drop 关闭 fd，由下次 ensure_replacement 重建，无需补救。
                    let _ = ret_tx.send(cs);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        });
        (handle, ret_rx)
    }

    /// 终止取消轮询任务并等待其退出，返回 `(cancel_stdin, poller 可能已写帧)`。
    ///
    /// 所有处置 worker 的路径都必须先经过这里再释放/复用：abort 之后**等待
    /// poller 真正退出**才返回，保证「poller 检查 active → 写 Cancel 帧」与
    /// 「结果臂释放 worker 复用」互斥——否则 poller 可能已通过 active 检查但尚未
    /// 写帧，结果臂先 release 了 worker，过期 Cancel 帧随后落入复用 worker 的
    /// stdin，误杀下一个任务（TOCTOU）。
    ///
    /// 第二个返回值为 `true` 表示 poller 已进入写帧阶段，worker stdin 上可能残留
    /// 完整或部分 Cancel 帧，复用会让残留帧与下一任务的 Dispatch 帧失步——
    /// 调用方应回收该 worker 而非复用。
    async fn stop_cancel_poller(
        canceller: tokio::task::JoinHandle<()>,
        cancel_stdin_rx: tokio::sync::oneshot::Receiver<Option<tokio::fs::File>>,
        writing: &AtomicBool,
    ) -> (Option<tokio::fs::File>, bool) {
        canceller.abort();
        match cancel_stdin_rx.await {
            // poller 观察到 flag 置位后自行结束并归还写端；写帧是否发生以
            // writing 标记为准。
            Ok(cancel_stdin) => (cancel_stdin, writing.load(Ordering::Acquire)),
            // poller 被 abort：等待任务彻底退出后再读 writing——此刻 poller 不可能
            // 再推进，writing 为 false 即证明从未进入写帧阶段（任务正常完成路径）。
            Err(_) => {
                // JoinError(Cancelled) 是 abort 的预期结果，仅需等待任务退出。
                let _ = canceller.await;
                (None, writing.load(Ordering::Acquire))
            }
        }
    }
}

/// 指标边带行前缀：worker 经 stderr 单行上报从属计时指标（见 `_worker.py`）。
const METRIC_LINE_PREFIX: &str = "actant_metric: ";

/// 将 worker stderr 逐行转发到 tracing，避免子进程日志丢失（隔离副产）。
///
/// 同时识别从属指标边带：以 ``actant_metric:`` 开头、形如
/// ``<name>=<value_ms>`` 的行，汇入对应 OTel histogram（当前仅
/// ``python.handler_ms``）；其余行原样作为日志透传，不改变可观测性契约。
async fn drain_stderr(mut stderr: ChildStderr) {
    let mut lines = tokio::io::BufReader::new(&mut stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(value) = line.strip_prefix(METRIC_LINE_PREFIX) {
            if let Some((name, value_ms)) = value.split_once('=') {
                if let Ok(ms) = value_ms.trim().parse::<u64>() {
                    if name == "python.handler_ms" {
                        crate::metrics::observe_python_handler_ms(ms);
                    }
                }
            }
            continue;
        }
        tracing::warn!(target: "actant.worker", "{line}");
    }
}

/// 提供兼容构造入口以便 `RuntimeBuilder` 以 `Arc<dyn TaskDispatcher>` 注入。
///
/// 使用独立的 trait 对象封装，避免 `ProcessTaskDispatcher` 直接实现 trait 时
/// 与测试用 `StubDispatcher` 的手写实现冲突（无实际冲突，仅为 API 清晰）。
#[async_trait::async_trait]
impl TaskDispatcher for ProcessTaskDispatcher {
    async fn dispatch(
        &self,
        _name: &str,
        payload: Vec<u8>,
        cancel_flag: CancelFlag,
        timeout: Duration,
    ) -> crate::common::Result<Vec<u8>> {
        let verified = crate::common::payload::verify(&self.signing_key, &payload)
            .map_err(|e| ActantError::Internal(format!("payload verification: {e}")))?;
        // 领用并发槽位（容量 = 进程池大小），确保公平获取空闲 worker。
        let _permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ActantError::Internal("worker pool closed".into()))?;
        let mut worker = self.pop_worker().await?;

        // 取出独立取消写端，交由 poller task 拥有；归还有两种路径：
        //  1) poller 通过 oneshot 在自己发完 Cancel 后归还（正常/取消分支）；
        //  2) abort poller 时，cancel_stdin 所有权在 poller 内 → oneshot 被 close 变为
        //     RecvError → 本函数视为 None，等下次 ensure_replacement / spawn_one 重建。
        // 这样 hot-path send_frame（→write_vectored）全程无 Mutex、无 Arc clone 竞争。
        let cancel_stdin = worker.cancel_stdin.take();
        let active = Arc::new(AtomicBool::new(true));
        // 与 poller 共享的写帧标记：poller 进入写 Cancel 路径前置位，dispatch 在
        // poller 退出后据此判断 worker stdin 是否可能残留过期 Cancel 帧。
        let writing = Arc::new(AtomicBool::new(false));
        let (canceller, cancel_stdin_rx) = Self::spawn_cancel_poller(
            cancel_stdin,
            cancel_flag.clone(),
            active.clone(),
            writing.clone(),
        );

        if let Err(e) = worker.send_frame(FRAME_DISPATCH, &verified).await {
            // 派发帧写失败：worker 已死，等待 poller 退出后替补并返回错误。
            active.store(false, Ordering::Relaxed);
            let (cancel_stdin, _) =
                Self::stop_cancel_poller(canceller, cancel_stdin_rx, &writing).await;
            worker.cancel_stdin = cancel_stdin;
            tracing::warn!(error = %e, "worker dispatch write failed; crashing worker");
            self.recover_crash(worker).await;
            return Err(ActantError::Worker(format!(
                "worker terminated before dispatch: {e}"
            )));
        }

        let outcome = tokio::select! {
            result = worker.read_result_frame() => {
                active.store(false, Ordering::Relaxed);
                // 先等 poller 彻底退出再决定 worker 去留：保证「poller 检查 active →
                // 写 Cancel 帧」与释放/复用互斥，过期帧不会落入复用的 worker。
                let (cancel_stdin, cancel_maybe_written) =
                    Self::stop_cancel_poller(canceller, cancel_stdin_rx, &writing).await;
                worker.cancel_stdin = cancel_stdin;
                match result {
                    Ok(Some(body)) => {
                        if cancel_maybe_written {
                            // 取消与完成竞态：Cancel 帧可能已写入该 worker 的 stdin，
                            // 复用会让残留帧误杀下一任务，回收并替补。
                            tracing::warn!(
                                "cancel frame raced with task completion; recycling worker"
                            );
                            self.recover_crash(worker).await;
                        } else {
                            self.release_worker(worker);
                        }
                        Ok(body)
                    }
                    Ok(None) => {
                        // EOF：worker 崩溃退出。替补后返回错误。
                        tracing::warn!("worker encountered EOF (crash) while reading result");
                        self.recover_crash(worker).await;
                        Err(ActantError::Worker(
                            "worker process crashed while executing task".into(),
                        ))
                    }
                    Err(e) => {
                        self.recover_crash(worker).await;
                        Err(e)
                    }
                }
            }
            _ = Self::wait_for_cancel(&cancel_flag, &active, self.cancel_grace) => {
                // 取消尚未被协作 worker 以结果帧响应（非协作 worker 阻塞在
                // 不可中断代码）：发送 `Cancel`、宽限等待协作退出，宽限耗尽才强杀，
                // 兜底回收槽位与进程池容量。
                active.store(false, Ordering::Relaxed);
                let (cancel_stdin, _) =
                    Self::stop_cancel_poller(canceller, cancel_stdin_rx, &writing).await;
                worker.cancel_stdin = cancel_stdin;
                tracing::warn!("task cancelled, granting grace before killing worker");
                self.terminate_and_replace(worker).await;
                Err(ActantError::Cancelled("task cancelled".into()))
            }
            _ = tokio::time::sleep(timeout) => {
                active.store(false, Ordering::Relaxed);
                let (cancel_stdin, _) =
                    Self::stop_cancel_poller(canceller, cancel_stdin_rx, &writing).await;
                worker.cancel_stdin = cancel_stdin;
                tracing::warn!(timeout_ms = timeout.as_millis(), "task hard-timeout, killing worker");
                // 硬超时：任务已失联，立即强杀即时释放资源（不等取消宽限期）。
                self.kill_and_replace(worker).await;
                Err(ActantError::Timeout(format!(
                    "task timed out after {}ms",
                    timeout.as_millis()
                )))
            }
        };
        outcome
    }

    fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        // 取出全部空闲 worker 并强杀。start_kill 只投递终止信号，必须 spawn wait()
        // 消费终止状态回收子进程，否则留下僵尸进程。调用方保证关闭前排空在途
        // 任务，且 shutdown 在 tokio 运行时上下文中执行（与 spawn_one 一致）。
        let workers = std::mem::take(&mut *self.free_workers.lock());
        for mut w in workers {
            if let Err(e) = w.child.start_kill() {
                tracing::warn!(error = %e, "failed to kill idle worker on shutdown");
            }
            tokio::spawn(async move {
                if let Err(e) = w.child.wait().await {
                    tracing::warn!(error = %e, "failed to reap worker on shutdown");
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/dispatcher.rs"]
mod tests;
