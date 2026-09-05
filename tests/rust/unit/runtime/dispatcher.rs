//! Unit tests extracted from `src/runtime/dispatcher.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.
//!
//! `ProcessTaskDispatcher` 依赖真实 Python worker 子进程，进程级隔离/硬超时/
//! 强杀行为由 Python e2e 测试覆盖（见 `tests/python/`）。本模块聚焦可封闭的
//! 纯 Rust 测试：payload 签名校验（无需子进程）、帧协议常量、trait 默认实现、
//! 取消标志。

use super::*;

const TEST_KEY: &[u8] = b"test-key";

fn signed(payload: &[u8]) -> Vec<u8> {
    crate::common::payload::sign(TEST_KEY, payload).unwrap()
}

/// 构造一个不拉取任何 worker 的 dispatcher。`dispatch` 在校验 payload 签名
/// 失败时提前返回，无需任何子进程即可覆盖这些路径。
fn hermetic_dispatcher(key: &[u8]) -> ProcessTaskDispatcher {
    ProcessTaskDispatcher::without_workers(key.to_vec())
}

#[test]
fn new_cancel_flag_starts_false() {
    let flag = new_cancel_flag();
    assert!(!flag.load(Ordering::Relaxed));
}

#[test]
fn new_requires_non_empty_worker_program() {
    let err = ProcessTaskDispatcher::new(1, String::new(), 1000, TEST_KEY.to_vec(), Vec::new());
    assert!(
        matches!(err, Err(ActantError::Config(_))),
        "empty worker_program should be rejected"
    );
}

#[tokio::test]
async fn dispatch_rejects_unsigned_payload() {
    let d = hermetic_dispatcher(TEST_KEY);
    let err = d
        .dispatch(
            "echo",
            b"raw-unsigned".to_vec(),
            new_cancel_flag(),
            Duration::MAX,
        )
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
    let d = hermetic_dispatcher(TEST_KEY);
    let mut tampered = signed(b"original");
    let last = tampered.len() - 1;
    tampered[last] ^= 0xFF;

    let err = d
        .dispatch("echo", tampered, new_cancel_flag(), Duration::MAX)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(ref m) if m.contains("signature mismatch")),
        "expected signature mismatch error, got: {:?}",
        err
    );
}

#[tokio::test]
async fn dispatch_with_empty_key_rejects_signed_payload() {
    // 空 key 禁用签名，但签名 payload（MAC_PREFIX 开头）应被拒绝，
    // 避免在禁用签名的节点上误处理本应签名的 payload。
    let d = hermetic_dispatcher(&[]);
    let signed_payload = crate::common::payload::sign(b"some-key", b"data").unwrap();

    let err = d
        .dispatch("echo", signed_payload, new_cancel_flag(), Duration::MAX)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(ref m)
            if m.contains("signing disabled but payload appears signed")),
        "expected signing-disabled rejection, got: {:?}",
        err
    );
}

#[test]
fn shutdown_default_impl_is_noop() {
    // 默认 shutdown 实现不应 panic；对已构造的 dispatcher 调用应安全。
    let d = hermetic_dispatcher(TEST_KEY);
    TaskDispatcher::shutdown(&d);
}

#[test]
fn shutdown_terminates_free_workers() {
    let d = hermetic_dispatcher(TEST_KEY);
    // shutdown 后 shutting_down 标志置位；无 worker 时不 panic。
    d.shutdown();
    assert!(d.shutting_down.load(Ordering::Acquire));
}

#[test]
fn cancel_frame_bytes_layout() {
    // Cancel 帧 = 4 字节小端长度(1) + 1 字节类型(FRAME_CANCEL) + 空正文。
    assert_eq!(CANCEL_FRAME_BYTES, [1, 0, 0, 0, FRAME_CANCEL]);
    assert_eq!(FRAME_CANCEL, 0x02);
    assert_eq!(FRAME_RESULT, 0x02);
    assert_eq!(FRAME_DISPATCH, 0x01);
}

// ───────────────────────── 取消轮询器契约（TOCTOU 回归） ─────────────────────────
//
// 取消轮询器契约（TOCTOU 防护）：若结果臂在 poller「检查 active 之后、写 Cancel 帧之前」
// 释放 worker 复用，过期 Cancel 帧会滞留在复用 worker 的 stdin 上误杀下一任务。契约：
//   1) 所有处置 worker 的路径必须经 `stop_cancel_poller`（abort + 等待退出）后才
//      释放/复用 worker；
//   2) `stop_cancel_poller` 返回 `maybe_written == false` ⇒ worker stdin 上不可能
//      残留任何字节；返回 `true` ⇒ 可能残留（完整或部分）Cancel 帧，必须回收。
// 以下测试用真实 pipe 模拟 worker stdin 验证该不变量（仅 Unix：Cancel 帧走
// dup 写端，是 Unix 专属路径）。

#[cfg(unix)]
mod cancel_poller {
    use super::*;

    // 变参 `fcntl` 必须经 libc crate 调用：手工声明的变参 extern 在当前工具链上
    // 会丢失第三个实参（F_SETFL 静默失效，本 fixture 的非阻塞前提随之破坏）。
    // 定参符号 read/pipe/close 一并走 libc，保持声明单点。

    /// 创建非阻塞读端 pipe，返回 `(读端 fd, tokio 写端 File)`。
    fn pipe_fixture() -> (std::os::raw::c_int, tokio::fs::File) {
        let mut fds = [0 as std::os::raw::c_int; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0, "pipe(2) failed");
        let flags = unsafe { libc::fcntl(fds[0], libc::F_GETFL) };
        assert!(flags >= 0, "fcntl(F_GETFL) failed");
        assert_eq!(
            unsafe { libc::fcntl(fds[0], libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0,
            "fcntl(F_SETFL) failed"
        );
        // SAFETY: fds[1] 是 pipe(2) 新建的写端，所有权移入 File。
        let write_end = unsafe { std::fs::File::from_raw_fd(fds[1]) };
        (fds[0], tokio::fs::File::from_std(write_end))
    }

    /// 非阻塞读一次：pipe 上无数据/写端已关返回 `None`，否则返回已读字节。
    ///
    /// 注：pipe 空且写端已关时 `read` 返回 0（EOF），与 EAGAIN 一并视为无数据。
    fn read_once(fd: std::os::raw::c_int) -> Option<Vec<u8>> {
        let mut buf = [0u8; 16];
        // SAFETY: fd 有效且 buf 足以容纳一次读取。
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n > 0 {
            Some(buf[..n as usize].to_vec())
        } else {
            None
        }
    }

    fn spawn(
        write_end: tokio::fs::File,
        flag: &CancelFlag,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Receiver<Option<tokio::fs::File>>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let active = Arc::new(AtomicBool::new(true));
        let writing = Arc::new(AtomicBool::new(false));
        let (handle, rx) = ProcessTaskDispatcher::spawn_cancel_poller(
            Some(write_end),
            flag.clone(),
            active.clone(),
            writing.clone(),
        );
        (handle, rx, active, writing)
    }

    /// flag 置位且 active 为真：poller 必须写出完整 Cancel 帧，并通过 writing
    /// 标记如实上报「可能已写帧」。
    #[tokio::test]
    async fn poller_writes_cancel_frame_and_reports_written() {
        let (read_fd, write_end) = pipe_fixture();
        let flag = new_cancel_flag();
        flag.store(true, Ordering::Relaxed);
        let (handle, rx, _active, writing) = spawn(write_end, &flag);

        let (cancel_stdin, maybe_written) = tokio::time::timeout(Duration::from_secs(2), async {
            let cs = rx.await.expect("poller must hand back cancel_stdin");
            (cs, writing.load(Ordering::Acquire))
        })
        .await
        .expect("poller should finish within timeout");
        assert!(maybe_written, "writing flag must be set after Cancel write");
        assert!(
            cancel_stdin.is_some(),
            "poller must hand back the write end"
        );
        assert_eq!(
            read_once(read_fd).as_deref(),
            Some(&CANCEL_FRAME_BYTES[..]),
            "poller must write the exact 5-byte Cancel frame"
        );
        let _ = handle.await;
        unsafe { libc::close(read_fd) };
    }

    /// flag 置位但 active 已为假：poller 不得写帧，writing 保持 false。
    #[tokio::test]
    async fn poller_skips_write_when_session_inactive() {
        let (read_fd, write_end) = pipe_fixture();
        let flag = new_cancel_flag();
        flag.store(true, Ordering::Relaxed);
        let (handle, rx, active, writing) = spawn(write_end, &flag);
        active.store(false, Ordering::Relaxed);

        let (cancel_stdin, maybe_written) = tokio::time::timeout(Duration::from_secs(2), async {
            let cs = rx.await.expect("poller must hand back cancel_stdin");
            (cs, writing.load(Ordering::Acquire))
        })
        .await
        .expect("poller should finish within timeout");
        assert!(!maybe_written, "inactive session must not enter write path");
        assert!(
            cancel_stdin.is_some(),
            "poller must still hand back the write end"
        );
        assert!(read_once(read_fd).is_none(), "no frame may be written");
        let _ = handle.await;
        unsafe { libc::close(read_fd) };
    }

    /// 核心回归（循环压测）：`stop_cancel_poller` 返回 clean（maybe_written ==
    /// false）时，worker stdin（此处为 pipe）上绝不允许残留任何字节。
    ///
    /// 每轮在 poller 休眠窗口内声明会话结束并停止，覆盖「结果臂先于 poller
    /// 唤醒完成释放」的竞态路径。
    #[tokio::test]
    async fn stop_cancel_poller_clean_exit_leaves_no_stale_frame() {
        for _ in 0..200 {
            let (read_fd, write_end) = pipe_fixture();
            let flag = new_cancel_flag();
            let (canceller, rx, _active, writing) = spawn(write_end, &flag);

            // 结果臂：立即声明会话结束，随后必须等待 poller 退出。
            //（stop_cancel_poller 内部 abort + await，返回后 poller 不可能再写。）
            let (cancel_stdin, maybe_written) =
                ProcessTaskDispatcher::stop_cancel_poller(canceller, rx, &writing).await;
            drop(cancel_stdin);

            assert!(
                !maybe_written,
                "flag never set: poller must not have entered the write path"
            );
            assert_eq!(
                read_once(read_fd),
                None,
                "clean stop must leave no stale frame on the worker stdin"
            );
            unsafe { libc::close(read_fd) };
        }
    }

    /// 核心回归（循环压测，flag 置位变体）：停止前瞬时置位取消标志。无论 poller
    /// 是否抢在被 abort 前完成写帧，输出都必须自洽：
    /// `maybe_written == true` ⇒ pipe 上恰有一个完整 Cancel 帧；
    /// `maybe_written == false` ⇒ pipe 上没有任何字节。
    #[tokio::test]
    async fn stop_cancel_poller_reports_and_pipe_state_agree() {
        for i in 0..200 {
            let (read_fd, write_end) = pipe_fixture();
            let flag = new_cancel_flag();
            let (canceller, rx, _active, writing) = spawn(write_end, &flag);
            if i % 2 == 0 {
                flag.store(true, Ordering::Relaxed);
            }

            let (cancel_stdin, maybe_written) =
                ProcessTaskDispatcher::stop_cancel_poller(canceller, rx, &writing).await;
            drop(cancel_stdin);

            if maybe_written {
                assert_eq!(
                    read_once(read_fd).as_deref(),
                    Some(&CANCEL_FRAME_BYTES[..]),
                    "written report must correspond to a full Cancel frame on the pipe"
                );
            } else {
                assert_eq!(
                    read_once(read_fd),
                    None,
                    "clean report must correspond to an empty pipe"
                );
            }
            unsafe { libc::close(read_fd) };
        }
    }
}
