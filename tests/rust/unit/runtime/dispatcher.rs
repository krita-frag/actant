//! Unit tests extracted from `src/runtime/dispatcher.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

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

// ───────────────────────── register_handler trait 方法测试 ─────────────────────────

#[tokio::test]
async fn register_handler_via_trait_method_succeeds_for_task_registry() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    let handler: TaskHandler = Arc::new(|_payload, _flag| Ok(b"via-trait".to_vec()));
    TaskDispatcher::register_handler(&registry, "trait-task", handler)
        .expect("register_handler should succeed on TaskRegistry");

    let result = registry
        .dispatch("trait-task", signed(b""), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"via-trait");
}

#[tokio::test]
async fn register_handler_default_impl_returns_error() {
    // 一个最小 dispatcher 仅实现 dispatch，register_handler 应使用默认实现返回错误。
    struct StubDispatcher;
    #[async_trait::async_trait]
    impl TaskDispatcher for StubDispatcher {
        async fn dispatch(
            &self,
            _name: &str,
            _payload: Vec<u8>,
            _cancel_flag: CancelFlag,
        ) -> crate::common::Result<Vec<u8>> {
            Ok(Vec::new())
        }
    }
    let stub = StubDispatcher;
    let handler: TaskHandler = Arc::new(|_p, _f| Ok(Vec::new()));
    let result = TaskDispatcher::register_handler(&stub, "x", handler);
    assert!(
        matches!(result, Err(ActantError::Internal(_))),
        "default register_handler should return Internal error"
    );
}

#[tokio::test]
async fn shutdown_default_impl_is_noop() {
    struct StubDispatcher;
    #[async_trait::async_trait]
    impl TaskDispatcher for StubDispatcher {
        async fn dispatch(
            &self,
            _name: &str,
            _payload: Vec<u8>,
            _cancel_flag: CancelFlag,
        ) -> crate::common::Result<Vec<u8>> {
            Ok(Vec::new())
        }
    }
    let stub = StubDispatcher;
    // 默认 shutdown 应不 panic。
    TaskDispatcher::shutdown(&stub);
}

// ───────────────────────── TaskRegistry 方法测试 ─────────────────────────

#[test]
fn with_drain_timeout_creates_registry() {
    let registry = TaskRegistry::with_drain_timeout(
        2,
        16,
        TEST_KEY.to_vec(),
        std::time::Duration::from_secs(1),
    )
    .expect("with_drain_timeout should succeed");
    // 通过 dispatch 间接验证 registry 可用。
    registry.register("echo", |_p, _f| Ok(b"ok".to_vec()));
}

#[test]
fn with_drain_timeout_zero_workers_returns_error() {
    let result = TaskRegistry::with_drain_timeout(
        0,
        16,
        TEST_KEY.to_vec(),
        std::time::Duration::from_secs(1),
    );
    assert!(result.is_err());
}

#[test]
fn into_dispatcher_returns_arc() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    let _arc: Arc<dyn TaskDispatcher> = registry.into_dispatcher();
}

#[tokio::test]
async fn shutdown_closes_thread_pool() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register("echo", |_p, _f| Ok(b"ok".to_vec()));
    registry.shutdown();
    // shutdown 后 dispatch 应失败（线程池已关闭）。
    let err = registry
        .dispatch("echo", signed(b""), new_cancel_flag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(_)),
        "dispatch after shutdown should fail"
    );
}

// ───────────────────────── 边缘场景测试 ─────────────────────────

#[tokio::test]
async fn dispatch_with_generic_name_uses_generic_handler_directly() {
    // 当 name == GENERIC_DISPATCH_NAME 时，应直接查找 generic handler 而非递归回退。
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register(GENERIC_DISPATCH_NAME, |_p, _f| {
        Ok(b"generic-direct".to_vec())
    });

    let result = registry
        .dispatch(GENERIC_DISPATCH_NAME, signed(b""), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"generic-direct");
}

#[tokio::test]
async fn dispatch_with_generic_name_and_no_handler_returns_error() {
    // GENERIC_DISPATCH_NAME 未注册时直接调用应报错而非无限递归。
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    let err = registry
        .dispatch(GENERIC_DISPATCH_NAME, signed(b""), new_cancel_flag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(_)),
        "dispatch GENERIC_DISPATCH_NAME without handler should error"
    );
}

#[tokio::test]
async fn dispatch_handler_receives_verified_payload() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register("echo-payload", |payload, _flag| Ok(payload));

    let original = b"hello-payload";
    let result = registry
        .dispatch("echo-payload", signed(original), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, original);
}

#[tokio::test]
async fn dispatch_handler_panic_with_string_payload_produces_error() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register("string-panicker", |_p, _f| {
        panic!("string panic message");
    });

    let err = registry
        .dispatch("string-panicker", signed(b""), new_cancel_flag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(ref m) if m.contains("string panic message")),
        "panic message should be captured, got: {:?}",
        err
    );
}

#[tokio::test]
async fn dispatch_handler_panic_with_unknown_type_produces_generic_error() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register("unknown-panicker", |_p, _f| {
        std::panic::panic_any(12345i32); // 非 &str / String 的 panic payload
    });

    let err = registry
        .dispatch("unknown-panicker", signed(b""), new_cancel_flag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(ref m) if m.contains("unknown panic")),
        "unknown panic type should produce generic message, got: {:?}",
        err
    );
}

#[tokio::test]
async fn register_overwrites_existing_handler() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register("task", |_p, _f| Ok(b"v1".to_vec()));
    registry.register("task", |_p, _f| Ok(b"v2".to_vec()));

    let result = registry
        .dispatch("task", signed(b""), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"v2");
}

#[tokio::test]
async fn register_handler_via_trait_overwrites_existing() {
    let registry = TaskRegistry::new(1, 8, TEST_KEY.to_vec()).unwrap();
    registry.register("task", |_p, _f| Ok(b"original".to_vec()));

    let handler: TaskHandler = Arc::new(|_p, _f| Ok(b"replaced".to_vec()));
    TaskDispatcher::register_handler(&registry, "task", handler).unwrap();

    let result = registry
        .dispatch("task", signed(b""), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"replaced");
}

#[tokio::test]
async fn dispatch_with_empty_signing_key_skips_verification() {
    // 空签名密钥应禁用 payload 验证（开发/测试模式）。
    let registry = TaskRegistry::new(1, 8, Vec::new()).unwrap();
    registry.register("echo", |_p, _f| Ok(b"ok".to_vec()));

    // 未签名的 payload 应能通过。
    let result = registry
        .dispatch("echo", b"raw-unsigned".to_vec(), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"ok");
}

// ───────────────────────── tx.send 失败路径测试（H1）─────────────────────────
//
// 以下测试覆盖 dispatcher.rs 中 `tx.send(...).is_err()` 三个分支：
// 当 dispatch future 在 handler 完成前被 drop（例如上层 tokio 任务被 abort），
// oneshot::Receiver 被释放，handler 完成时 `tx.send()` 返回 `Err`。
// 此时 dispatcher 仅记录 warn 日志，不应 panic 或污染线程池。
//
// 实现策略：使用 `tokio::time::timeout` 在 handler 完成前 drop dispatch future
// （释放 rx），然后等待足够长时间让 handler 完成（命中 tx.send().is_err() 路径），
// 最后再次 dispatch 验证线程池仍然可用。
// 注意：不能使用 `Barrier::wait()` 同步，因为验证性 dispatch 会再次调用 handler，
// 导致 barrier 在无第二个参与者时永久阻塞。

#[tokio::test]
async fn dispatch_dropped_future_with_ok_handler_does_not_corrupt_registry() {
    // 覆盖 `tx.send(Ok(value)).is_err()` 分支。
    let registry = TaskRegistry::new(2, 8, TEST_KEY.to_vec()).unwrap();

    registry.register("slow-ok", move |_payload, _flag| {
        std::thread::sleep(std::time::Duration::from_millis(80));
        // future 被 drop 后，tx.send(Ok(..)) 返回 Err。
        Ok(b"done".to_vec())
    });

    // timeout 在 20ms 时 drop dispatch future（rx 被释放）。
    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        registry.dispatch("slow-ok", signed(b""), new_cancel_flag()),
    )
    .await;
    assert!(timed_out.is_err(), "dispatch should time out");

    // 等待 handler 完成（80ms sleep + 余量），命中 tx.send().is_err() 路径。
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    // 线程池应仍然可用——验证性 dispatch 不会阻塞。
    let result = registry
        .dispatch("slow-ok", signed(b""), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"done");
}

#[tokio::test]
async fn dispatch_dropped_future_with_err_handler_does_not_corrupt_registry() {
    // 覆盖 `tx.send(Err(e)).is_err()` 分支。
    let registry = TaskRegistry::new(2, 8, TEST_KEY.to_vec()).unwrap();

    registry.register("slow-err", move |_payload, _flag| {
        std::thread::sleep(std::time::Duration::from_millis(80));
        Err(ActantError::Worker("late error".into()))
    });

    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        registry.dispatch("slow-err", signed(b""), new_cancel_flag()),
    )
    .await;
    assert!(timed_out.is_err(), "dispatch should time out");

    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    // 线程池应仍然可用——handler 返回 Err 被正常传播给调用方。
    let err = registry
        .dispatch("slow-err", signed(b""), new_cancel_flag())
        .await
        .unwrap_err();
    assert!(matches!(err, ActantError::Worker(_)));
}

#[tokio::test]
async fn dispatch_dropped_future_with_panic_handler_does_not_kill_worker() {
    // 覆盖 `tx.send(Err(panic)).is_err()` 分支。
    // 关键断言：catch_unwind 捕获 panic 后 worker 线程应存活。
    let registry = Arc::new(TaskRegistry::new(2, 8, TEST_KEY.to_vec()).unwrap());
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));

    registry.register("slow-panic", {
        let panicked = panicked.clone();
        move |_payload, _flag| {
            std::thread::sleep(std::time::Duration::from_millis(80));
            panicked.store(true, std::sync::atomic::Ordering::SeqCst);
            panic!("late panic after future drop");
        }
    });

    let timed_out = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        TaskDispatcher::dispatch(&*registry, "slow-panic", signed(b""), new_cancel_flag()),
    )
    .await;
    assert!(timed_out.is_err(), "dispatch should time out");

    // 等待 handler 完成（sleep + panic），catch_unwind 捕获 panic。
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    assert!(
        panicked.load(std::sync::atomic::Ordering::SeqCst),
        "handler should have panicked"
    );

    // 注册一个独立 handler 验证 worker 线程仍然存活。
    registry.register("healthy", |_p, _f| Ok(b"alive".to_vec()));
    let result = registry
        .dispatch("healthy", signed(b""), new_cancel_flag())
        .await
        .unwrap();
    assert_eq!(result, b"alive");
}

// ───────────────────────── 线程池容量与签名禁用边界测试（H1）─────────────────────────

#[tokio::test]
async fn dispatch_when_pool_at_capacity_returns_error() {
    // 覆盖 submit() 的 `TrySendError::Full` 分支。
    // 线程池 1 worker + channel capacity 1：最多 2 个 in-flight 任务（1 运行 + 1 排队）。
    let registry = TaskRegistry::new(1, 1, TEST_KEY.to_vec()).unwrap();

    registry.register("blocking", |_payload, _flag| {
        // 睡眠足够长以确保测试期间 worker 被占用。
        std::thread::sleep(std::time::Duration::from_millis(300));
        Ok(b"done".to_vec())
    });

    let registry = Arc::new(registry);

    // 第一次 dispatch：启动并占用 worker。
    let r1 = registry.clone();
    tokio::spawn(async move {
        let _ = TaskDispatcher::dispatch(&*r1, "blocking", signed(b""), new_cancel_flag()).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // 第二次 dispatch：应进入 channel 队列（capacity 1）。
    let r2 = registry.clone();
    tokio::spawn(async move {
        let _ = TaskDispatcher::dispatch(&*r2, "blocking", signed(b""), new_cancel_flag()).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // 第三次 dispatch：channel 已满，submit 应返回 Full 错误。
    let err = registry
        .dispatch("blocking", signed(b""), new_cancel_flag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(ref m) if m.contains("at capacity")),
        "expected capacity error, got: {:?}",
        err
    );

    // 等待排队的任务完成，确保 worker 线程能干净退出。
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
}

#[tokio::test]
async fn dispatch_with_empty_key_rejects_signed_payload() {
    // 覆盖 verify() 在空 key + MAC_PREFIX 数据时返回错误的路径。
    // 空 key 禁用签名，但若 payload 看起来已被签名（以 MAC_PREFIX 开头），
    // 应拒绝以避免在禁用签名的节点上误处理本应签名的 payload。
    let registry = TaskRegistry::new(1, 8, Vec::new()).unwrap();
    registry.register("echo", |_p, _f| Ok(b"ok".to_vec()));

    // 用非空 key 签名一个 payload，然后用空 key 的 registry dispatch。
    let signed_payload = crate::common::payload::sign(b"some-key", b"data").unwrap();

    let err = registry
        .dispatch("echo", signed_payload, new_cancel_flag())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ActantError::Internal(ref m)
            if m.contains("signing disabled but payload appears signed")),
        "expected signing-disabled rejection, got: {:?}",
        err
    );
}
