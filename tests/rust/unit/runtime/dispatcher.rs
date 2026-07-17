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
