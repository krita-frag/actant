//! Unit tests extracted from `src/runtime/capability/actor.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::*;
use crate::common::{ActorId, ActorMessage};
use crate::runtime::capability::actor::CapabilityActor;
use crate::runtime::capability::{
    Capability, CapabilityCodec, CapabilityEnvelope, EffectKind, ErasedHandler, TypedCodec,
};

#[derive(Clone, Default)]
struct Ping;
impl Capability for Ping {
    type Request = String;
    type Response = String;
}

fn make_codec() -> Arc<dyn CapabilityCodec> {
    Arc::new(TypedCodec::<Ping>::new())
}

fn make_message(kind: EffectKind, payload: &str) -> ActorMessage {
    let req = payload.to_string();
    let bytes = postcard::to_allocvec(&req).unwrap();
    let envelope = CapabilityEnvelope {
        kind,
        payload: bytes,
    };
    ActorMessage::new(
        ActorId::from("cap-target"),
        "handle".into(),
        postcard::to_allocvec(&envelope).unwrap(),
    )
}

#[tokio::test]
async fn ask_returns_first_some_response() {
    let handler = {
        struct H;
        #[async_trait::async_trait]
        impl ErasedHandler for H {
            async fn ask(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
                Some(Box::new("pong".to_string()))
            }
            async fn perform(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn emit(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<()> {
                unreachable!()
            }
        }
        Arc::new(H)
    };

    let mut actor = CapabilityActor::new(vec![handler], make_codec());
    let result = actor
        .handle_message(make_message(EffectKind::Ask, "ping"))
        .await
        .unwrap();
    assert!(result.error.is_none());
    assert_eq!(result.payload[0], 1); // 有返回值的标记
    let resp: String = postcard::from_bytes(&result.payload[1..]).unwrap();
    assert_eq!(resp, "pong");
}

#[tokio::test]
async fn ask_returns_none_when_all_handlers_return_none() {
    let handler = {
        struct H;
        #[async_trait::async_trait]
        impl ErasedHandler for H {
            async fn ask(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
                None
            }
            async fn perform(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn emit(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<()> {
                unreachable!()
            }
        }
        Arc::new(H)
    };

    let mut actor = CapabilityActor::new(vec![handler], make_codec());
    let result = actor
        .handle_message(make_message(EffectKind::Ask, "ping"))
        .await
        .unwrap();
    assert!(result.error.is_none());
    assert_eq!(result.payload, vec![0u8]);
}

#[tokio::test]
async fn perform_returns_last_handler_result() {
    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();
    let handler = {
        struct H(Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl ErasedHandler for H {
            async fn ask(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn perform(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
                self.0.store(true, Ordering::SeqCst);
                Ok(Box::new("performed".to_string()))
            }
            async fn emit(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<()> {
                unreachable!()
            }
        }
        Arc::new(H(called_clone))
    };

    let mut actor = CapabilityActor::new(vec![handler], make_codec());
    let result = actor
        .handle_message(make_message(EffectKind::Perform, "ping"))
        .await
        .unwrap();
    assert!(called.load(Ordering::SeqCst));
    assert!(result.error.is_none());
    let resp: String = postcard::from_bytes(&result.payload).unwrap();
    assert_eq!(resp, "performed");
}

#[tokio::test]
async fn perform_without_handlers_returns_error() {
    let mut actor = CapabilityActor::new(vec![], make_codec());
    let result = actor
        .handle_message(make_message(EffectKind::Perform, "ping"))
        .await
        .unwrap();
    assert!(result.error.is_some());
}

#[tokio::test]
async fn emit_calls_all_handlers() {
    let called_a = Arc::new(AtomicBool::new(false));
    let called_b = Arc::new(AtomicBool::new(false));
    let handler_a = {
        struct H(Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl ErasedHandler for H {
            async fn ask(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn perform(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn emit(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<()> {
                self.0.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
        Arc::new(H(called_a))
    };
    let handler_b = {
        struct H(Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl ErasedHandler for H {
            async fn ask(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn perform(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
                unreachable!()
            }
            async fn emit(
                &self,
                _req: Arc<dyn std::any::Any + Send + Sync>,
            ) -> crate::common::Result<()> {
                self.0.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
        Arc::new(H(called_b))
    };

    let mut actor = CapabilityActor::new(vec![handler_a, handler_b], make_codec());
    let result = actor
        .handle_message(make_message(EffectKind::Emit, "ping"))
        .await
        .unwrap();
    assert!(result.error.is_none());
    assert!(result.payload.is_empty());
}

#[tokio::test]
async fn invalid_envelope_payload_returns_error() {
    let mut actor = CapabilityActor::new(vec![], make_codec());
    let msg = ActorMessage::new(ActorId::from("cap-target"), "handle".into(), vec![255u8; 8]);
    let result = actor.handle_message(msg).await.unwrap();
    assert!(result.error.is_some());
}

// emit 反应型语义：单个 handler 失败不中断后续 handler 调用，
// 全部调用完成后聚合失败信息回给调用方。
#[tokio::test]
async fn emit_handler_error_calls_remaining_handlers_and_aggregates_error() {
    struct FailingHandler;
    #[async_trait::async_trait]
    impl ErasedHandler for FailingHandler {
        async fn ask(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn perform(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn emit(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<()> {
            Err(crate::common::ActantError::Internal(
                "emit handler failed".into(),
            ))
        }
    }

    let called = Arc::new(AtomicBool::new(false));
    struct OkHandler(Arc<AtomicBool>);
    #[async_trait::async_trait]
    impl ErasedHandler for OkHandler {
        async fn ask(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn perform(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn emit(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<()> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    let ok_handler = Arc::new(OkHandler(called.clone()));

    let mut actor = CapabilityActor::new(vec![Arc::new(FailingHandler), ok_handler], make_codec());
    let result = actor
        .handle_message(make_message(EffectKind::Emit, "ping"))
        .await
        .unwrap();
    // 后续 handler 仍被调用
    assert!(called.load(Ordering::SeqCst), "remaining handler must run");
    // 调用方收到聚合错误
    let error = result.error.expect("aggregated emit error expected");
    assert!(error.message.contains("emit handler failed"));
    assert!(error.message.contains("1/2 handlers failed"));
}

// H7.3：emit 聚合错误 kind 保真——首个失败 handler 的 kind 编码进错误消息
// 前缀（`[actant:storage] ...`），Python 侧 decode_error_kind 据此重建
// 对应异常子类，而非统一退化为 internal/task。
#[tokio::test]
async fn emit_aggregate_error_preserves_first_failure_kind() {
    struct StorageFailingHandler;
    #[async_trait::async_trait]
    impl ErasedHandler for StorageFailingHandler {
        async fn ask(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn perform(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn emit(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<()> {
            Err(crate::common::ActantError::Storage("disk on fire".into()))
        }
    }
    struct InternalFailingHandler;
    #[async_trait::async_trait]
    impl ErasedHandler for InternalFailingHandler {
        async fn ask(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn perform(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<Box<dyn std::any::Any + Send + Sync>> {
            unreachable!()
        }
        async fn emit(
            &self,
            _req: Arc<dyn std::any::Any + Send + Sync>,
        ) -> crate::common::Result<()> {
            Err(crate::common::ActantError::Internal("later failure".into()))
        }
    }

    // 首个失败为 Storage，后续失败为 Internal：前缀必须取首个（storage）。
    let mut actor = CapabilityActor::new(
        vec![
            Arc::new(StorageFailingHandler),
            Arc::new(InternalFailingHandler),
        ],
        make_codec(),
    );
    let result = actor
        .handle_message(make_message(EffectKind::Emit, "ping"))
        .await
        .unwrap();
    let error = result.error.expect("aggregated emit error expected");
    assert!(
        error.message.starts_with("[actant:storage] "),
        "aggregate error must carry first failure kind prefix, got: {}",
        error.message
    );
    assert!(error.message.contains("2/2 handlers failed"));
    assert!(error.message.contains("disk on fire"));
}
