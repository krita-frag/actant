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
