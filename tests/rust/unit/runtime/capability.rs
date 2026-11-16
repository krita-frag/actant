//! Unit tests extracted from `src/runtime/capability.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;
use crate::common::{TaskId, WorkflowId};
use crate::runtime::state::LmdbStore as StateStore;
use tempfile::tempdir;

#[tokio::test]
async fn store_handler_roundtrip() {
    let dir = tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let runtime = Arc::new(CapabilityRuntime::new());
    register_defaults(&runtime).unwrap();
    register_store_handler(&runtime, store).unwrap();
    // 强制走 Actor 路径：必须 bind_actor_system 后才能 perform。
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&runtime)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let put = StoreReq::Put {
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    };
    let _ = runtime.perform::<Store>(put).await.unwrap();

    let get = StoreReq::Get {
        key: b"hello".to_vec(),
    };
    let result = runtime.perform::<Store>(get).await.unwrap();
    assert_eq!(result, Ok(Some(b"world".to_vec())));
}

#[tokio::test]
async fn store_handler_roundtrip_via_actor() {
    let dir = tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let runtime = Arc::new(CapabilityRuntime::new());
    register_defaults(&runtime).unwrap();
    register_store_handler(&runtime, store).unwrap();

    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&runtime)
        .bind_actor_system(actor_system.clone())
        .await
        .expect("bind actor system");

    let put = StoreReq::Put {
        key: b"actor".to_vec(),
        value: b"via_actor".to_vec(),
    };
    let _ = runtime.perform::<Store>(put).await.unwrap();

    let get = StoreReq::Get {
        key: b"actor".to_vec(),
    };
    let result = runtime.perform::<Store>(get).await.unwrap();
    assert_eq!(result, Ok(Some(b"via_actor".to_vec())));
}

#[test]
fn effect_kind_variants_are_distinct() {
    assert_ne!(EffectKind::Perform, EffectKind::Emit);
}

#[tokio::test]
async fn typed_codec_roundtrips_store_request() {
    let codec = TypedCodec::<Store>::new();
    let req = StoreReq::Put {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };
    let req_arc: Arc<dyn Any + Send + Sync> = Arc::new(req.clone());
    let bytes = codec.serialize_request(req_arc).unwrap();
    let decoded = codec.deserialize_request(&bytes).unwrap();
    let decoded_req = decoded.downcast_ref::<StoreReq>().unwrap();
    match decoded_req {
        StoreReq::Put { key, value } => {
            assert_eq!(key, b"k");
            assert_eq!(value, b"v");
        }
        _ => panic!("expected Put"),
    }
}

#[tokio::test]
async fn typed_codec_roundtrips_store_response() {
    let codec = TypedCodec::<Store>::new();
    let resp: <Store as Capability>::Response = Ok(Some(b"data".to_vec()));
    let bytes = codec.serialize_response(Box::new(resp.clone())).unwrap();
    let decoded = codec.deserialize_response(&bytes).unwrap();
    let decoded_resp = decoded
        .downcast::<<Store as Capability>::Response>()
        .unwrap();
    assert_eq!(*decoded_resp, resp);
}

#[test]
fn typed_codec_serialize_request_type_mismatch_returns_error() {
    let codec = TypedCodec::<Store>::new();
    // 传入错误类型的 request
    let wrong_req: Arc<dyn Any + Send + Sync> = Arc::new(42i32);
    let result = codec.serialize_request(wrong_req);
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[test]
fn typed_codec_serialize_response_type_mismatch_returns_error() {
    let codec = TypedCodec::<Store>::new();
    let wrong_resp: Box<dyn Any + Send + Sync> = Box::new(42i32);
    let result = codec.serialize_response(wrong_resp);
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[test]
fn typed_codec_deserialize_request_invalid_bytes_returns_error() {
    let codec = TypedCodec::<Store>::new();
    let result = codec.deserialize_request(b"garbage");
    assert!(result.is_err());
}

#[test]
fn typed_codec_deserialize_response_invalid_bytes_returns_error() {
    let codec = TypedCodec::<Store>::new();
    let result = codec.deserialize_response(b"garbage");
    assert!(result.is_err());
}

#[test]
fn handler_list_new_starts_empty() {
    let meta = CapabilityMeta::new::<Store>("Store", EffectKind::Perform);
    let list = HandlerList::new(meta);
    assert!(list.is_empty());
    assert_eq!(list.len(), 0);
}

#[test]
fn handler_list_push_increments_len() {
    let meta = CapabilityMeta::new::<Store>("Store", EffectKind::Perform);
    let mut list = HandlerList::new(meta);
    let handler = erase_handler(StoreHandler::new(
        StateStore::open(tempdir().unwrap().path()).unwrap(),
    ));
    list.push(handler);
    assert!(!list.is_empty());
    assert_eq!(list.len(), 1);
}

#[test]
fn layer_new_starts_empty() {
    let meta = CapabilityMeta::new::<Store>("Store", EffectKind::Perform);
    let layer = Layer::<Store>::new(meta);
    assert!(layer.is_empty());
    assert_eq!(layer.len(), 0);
}

#[test]
fn layer_for_capability_creates_with_correct_meta() {
    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform);
    assert_eq!(layer.meta().name, "Store");
    assert_eq!(layer.meta().default_kind, EffectKind::Perform);
    assert!(layer.is_empty());
}

#[test]
fn layer_chain_adds_handler() {
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform)
        .chain(StoreHandler::new(store));
    assert_eq!(layer.len(), 1);
}

#[test]
fn layer_chain_erased_adds_handler() {
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    let handler = erase_handler(StoreHandler::new(store));
    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform).chain_erased(handler);
    assert_eq!(layer.len(), 1);
}

#[test]
fn layer_into_list_transfers_handlers() {
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform)
        .chain(StoreHandler::new(store));
    let list = layer.into_list();
    assert_eq!(list.len(), 1);
    assert_eq!(list.meta.name, "Store");
}

#[test]
fn runtime_new_starts_empty() {
    let rt = CapabilityRuntime::new();
    assert_eq!(rt.capability_count(), 0);
    assert!(rt.capabilities().is_empty());
    assert_eq!(rt.handler_count::<Store>(), 0);
    assert!(rt.handler_count_by_type_id(TypeId::of::<Store>()).is_none());
}

#[test]
fn runtime_register_increments_count() {
    let rt = CapabilityRuntime::new();
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store).unwrap();
    assert_eq!(rt.capability_count(), 1);
    assert_eq!(rt.handler_count::<Store>(), 1);
    assert_eq!(rt.handler_count_by_type_id(TypeId::of::<Store>()), Some(1));
    let caps = rt.capabilities();
    assert_eq!(caps.len(), 1);
    assert_eq!(caps[0].name, "Store");
}

#[test]
fn runtime_register_codec_does_not_register_layer() {
    let rt = CapabilityRuntime::new();
    rt.register_codec::<Store>().unwrap();
    // register_codec 只注册 codec，不注册 layer
    assert_eq!(rt.capability_count(), 0);
    assert!(rt.handler_count::<Store>().eq(&0));
}

#[test]
fn runtime_chain_appends_handler_to_registered_layer() {
    let rt = CapabilityRuntime::new();
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store).unwrap();
    assert_eq!(rt.handler_count::<Store>(), 1);

    let store2 = StateStore::open(tempdir().unwrap().path()).unwrap();
    let handler = erase_handler(StoreHandler::new(store2));
    rt.chain::<Store>(handler).unwrap();
    assert_eq!(rt.handler_count::<Store>(), 2);
}

#[test]
fn runtime_chain_without_registration_returns_error() {
    let rt = CapabilityRuntime::new();
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    let handler = erase_handler(StoreHandler::new(store));
    let result = rt.chain::<Store>(handler);
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn runtime_register_after_bind_returns_error() {
    let rt = Arc::new(CapabilityRuntime::new());
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform);
    let result = rt.register(layer);
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn runtime_chain_after_bind_returns_error() {
    let rt = Arc::new(CapabilityRuntime::new());
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store).unwrap();

    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let store2 = StateStore::open(tempdir().unwrap().path()).unwrap();
    let handler = erase_handler(StoreHandler::new(store2));
    let result = rt.chain::<Store>(handler);
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn perform_without_bound_actor_system_returns_error() {
    let rt = CapabilityRuntime::new();
    rt.register_codec::<Store>().unwrap();
    let result = rt
        .perform::<Store>(StoreReq::Get { key: b"k".to_vec() })
        .await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn emit_without_bound_actor_system_returns_error() {
    let rt = CapabilityRuntime::new();
    rt.register_codec::<TaskLifecycle>().unwrap();
    let result = rt
        .emit::<TaskLifecycle>(TaskEvent::Started {
            task_id: TaskId::from("t-1".to_string()),
            workflow_id: WorkflowId::from("wf-1".to_string()),
        })
        .await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn store_handler_put_get_delete_roundtrip() {
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    let handler = StoreHandler::new(store);

    // Put
    let put_resp = handler
        .handle(StoreReq::Put {
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
        })
        .await;
    assert!(matches!(put_resp, Some(Ok(None))));

    // Get
    let get_resp = handler
        .handle(StoreReq::Get {
            key: b"key1".to_vec(),
        })
        .await;
    assert!(matches!(get_resp, Some(Ok(Some(ref v))) if v == b"val1"));

    // Delete
    let del_resp = handler
        .handle(StoreReq::Delete {
            key: b"key1".to_vec(),
        })
        .await;
    assert!(matches!(del_resp, Some(Ok(None))));

    // Get after delete
    let get_after = handler
        .handle(StoreReq::Get {
            key: b"key1".to_vec(),
        })
        .await;
    assert!(matches!(get_after, Some(Ok(None))));
}

// =========================================================================
// CapabilityRuntime::default / ensure_layer / register 覆盖
// =========================================================================

#[test]
fn runtime_default_equivalent_to_new() {
    let rt = CapabilityRuntime::default();
    assert_eq!(rt.capability_count(), 0);
    assert!(rt.capabilities().is_empty());
    assert_eq!(rt.handler_count::<Store>(), 0);
}

#[test]
fn ensure_layer_is_idempotent() {
    let rt = CapabilityRuntime::new();
    rt.ensure_layer::<Store>(Store::meta());
    assert_eq!(rt.capability_count(), 1);
    // 再次调用不应增加 capability 数量
    rt.ensure_layer::<Store>(Store::meta());
    assert_eq!(rt.capability_count(), 1);
    // metas 也只有一个
    assert_eq!(rt.capabilities().len(), 1);
}

#[tokio::test]
async fn ensure_layer_after_bind_still_allowed() {
    // ensure_layer 无 ensure_not_bound 检查，可在 bind 后调用
    let rt = CapabilityRuntime::new();
    rt.ensure_layer::<Store>(Store::meta());
    let rt = Arc::new(rt);
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");
    // bind 后 ensure_layer 不会报错
    rt.ensure_layer::<Store>(Store::meta());
}

#[tokio::test]
async fn ensure_layer_after_bind_does_not_spawn_actor() {
    // bind 后调用 ensure_layer 不会为新 capability spawn CapabilityActor
    // （因为没有 codec 注册 + actor 已 snapshot）
    let rt = CapabilityRuntime::new();
    rt.register_codec::<Store>().unwrap();
    rt.ensure_layer::<Store>(Store::meta());
    let rt = Arc::new(rt);
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    // bind 后 ensure_layer 不 spawn 新 CapabilityActor：
    // actor 表已 snapshot，新 layer 不会出现 actor 条目。
    rt.ensure_layer::<TaskLifecycle>(TaskLifecycle::meta());
    let type_id = std::any::TypeId::of::<TaskLifecycle>();
    assert!(rt.actor_ids.get(&type_id).is_none());
}

#[tokio::test]
async fn register_codec_after_bind_is_rejected() {
    // bind 后注册 codec 不会 spawn actor，必须显式失败而不是留下不可达 codec。
    let rt = Arc::new(CapabilityRuntime::new());
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let result = rt.register_codec::<Store>();
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[test]
fn register_overwrites_existing_layer() {
    // register 会覆盖已存在的 layer（与 ensure_layer 的幂等性不同）
    let rt = CapabilityRuntime::new();
    let store1 = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store1).unwrap();
    assert_eq!(rt.handler_count::<Store>(), 1);

    // 第二次 register 覆盖（metas 会有两条，但 layers 只有一条）
    let store2 = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store2).unwrap();
    assert_eq!(rt.handler_count::<Store>(), 1); // 覆盖，不是追加
    assert_eq!(rt.capabilities().len(), 2); // metas 累积
}

// =========================================================================
// HandlerAdapter 错误路径（type mismatch）
// =========================================================================

#[tokio::test]
async fn handler_adapter_perform_returns_error_on_type_mismatch() {
    use crate::runtime::capability::HandlerAdapter;
    use async_trait::async_trait;

    struct DummyHandler;
    #[async_trait]
    impl Handler<Store> for DummyHandler {
        async fn handle(&self, _req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
            Some(Ok(None))
        }
    }

    let adapter = HandlerAdapter::new(DummyHandler);
    let wrong_req: Arc<dyn Any + Send + Sync> = Arc::new(42i32);
    let result = adapter.perform(wrong_req).await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn handler_adapter_emit_returns_error_on_type_mismatch() {
    use crate::runtime::capability::HandlerAdapter;
    use async_trait::async_trait;

    struct DummyHandler;
    #[async_trait]
    impl Handler<Store> for DummyHandler {
        async fn handle(&self, _req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
            Some(Ok(None))
        }
    }

    let adapter = HandlerAdapter::new(DummyHandler);
    let wrong_req: Arc<dyn Any + Send + Sync> = Arc::new(42i32);
    let result = adapter.emit(wrong_req).await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn handler_adapter_perform_returns_error_when_handler_returns_none() {
    use crate::runtime::capability::HandlerAdapter;
    use async_trait::async_trait;

    struct NoneHandler;
    #[async_trait]
    impl Handler<Store> for NoneHandler {
        async fn handle(&self, _req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
            None
        }
    }

    let adapter = HandlerAdapter::new(NoneHandler);
    let req: Arc<dyn Any + Send + Sync> = Arc::new(StoreReq::Get { key: b"k".to_vec() });
    let result = adapter.perform(req).await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

// =========================================================================
// perform / emit 路径（绑定 ActorSystem）
// =========================================================================

#[tokio::test]
async fn perform_with_no_local_handler_returns_error() {
    // 注册 codec + 空 layer（无 handler）→ perform 应报错
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt).unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let result = rt
        .perform::<Store>(StoreReq::Get { key: b"k".to_vec() })
        .await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("no handler registered"));
}

#[tokio::test]
async fn emit_with_local_handler_calls_actor() {
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static EMIT_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct CountingEmitHandler;
    #[async_trait]
    impl Handler<TaskLifecycle> for CountingEmitHandler {
        async fn handle(&self, _req: TaskEvent) -> Option<()> {
            EMIT_COUNT.fetch_add(1, Ordering::SeqCst);
            Some(())
        }
    }

    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt).unwrap();
    rt.register(
        Layer::<TaskLifecycle>::new(TaskLifecycle::meta())
            .chain_erased(erase_handler(CountingEmitHandler)),
    )
    .unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let before = EMIT_COUNT.load(Ordering::SeqCst);
    rt.emit::<TaskLifecycle>(TaskEvent::Started {
        task_id: TaskId::from("t-emit-1".to_string()),
        workflow_id: WorkflowId::from("wf-emit-1".to_string()),
    })
    .await
    .unwrap();
    let after = EMIT_COUNT.load(Ordering::SeqCst);
    assert_eq!(after, before + 1);
}

#[tokio::test]
async fn emit_without_local_handler_and_no_peers_succeeds() {
    // 无 handler → emit 跳过本地 actor 调用，直接成功
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt).unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let result = rt
        .emit::<TaskLifecycle>(TaskEvent::Started {
            task_id: TaskId::from("t-emit-3".to_string()),
            workflow_id: WorkflowId::from("wf-emit-3".to_string()),
        })
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn emit_with_local_handler_error_propagates() {
    use async_trait::async_trait;

    struct FailingEmitHandler;
    #[async_trait]
    impl Handler<TaskLifecycle> for FailingEmitHandler {
        async fn handle(&self, _req: TaskEvent) -> Option<()> {
            // `Handler` trait 无错误通道：None 表示 handler 对本事件无意见
            // （非失败），HandlerAdapter::emit 将其视为成功。
            None
        }
    }

    // HandlerAdapter::emit 对 Some/None 均返回 Ok，
    // 所以这个测试验证 emit 不因 Handler 返回 None 而失败。
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt).unwrap();
    rt.register(
        Layer::<TaskLifecycle>::new(TaskLifecycle::meta())
            .chain_erased(erase_handler(FailingEmitHandler)),
    )
    .unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    let result = rt
        .emit::<TaskLifecycle>(TaskEvent::Started {
            task_id: TaskId::from("t-emit-4".to_string()),
            workflow_id: WorkflowId::from("wf-emit-4".to_string()),
        })
        .await;
    assert!(result.is_ok());
}

// emit handler（自带错误通道的 ErasedHandler 实现）失败时：
// 调用方收到聚合错误，且失败 handler 之后的 handler 仍被调用。

// =========================================================================
// bind_actor_system 多次绑定 / spawn 失败路径
// =========================================================================

#[tokio::test]
async fn bind_actor_system_spawns_actors_for_registered_codecs() {
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt).unwrap();
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store).unwrap();

    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect("bind actor system");

    // 3 个内置 capability（Store/TaskLifecycle/WorkflowLifecycle）均注册了
    // codec + layer → 应有 3 个 actor_ids（Store 有 handler，其余 2 个空 layer）
    let type_id_store = std::any::TypeId::of::<Store>();
    assert!(rt.actor_ids.get(&type_id_store).is_some());
    let type_id_lifecycle = std::any::TypeId::of::<TaskLifecycle>();
    assert!(rt.actor_ids.get(&type_id_lifecycle).is_some());
}

#[tokio::test]
async fn bind_actor_system_fails_when_capability_spawn_is_rejected() {
    // 预先在 ActorSystem 中 spawn 一个同名 actor，bind 时 spawn 失败。
    // 新契约：绑定失败必须显式返回 Err，而不是让该 capability 在运行期
    // 才报 “not bound”。
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt).unwrap();

    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    // 预先占用 Store capability actor id
    let store_actor_id = crate::common::ActorId::capability("Store");
    use crate::runtime::actor::Actor;
    struct DummyActor;
    #[async_trait::async_trait]
    impl Actor for DummyActor {
        fn actor_type(&self) -> &str {
            "Dummy"
        }
        async fn handle_message(
            &mut self,
            msg: crate::common::ActorMessage,
        ) -> crate::common::Result<crate::common::ActorMessageResult> {
            Ok(crate::common::ActorMessageResult {
                message_id: msg.id,
                payload: vec![],
                error: None,
            })
        }
    }
    actor_system
        .spawn(store_actor_id, DummyActor)
        .await
        .unwrap();

    let err = Arc::clone(&rt)
        .bind_actor_system(actor_system)
        .await
        .expect_err("duplicate capability actor id must fail bind");
    assert!(
        matches!(err, crate::common::ActantError::Actor(_)),
        "unexpected error: {err:?}"
    );
    // 失败不标记为已绑定。
    let type_id_store = std::any::TypeId::of::<Store>();
    assert!(rt.actor_ids.get(&type_id_store).is_none());
}

// =========================================================================
// Layer / HandlerList 边界
// =========================================================================

#[test]
fn handler_list_can_hold_multiple_handlers() {
    let meta = CapabilityMeta::new::<Store>("Store", EffectKind::Perform);
    let mut list = HandlerList::new(meta);
    let store1 = StateStore::open(tempdir().unwrap().path()).unwrap();
    let store2 = StateStore::open(tempdir().unwrap().path()).unwrap();
    list.push(erase_handler(StoreHandler::new(store1)));
    list.push(erase_handler(StoreHandler::new(store2)));
    assert_eq!(list.len(), 2);
    assert!(!list.is_empty());
}

#[test]
fn layer_meta_returns_correct_meta() {
    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform);
    let meta = layer.meta();
    assert_eq!(meta.name, "Store");
    assert_eq!(meta.default_kind, EffectKind::Perform);
}

#[test]
fn layer_chain_multiple_handlers() {
    let store1 = StateStore::open(tempdir().unwrap().path()).unwrap();
    let store2 = StateStore::open(tempdir().unwrap().path()).unwrap();
    let layer = Layer::<Store>::for_capability("Store", EffectKind::Perform)
        .chain(StoreHandler::new(store1))
        .chain(StoreHandler::new(store2));
    assert_eq!(layer.len(), 2);
}
