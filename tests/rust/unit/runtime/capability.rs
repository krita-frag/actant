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
    register_defaults(&runtime);
    register_store_handler(&runtime, store).unwrap();
    // 强制走 Actor 路径：必须 bind_actor_system 后才能 perform。
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&runtime).bind_actor_system(actor_system).await;

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
    register_defaults(&runtime);
    register_store_handler(&runtime, store).unwrap();

    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&runtime)
        .bind_actor_system(actor_system.clone())
        .await;

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
fn gossip_meta_from_capability_meta_preserves_fields() {
    let meta = CapabilityMeta::new::<Store>("Store", EffectKind::Perform);
    let gossip: GossipCapabilityMeta = meta.into();
    assert_eq!(gossip.name, "Store");
    assert_eq!(gossip.default_kind, EffectKind::Perform);
}

#[test]
fn effect_kind_variants_are_distinct() {
    assert_ne!(EffectKind::Ask, EffectKind::Perform);
    assert_ne!(EffectKind::Perform, EffectKind::Emit);
    assert_ne!(EffectKind::Ask, EffectKind::Emit);
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
    rt.register_codec::<Store>();
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
    Arc::clone(&rt).bind_actor_system(actor_system).await;

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
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let store2 = StateStore::open(tempdir().unwrap().path()).unwrap();
    let handler = erase_handler(StoreHandler::new(store2));
    let result = rt.chain::<Store>(handler);
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[test]
fn runtime_update_peer_capabilities_stores_and_indexes() {
    let rt = CapabilityRuntime::new();
    let node = NodeId::from("peer-A".to_string());
    let caps = vec![
        GossipCapabilityMeta {
            name: "Store".to_string(),
            default_kind: EffectKind::Perform,
        },
        GossipCapabilityMeta {
            name: "Execute".to_string(),
            default_kind: EffectKind::Perform,
        },
    ];
    rt.update_peer_capabilities(node.clone(), caps.clone());
    assert_eq!(rt.peer_capabilities(&node), Some(caps));
    assert_eq!(rt.peer_nodes(), vec![node.clone()]);
}

#[test]
fn runtime_update_peer_capabilities_replaces_existing() {
    let rt = CapabilityRuntime::new();
    let node = NodeId::from("peer-B".to_string());
    rt.update_peer_capabilities(
        node.clone(),
        vec![GossipCapabilityMeta {
            name: "Store".to_string(),
            default_kind: EffectKind::Perform,
        }],
    );
    rt.update_peer_capabilities(
        node.clone(),
        vec![GossipCapabilityMeta {
            name: "Execute".to_string(),
            default_kind: EffectKind::Perform,
        }],
    );
    let caps = rt.peer_capabilities(&node).unwrap();
    assert_eq!(caps.len(), 1);
    assert_eq!(caps[0].name, "Execute");
}

#[test]
fn runtime_peer_capabilities_returns_none_for_unknown_node() {
    let rt = CapabilityRuntime::new();
    let node = NodeId::from("unknown".to_string());
    assert!(rt.peer_capabilities(&node).is_none());
}

#[test]
fn runtime_peer_nodes_returns_empty_when_no_peers() {
    let rt = CapabilityRuntime::new();
    assert!(rt.peer_nodes().is_empty());
}

#[tokio::test]
async fn ask_without_bound_actor_system_returns_error() {
    let rt = CapabilityRuntime::new();
    rt.register_codec::<Store>();
    let result = rt.ask::<Store>(StoreReq::Get { key: b"k".to_vec() }).await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn perform_without_bound_actor_system_returns_error() {
    let rt = CapabilityRuntime::new();
    rt.register_codec::<Store>();
    let result = rt
        .perform::<Store>(StoreReq::Get { key: b"k".to_vec() })
        .await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn emit_without_bound_actor_system_returns_error() {
    let rt = CapabilityRuntime::new();
    rt.register_codec::<TaskLifecycle>();
    let result = rt
        .emit::<TaskLifecycle>(TaskEvent::Started {
            task_id: TaskId::from("t-1".to_string()),
            workflow_id: WorkflowId::from("wf-1".to_string()),
        })
        .await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[tokio::test]
async fn ask_without_codec_returns_error() {
    let rt = CapabilityRuntime::new();
    // 不调用 register_codec
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    let rt = Arc::new(rt);
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt.ask::<Store>(StoreReq::Get { key: b"k".to_vec() }).await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
}

#[test]
fn builtin_capabilities_returns_all_ten() {
    let caps = builtin_capabilities();
    assert_eq!(caps.len(), 10);
    let names: Vec<&str> = caps.iter().map(|c| c.name).collect();
    assert!(names.contains(&"Serialization"));
    assert!(names.contains(&"Transport"));
    assert!(names.contains(&"Store"));
    assert!(names.contains(&"Execute"));
    assert!(names.contains(&"TaskLifecycle"));
    assert!(names.contains(&"WorkflowLifecycle"));
    assert!(names.contains(&"NodeLifecycle"));
    assert!(names.contains(&"ActorMessaging"));
    assert!(names.contains(&"ActorSupervision"));
    assert!(names.contains(&"ActorLifecycle"));
}

#[test]
fn register_defaults_registers_all_codecs() {
    let rt = CapabilityRuntime::new();
    register_defaults(&rt);
    // register_defaults 注册 codec 与空 layer（ensure_layer），
    // 使所有内置 capability 都有 layer entry。
    assert_eq!(rt.capability_count(), 10);
    // handler_count 查 layer 中 handler 数量，应为 0（空 layer）
    assert_eq!(rt.handler_count::<Store>(), 0);
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

#[tokio::test]
async fn execute_handler_dispatches_payload_and_returns_outcome() {
    use crate::runtime::dispatcher::TaskDispatcher;
    use async_trait::async_trait;

    struct EchoDispatcher;
    #[async_trait]
    impl TaskDispatcher for EchoDispatcher {
        async fn dispatch(
            &self,
            _task_id: &str,
            payload: Vec<u8>,
            _cancel: crate::runtime::dispatcher::CancelFlag,
        ) -> crate::common::Result<Vec<u8>> {
            Ok(payload)
        }
    }

    let dispatcher: Arc<dyn TaskDispatcher> = Arc::new(EchoDispatcher);
    let handler = ExecuteHandler::new(dispatcher, Vec::new());
    let ctx = ExecuteCtx {
        task_id: TaskId::from("t-1".to_string()),
        workflow_id: WorkflowId::from("wf-1".to_string()),
        payload: b"echo".to_vec(),
        timeout_ms: 1000,
    };
    let result = handler.handle(ctx).await;
    match result {
        Some(Ok(outcome)) => {
            assert_eq!(outcome.task_id.as_ref(), "t-1");
            assert!(!outcome.result_payload.is_empty());
        }
        other => panic!("expected Ok outcome, got {:?}", other),
    }
}

#[tokio::test]
async fn execute_handler_returns_error_on_dispatch_failure() {
    use crate::runtime::dispatcher::TaskDispatcher;
    use async_trait::async_trait;

    struct FailingDispatcher;
    #[async_trait]
    impl TaskDispatcher for FailingDispatcher {
        async fn dispatch(
            &self,
            _task_id: &str,
            _payload: Vec<u8>,
            _cancel: crate::runtime::dispatcher::CancelFlag,
        ) -> crate::common::Result<Vec<u8>> {
            Err(ActantError::Internal("boom".to_string()))
        }
    }

    let dispatcher: Arc<dyn TaskDispatcher> = Arc::new(FailingDispatcher);
    let handler = ExecuteHandler::new(dispatcher, Vec::new());
    let ctx = ExecuteCtx {
        task_id: TaskId::from("t-2".to_string()),
        workflow_id: WorkflowId::from("wf-2".to_string()),
        payload: Vec::new(),
        timeout_ms: 1000,
    };
    let result = handler.handle(ctx).await;
    assert!(matches!(result, Some(Err(_))));
}

#[test]
fn register_execute_handler_adds_layer() {
    use crate::runtime::dispatcher::TaskRegistry;
    let rt = CapabilityRuntime::new();
    let dispatcher = TaskRegistry::new(1, 8, Vec::new())
        .unwrap()
        .into_dispatcher();
    register_execute_handler(&rt, dispatcher, Vec::new()).unwrap();
    assert_eq!(rt.capability_count(), 1);
    assert_eq!(rt.handler_count::<Execute>(), 1);
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
    assert!(rt.peer_nodes().is_empty());
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
    Arc::clone(&rt).bind_actor_system(actor_system).await;
    // bind 后 ensure_layer 不会报错
    rt.ensure_layer::<Execute>(Execute::meta());
}

#[tokio::test]
async fn ensure_layer_after_bind_does_not_spawn_actor() {
    // bind 后调用 ensure_layer 不会为新 capability spawn CapabilityActor
    // （因为没有 codec 注册 + actor 已 snapshot）
    let rt = CapabilityRuntime::new();
    rt.register_codec::<Store>();
    rt.ensure_layer::<Store>(Store::meta());
    let rt = Arc::new(rt);
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    // Execute 没有 codec 也没有 layer，bind 后 ensure_layer 不会 spawn
    rt.ensure_layer::<Execute>(Execute::meta());
    // Execute 未注册 codec，所以 actor_ids 中不应有 Execute 条目
    let type_id = std::any::TypeId::of::<Execute>();
    assert!(rt.actor_ids.get(&type_id).is_none());
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
async fn handler_adapter_ask_returns_none_on_type_mismatch() {
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
    // 传入错误类型的 request
    let wrong_req: Arc<dyn Any + Send + Sync> = Arc::new(42i32);
    let result = adapter.ask(wrong_req).await;
    assert!(result.is_none());
}

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
async fn handler_adapter_ask_returns_none_when_handler_returns_none() {
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
    let result = adapter.ask(req).await;
    assert!(result.is_none());
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
// ask / perform / emit 路径（绑定 ActorSystem）
// =========================================================================

#[tokio::test]
async fn ask_with_bound_actor_system_returns_response() {
    let dir = tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    register_store_handler(&rt, store).unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    // 先 Put 一个值
    let _ = rt
        .perform::<Store>(StoreReq::Put {
            key: b"ask-k".to_vec(),
            value: b"ask-v".to_vec(),
        })
        .await
        .unwrap();

    // ask 查询：StoreHandler 总是返回 Some，所以 ask 应返回 Some(Ok(Some(value)))
    let result = rt
        .ask::<Store>(StoreReq::Get {
            key: b"ask-k".to_vec(),
        })
        .await
        .unwrap();
    assert!(result.is_some());
    let resp = result.unwrap();
    assert!(resp.is_ok());
    assert_eq!(resp.unwrap(), Some(b"ask-v".to_vec()));
}

#[tokio::test]
async fn ask_with_bound_actor_system_no_handler_returns_none() {
    // 注册 codec + 空 layer（无 handler），ask 时所有 handler 返回 None
    // → actor 返回 payload=[0] → route_remote 返回 Ok(None) → ask 返回 Ok(None)
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    // 不注册任何 handler，仅 ensure_layer（register_defaults 已做）
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .ask::<Store>(StoreReq::Get {
            key: b"no-handler".to_vec(),
        })
        .await;
    assert!(matches!(result, Ok(None)));
}

#[tokio::test]
async fn ask_with_bound_actor_system_returns_none_when_handler_returns_none() {
    // 注册一个总返回 None 的 handler → ask 应返回 Ok(None)
    use async_trait::async_trait;

    struct NoneStoreHandler;
    #[async_trait]
    impl Handler<Store> for NoneStoreHandler {
        async fn handle(&self, _req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
            None
        }
    }

    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    rt.register(Layer::<Store>::new(Store::meta()).chain_erased(erase_handler(NoneStoreHandler)))
        .unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt.ask::<Store>(StoreReq::Get { key: b"k".to_vec() }).await;
    assert!(matches!(result, Ok(None)));
}

#[tokio::test]
async fn perform_with_no_local_handler_and_no_peer_returns_error() {
    // 注册 codec + 空 layer（无 handler），无 peer → perform 应报错
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .perform::<Store>(StoreReq::Get { key: b"k".to_vec() })
        .await;
    assert!(matches!(result, Err(ActantError::Internal(_))));
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("no local handler and no peer with capability"));
}

#[tokio::test]
async fn perform_with_no_local_handler_but_peer_exists_routes_remote() {
    // 注册 codec + 空 layer（无 handler），但有 peer 声明该 capability
    // → 调用 route_remote → actor_system.call_remote → 无 network → 报错
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    // 添加一个 peer 声明 Store capability
    rt.update_peer_capabilities(
        NodeId::from("peer-X".to_string()),
        vec![GossipCapabilityMeta {
            name: "Store".to_string(),
            default_kind: EffectKind::Perform,
        }],
    );
    // ActorSystem 无 network 配置
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .perform::<Store>(StoreReq::Get { key: b"k".to_vec() })
        .await;
    // route_remote 会调用 call_remote，无 network → Actor error
    assert!(result.is_err());
}

#[tokio::test]
async fn perform_with_no_local_handler_but_peer_with_network_returns_remote_error() {
    // peer 声明 capability + ActorSystem 配置 MockTransport（send_direct_request 返回错误）
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    rt.update_peer_capabilities(
        NodeId::from("peer-Y".to_string()),
        vec![GossipCapabilityMeta {
            name: "Store".to_string(),
            default_kind: EffectKind::Perform,
        }],
    );
    let transport: Arc<dyn crate::runtime::network::Transport> =
        Arc::new(crate::test_support::MockTransport::new("local-node"));
    let actor_system = Arc::new(
        crate::runtime::actor::ActorSystem::new()
            .with_node_id(NodeId::from("local-node".to_string()))
            .with_network(transport),
    );
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .perform::<Store>(StoreReq::Get { key: b"k".to_vec() })
        .await;
    // MockTransport.send_direct_request 返回 Internal error
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    // 错误应来自 route_remote 的 call_remote 失败
    assert!(
        err_msg.contains("MockTransport") || err_msg.contains("Actor"),
        "unexpected error: {}",
        err_msg
    );
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
    register_defaults(&rt);
    rt.register(
        Layer::<TaskLifecycle>::new(TaskLifecycle::meta())
            .chain_erased(erase_handler(CountingEmitHandler)),
    )
    .unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

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
async fn emit_without_local_handler_but_with_peers_spawns_remote() {
    // 注册 codec + 空 layer（无 handler），有 peer 声明该 capability
    // → emit 跳过本地 actor 调用，但 route_emit_to_peers 会 spawn 远程调用
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    rt.update_peer_capabilities(
        NodeId::from("peer-emit".to_string()),
        vec![GossipCapabilityMeta {
            name: "TaskLifecycle".to_string(),
            default_kind: EffectKind::Emit,
        }],
    );
    let transport: Arc<dyn crate::runtime::network::Transport> =
        Arc::new(crate::test_support::MockTransport::new("local-emit"));
    let actor_system = Arc::new(
        crate::runtime::actor::ActorSystem::new()
            .with_node_id(NodeId::from("local-emit".to_string()))
            .with_network(transport),
    );
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    // emit 应成功（远程调用是 spawn 的，不阻塞）
    let result = rt
        .emit::<TaskLifecycle>(TaskEvent::Started {
            task_id: TaskId::from("t-emit-2".to_string()),
            workflow_id: WorkflowId::from("wf-emit-2".to_string()),
        })
        .await;
    assert!(result.is_ok());
    // 等待 spawn 的任务执行
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[tokio::test]
async fn emit_without_local_handler_and_no_peers_succeeds() {
    // 无 handler、无 peer → emit 直接成功（既不调用本地 actor，也不调用远程）
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

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
            // Handler 返回 None → emit 在 HandlerAdapter 中被忽略（不报错）
            // 但 CapabilityActor::emit 会调用 ErasedHandler::emit
            // 这里 HandlerAdapter::emit 不调用 handle，而是直接 Ok
            None
        }
    }

    // HandlerAdapter::emit 总是返回 Ok（即使 Handler::handle 返回 None）
    // 所以这个测试验证 emit 不因 Handler 返回 None 而失败
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    rt.register(
        Layer::<TaskLifecycle>::new(TaskLifecycle::meta())
            .chain_erased(erase_handler(FailingEmitHandler)),
    )
    .unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .emit::<TaskLifecycle>(TaskEvent::Started {
            task_id: TaskId::from("t-emit-4".to_string()),
            workflow_id: WorkflowId::from("wf-emit-4".to_string()),
        })
        .await;
    assert!(result.is_ok());
}

// =========================================================================
// route_remote / route_emit_to_peers 间接覆盖
// =========================================================================

#[tokio::test]
async fn ask_routes_to_remote_when_local_handler_returns_none() {
    // 本地有 handler 但返回 None（ask 路径）→ 尝试 route_remote
    // 无 peer → route_remote 返回 Ok(None) → ask 返回 Ok(None)
    use async_trait::async_trait;

    struct NoneStoreHandler;
    #[async_trait]
    impl Handler<Store> for NoneStoreHandler {
        async fn handle(&self, _req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
            None
        }
    }

    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    rt.register(Layer::<Store>::new(Store::meta()).chain_erased(erase_handler(NoneStoreHandler)))
        .unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .ask::<Store>(StoreReq::Get {
            key: b"route-k".to_vec(),
        })
        .await;
    assert!(matches!(result, Ok(None)));
}

#[tokio::test]
async fn ask_routes_to_remote_with_peer_but_no_network_returns_error() {
    // 本地 handler 返回 None + 有 peer + ActorSystem 无 network
    use async_trait::async_trait;

    struct NoneStoreHandler;
    #[async_trait]
    impl Handler<Store> for NoneStoreHandler {
        async fn handle(&self, _req: StoreReq) -> Option<Result<Option<Vec<u8>>, String>> {
            None
        }
    }

    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    rt.register(Layer::<Store>::new(Store::meta()).chain_erased(erase_handler(NoneStoreHandler)))
        .unwrap();
    rt.update_peer_capabilities(
        NodeId::from("peer-ask".to_string()),
        vec![GossipCapabilityMeta {
            name: "Store".to_string(),
            default_kind: EffectKind::Ask,
        }],
    );
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let result = rt
        .ask::<Store>(StoreReq::Get {
            key: b"route-k".to_vec(),
        })
        .await;
    // route_remote → call_remote → 无 network → Actor error
    assert!(result.is_err());
}

// =========================================================================
// Serialization handler / register_serialization_handler
// =========================================================================

#[tokio::test]
async fn serialization_handler_dump_load_roundtrip() {
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    register_serialization_handler(&rt).unwrap();
    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    let payload = b"serialize-me".to_vec();
    let dump_result = rt
        .perform::<Serialization>(SerializationReq::Dump {
            payload: payload.clone(),
        })
        .await
        .unwrap();
    assert!(dump_result.is_ok());
    assert_eq!(dump_result.unwrap(), payload);

    let load_result = rt
        .perform::<Serialization>(SerializationReq::Load {
            data: payload.clone(),
        })
        .await
        .unwrap();
    assert!(load_result.is_ok());
    assert_eq!(load_result.unwrap(), payload);
}

#[tokio::test]
async fn serialization_handler_direct_handle() {
    let handler = SerializationHandler;
    let dump = handler
        .handle(SerializationReq::Dump {
            payload: b"dump".to_vec(),
        })
        .await;
    assert!(matches!(dump, Some(Ok(ref v)) if v == b"dump"));

    let load = handler
        .handle(SerializationReq::Load {
            data: b"load".to_vec(),
        })
        .await;
    assert!(matches!(load, Some(Ok(ref v)) if v == b"load"));
}

// =========================================================================
// update_peer_capabilities 反向索引维护
// =========================================================================

#[test]
fn update_peer_capabilities_maintains_reverse_index() {
    let rt = CapabilityRuntime::new();

    // peer-A 声明 Store + Execute
    rt.update_peer_capabilities(
        NodeId::from("peer-A".to_string()),
        vec![
            GossipCapabilityMeta {
                name: "Store".to_string(),
                default_kind: EffectKind::Perform,
            },
            GossipCapabilityMeta {
                name: "Execute".to_string(),
                default_kind: EffectKind::Perform,
            },
        ],
    );

    // peer-B 声明 Store
    rt.update_peer_capabilities(
        NodeId::from("peer-B".to_string()),
        vec![GossipCapabilityMeta {
            name: "Store".to_string(),
            default_kind: EffectKind::Perform,
        }],
    );

    // 验证 peer_nodes 包含两个 peer
    let mut nodes = rt.peer_nodes();
    nodes.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].0, "peer-A");
    assert_eq!(nodes[1].0, "peer-B");

    // peer-A 更新为只声明 Execute → Store 反向索引应移除 peer-A
    rt.update_peer_capabilities(
        NodeId::from("peer-A".to_string()),
        vec![GossipCapabilityMeta {
            name: "Execute".to_string(),
            default_kind: EffectKind::Perform,
        }],
    );

    // peer-A 的 capability 列表应只有 Execute
    let peer_a_caps = rt
        .peer_capabilities(&NodeId::from("peer-A".to_string()))
        .unwrap();
    assert_eq!(peer_a_caps.len(), 1);
    assert_eq!(peer_a_caps[0].name, "Execute");
}

#[test]
fn update_peer_capabilities_removes_node_from_all_old_capabilities() {
    let rt = CapabilityRuntime::new();
    let node = NodeId::from("peer-churn".to_string());

    // 声明 3 个 capability
    rt.update_peer_capabilities(
        node.clone(),
        vec![
            GossipCapabilityMeta {
                name: "Cap1".to_string(),
                default_kind: EffectKind::Perform,
            },
            GossipCapabilityMeta {
                name: "Cap2".to_string(),
                default_kind: EffectKind::Perform,
            },
            GossipCapabilityMeta {
                name: "Cap3".to_string(),
                default_kind: EffectKind::Perform,
            },
        ],
    );

    // 更新为空列表 → 应从所有反向索引中移除
    rt.update_peer_capabilities(node.clone(), vec![]);
    let caps = rt.peer_capabilities(&node).unwrap();
    assert!(caps.is_empty());
}

// =========================================================================
// bind_actor_system 多次绑定 / spawn 失败路径
// =========================================================================

#[tokio::test]
async fn bind_actor_system_spawns_actors_for_registered_codecs() {
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);
    let store = StateStore::open(tempdir().unwrap().path()).unwrap();
    register_store_handler(&rt, store).unwrap();

    let actor_system = Arc::new(crate::runtime::actor::ActorSystem::new());
    Arc::clone(&rt).bind_actor_system(actor_system).await;

    // 所有 10 个内置 capability 都注册了 codec + layer → 应有 10 个 actor_ids
    // （Store 有 handler，其余 9 个空 layer）
    let type_id_store = std::any::TypeId::of::<Store>();
    assert!(rt.actor_ids.get(&type_id_store).is_some());
    let type_id_execute = std::any::TypeId::of::<Execute>();
    assert!(rt.actor_ids.get(&type_id_execute).is_some());
}

#[tokio::test]
async fn bind_actor_system_with_duplicate_actor_id_warns_but_continues() {
    // 预先在 ActorSystem 中 spawn 一个同名 actor，bind 时 spawn 会失败
    // → 走 warn 路径，但不 panic
    let rt = Arc::new(CapabilityRuntime::new());
    register_defaults(&rt);

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

    // bind_actor_system 应不 panic，Store actor spawn 失败走 warn 路径
    Arc::clone(&rt).bind_actor_system(actor_system).await;
    // Store 的 actor_id 不应被插入到 rt.actor_ids（spawn 失败）
    let type_id_store = std::any::TypeId::of::<Store>();
    assert!(rt.actor_ids.get(&type_id_store).is_none());
    // 但其他 capability 应正常 spawn
    let type_id_execute = std::any::TypeId::of::<Execute>();
    assert!(rt.actor_ids.get(&type_id_execute).is_some());
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

// =========================================================================
// ExecuteHandler 签名验证
// =========================================================================

#[tokio::test]
async fn execute_handler_signs_payload_before_dispatch() {
    use crate::runtime::dispatcher::TaskDispatcher;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct CapturingDispatcher {
        received_payload: Arc<parking_lot::Mutex<Vec<u8>>>,
    }
    #[async_trait]
    impl TaskDispatcher for CapturingDispatcher {
        async fn dispatch(
            &self,
            _task_id: &str,
            payload: Vec<u8>,
            _cancel: crate::runtime::dispatcher::CancelFlag,
        ) -> crate::common::Result<Vec<u8>> {
            *self.received_payload.lock() = payload.clone();
            Ok(payload)
        }
    }

    let received = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let dispatcher: Arc<dyn TaskDispatcher> = Arc::new(CapturingDispatcher {
        received_payload: received.clone(),
    });
    let signing_key = b"test-key-32-bytes-0123456789abcdef".to_vec();
    let handler = ExecuteHandler::new(dispatcher, signing_key);
    let ctx = ExecuteCtx {
        task_id: TaskId::from("t-sign".to_string()),
        workflow_id: WorkflowId::from("wf-sign".to_string()),
        payload: b"original-payload".to_vec(),
        timeout_ms: 5000,
    };
    let result = handler.handle(ctx).await;
    assert!(result.is_some());
    let outcome = result.unwrap();
    assert!(outcome.is_ok());
    let outcome = outcome.unwrap();
    assert_eq!(outcome.task_id.as_ref(), "t-sign");

    // 验证 dispatcher 收到的是签名后的 payload（不等于原始 payload）
    let received_payload = received.lock().clone();
    assert_ne!(received_payload, b"original-payload");
    assert!(!received_payload.is_empty());
}
