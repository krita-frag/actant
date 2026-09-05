//! Effect-Resource-Handler 统一扩展架构。
//!
//! 本模块定义了 actant 的统一扩展抽象：
//! - [`Capability`]：能力的声明（"能做什么"），关联 Request/Response 类型
//! - [`Handler`]：能力的具体实现
//! - [`Layer`]：handler 链，支持多 handler 叠加
//! - [`CapabilityRuntime`]：注册表 + dispatcher，提供 `ask`/`perform`/`emit` 三种执行模型
//!
//! # 三种 Effect 类型
//!
//! | 类型   | 语义               | 执行模型                                 |
//! |--------|--------------------|------------------------------------------|
//! | `ask`  | 决策型，请求返回值 | chain 中第一个返回 `Some` 的 handler 决定结果 |
//! | `perform` | 副作用型，执行 Rust trait | 单 handler 执行，结果直接返回       |
//! | `emit` | 反应型，触发订阅   | 所有 handler 顺序执行，无返回值          |

use std::any::{Any, TypeId};

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::common::{ActantError, ActorId, NodeId};

/// 能力的声明 trait。
pub trait Capability: Send + Sync + 'static {
    type Request: Send + Sync + 'static;
    type Response: Send + Sync + 'static;
}

/// 可在节点间序列化传输的 capability 元信息。
///
/// 由 [`CapabilityMeta`] 转换而来，用于 capability gossip 广播。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GossipCapabilityMeta {
    pub name: String,
    pub default_kind: EffectKind,
}

impl From<CapabilityMeta> for GossipCapabilityMeta {
    fn from(meta: CapabilityMeta) -> Self {
        Self {
            name: meta.name.to_string(),
            default_kind: meta.default_kind,
        }
    }
}

/// Effect 执行语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EffectKind {
    Ask,
    Perform,
    Emit,
}

/// Capability Actor 间传输的消息 envelope。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityEnvelope {
    pub kind: EffectKind,
    pub payload: Vec<u8>,
}

/// Capability 序列化编解码器。
///
/// 由于 capability 请求/响应是类型擦除的 `dyn Any`，而 Actor 消息负载为 `Vec<u8>`，
/// codec 负责在两者间转换。只有注册了 codec 的 capability 才会走 Actor 化执行路径。
pub trait CapabilityCodec: Send + Sync {
    fn serialize_request(
        &self,
        req: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Vec<u8>, ActantError>;

    fn deserialize_request(
        &self,
        bytes: &[u8],
    ) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, ActantError>;

    fn serialize_response(
        &self,
        resp: Box<dyn std::any::Any + Send + Sync>,
    ) -> Result<Vec<u8>, ActantError>;

    fn deserialize_response(
        &self,
        bytes: &[u8],
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, ActantError>;
}

/// 为具体 capability 类型实现的编解码器。
#[derive(Clone, Copy, Default)]
pub struct TypedCodec<C: Capability>(std::marker::PhantomData<C>);

impl<C: Capability> TypedCodec<C> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<C: Capability> CapabilityCodec for TypedCodec<C>
where
    C::Request: Serialize + for<'de> Deserialize<'de> + Clone + std::any::Any + Send + Sync,
    C::Response: Serialize + for<'de> Deserialize<'de> + std::any::Any + Send + Sync,
{
    fn serialize_request(
        &self,
        req: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Vec<u8>, ActantError> {
        let req = req.downcast_ref::<C::Request>().ok_or_else(|| {
            ActantError::Internal("capability codec: request type mismatch".into())
        })?;
        postcard::to_allocvec(req).map_err(|e| ActantError::Serialization(e.to_string()))
    }

    fn deserialize_request(
        &self,
        bytes: &[u8],
    ) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, ActantError> {
        // 远端 capability 请求：先校验大小上限。
        let req: C::Request = crate::common::decode_postcard(bytes)?;
        Ok(std::sync::Arc::new(req))
    }

    fn serialize_response(
        &self,
        resp: Box<dyn std::any::Any + Send + Sync>,
    ) -> Result<Vec<u8>, ActantError> {
        let resp = resp.downcast::<C::Response>().map_err(|_| {
            ActantError::Internal("capability codec: response type mismatch".into())
        })?;
        postcard::to_allocvec(&*resp).map_err(|e| ActantError::Serialization(e.to_string()))
    }

    fn deserialize_response(
        &self,
        bytes: &[u8],
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, ActantError> {
        // 远端 capability 响应：先校验大小上限。
        let resp: C::Response = crate::common::decode_postcard(bytes)?;
        Ok(Box::new(resp))
    }
}

/// Capability 的元数据（运行时类型信息）。
#[derive(Clone)]
pub struct CapabilityMeta {
    pub type_id: TypeId,
    pub name: &'static str,
    pub default_kind: EffectKind,
}

impl CapabilityMeta {
    pub fn new<C: Capability>(name: &'static str, default_kind: EffectKind) -> Self {
        Self {
            type_id: TypeId::of::<C>(),
            name,
            default_kind,
        }
    }
}

#[async_trait]
pub trait ErasedHandler: Send + Sync {
    async fn ask(&self, req: Arc<dyn Any + Send + Sync>) -> Option<Box<dyn Any + Send + Sync>>;

    async fn perform(
        &self,
        req: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActantError>;

    async fn emit(&self, req: Arc<dyn Any + Send + Sync>) -> Result<(), ActantError>;
}

#[async_trait]
pub trait Handler<C: Capability>: Send + Sync {
    async fn handle(&self, req: C::Request) -> Option<C::Response>;
}

pub struct HandlerAdapter<H, C: Capability> {
    pub handler: H,
    _phantom: std::marker::PhantomData<C>,
}

impl<H, C: Capability> HandlerAdapter<H, C> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<H, C> ErasedHandler for HandlerAdapter<H, C>
where
    H: Handler<C> + 'static,
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
{
    async fn ask(&self, req: Arc<dyn Any + Send + Sync>) -> Option<Box<dyn Any + Send + Sync>> {
        let req_ref = req.downcast_ref::<C::Request>()?;
        let resp = Handler::handle(&self.handler, req_ref.clone()).await?;
        Some(Box::new(resp))
    }

    async fn perform(
        &self,
        req: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, ActantError> {
        let req_ref = req
            .downcast_ref::<C::Request>()
            .ok_or_else(|| ActantError::Internal("perform: request type mismatch".into()))?;
        let resp = Handler::handle(&self.handler, req_ref.clone())
            .await
            .ok_or_else(|| ActantError::Internal("perform: handler returned None".into()))?;
        Ok(Box::new(resp))
    }

    async fn emit(&self, req: Arc<dyn Any + Send + Sync>) -> Result<(), ActantError> {
        let req_ref = req
            .downcast_ref::<C::Request>()
            .ok_or_else(|| ActantError::Internal("emit: request type mismatch".into()))?;
        // `Handler` trait 无错误通道：handle 返回 `Option<Response>`，None 表示
        // 该 handler 对本事件无意见（非失败），emit 语义下响应值不消费。
        // 自带错误通道的 handler（如 Python emit handler）直接实现
        // `ErasedHandler::emit`，其错误由调用循环（CapabilityActor）聚合后
        // 透传给调用方。
        let _response = Handler::handle(&self.handler, req_ref.clone()).await;
        Ok(())
    }
}

pub fn erase_handler<H, C>(handler: H) -> Arc<dyn ErasedHandler>
where
    H: Handler<C> + 'static,
    C: Capability + 'static,
    C::Request: Any + Send + Sync + Clone,
    C::Response: Any + Send + Sync,
{
    Arc::new(HandlerAdapter::new(handler))
}

#[derive(Clone)]
pub struct HandlerList {
    pub meta: CapabilityMeta,
    pub handlers: Vec<Arc<dyn ErasedHandler>>,
}

impl HandlerList {
    pub fn new(meta: CapabilityMeta) -> Self {
        Self {
            meta,
            handlers: Vec::new(),
        }
    }

    pub fn push(&mut self, handler: Arc<dyn ErasedHandler>) {
        self.handlers.push(handler);
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

pub struct Layer<C: Capability> {
    meta: CapabilityMeta,
    handlers: Vec<Arc<dyn ErasedHandler>>,
    _phantom: std::marker::PhantomData<C>,
}

impl<C: Capability> Layer<C> {
    pub fn new(meta: CapabilityMeta) -> Self {
        Self {
            meta,
            handlers: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn for_capability(name: &'static str, default_kind: EffectKind) -> Self {
        Self::new(CapabilityMeta::new::<C>(name, default_kind))
    }

    pub fn chain<H>(mut self, handler: H) -> Self
    where
        H: Handler<C> + 'static,
        C::Request: Any + Send + Sync + Clone,
        C::Response: Any + Send + Sync,
    {
        self.handlers.push(Arc::new(HandlerAdapter::new(handler)));
        self
    }

    pub fn chain_erased(mut self, handler: Arc<dyn ErasedHandler>) -> Self {
        self.handlers.push(handler);
        self
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn into_list(self) -> HandlerList {
        HandlerList {
            meta: self.meta,
            handlers: self.handlers,
        }
    }

    pub fn meta(&self) -> &CapabilityMeta {
        &self.meta
    }
}

pub struct CapabilityRuntime {
    layers: DashMap<TypeId, HandlerList>,
    metas: RwLock<Vec<CapabilityMeta>>,
    codecs: DashMap<TypeId, Arc<dyn CapabilityCodec>>,
    actor_system: parking_lot::RwLock<Option<Arc<crate::runtime::actor::ActorSystem>>>,
    actor_ids: DashMap<TypeId, crate::common::ActorId>,
    /// 从 capability gossip 学到的远端节点 capability 元信息。
    /// key 为远端节点 id，value 为该节点广播的 capability 列表。
    peer_capabilities: DashMap<NodeId, Vec<GossipCapabilityMeta>>,
    /// 反向索引：capability name → 持有该 capability 的远端节点集合。
    /// 由 `update_peer_capabilities` 自动维护，避免 `peer_capabilities.iter()`
    /// 全表扫描（O(N×M)）查找持有某 capability 的节点。
    cap_to_peers: DashMap<String, DashSet<NodeId>>,
}

impl Default for CapabilityRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl CapabilityRuntime {
    pub fn new() -> Self {
        Self {
            layers: DashMap::new(),
            metas: RwLock::new(Vec::new()),
            codecs: DashMap::new(),
            actor_system: parking_lot::RwLock::new(None),
            actor_ids: DashMap::new(),
            peer_capabilities: DashMap::new(),
            cap_to_peers: DashMap::new(),
        }
    }

    /// 绑定 `ActorSystem` 并为所有已注册 codec 的 capability spawn
    /// `CapabilityActor`。
    ///
    /// 关键不变量：Actor 在 spawn 时**从当前 `layers` snapshot handlers**，
    /// 而非 register 时拿到的副本。因此所有 `register` / `chain` /
    /// `register_store_handler` / `register_execute_handler` 必须在
    /// `bind_actor_system` **之前**完成；绑定后再调用会返回错误。
    /// 取消运行期热更新后，CapabilityRuntime 不再持有后台任务和更新通道，
    /// 结构更简洁，也不存在 Actor 重启窗口。
    pub async fn bind_actor_system(
        self: Arc<Self>,
        actor_system: Arc<crate::runtime::actor::ActorSystem>,
    ) {
        *self.actor_system.write() = Some(actor_system.clone());

        // 遍历 layers，为每个已注册 codec 的 capability spawn 一个 CapabilityActor。
        // handlers 在此时 snapshot，确保 bind 前的覆盖式注册生效。
        type SpawnTask = (
            TypeId,
            crate::common::ActorId,
            Vec<Arc<dyn ErasedHandler>>,
            Arc<dyn CapabilityCodec>,
        );
        let spawn_tasks: Vec<SpawnTask> = {
            let mut tasks = Vec::new();
            for entry in self.layers.iter() {
                let type_id = *entry.key();
                let list = entry.value();
                if let Some(codec) = self.codecs.get(&type_id).map(|e| e.clone()) {
                    let actor_id = ActorId::capability(list.meta.name);
                    tasks.push((type_id, actor_id, list.handlers.clone(), codec));
                }
            }
            tasks
        };

        for (type_id, actor_id, handlers, codec) in spawn_tasks {
            let actor = crate::runtime::capability::actor::CapabilityActor::new(handlers, codec);
            match actor_system.spawn(actor_id.clone(), actor).await {
                Ok(_) => {
                    self.actor_ids.insert(type_id, actor_id);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        capability = %actor_id.as_str(),
                        "failed to spawn capability actor"
                    );
                }
            }
        }
    }

    /// 显式注册一个 capability 的序列化 codec。只有注册了 codec 的 capability
    /// 才会走 Actor 化执行路径。
    pub fn register_codec<C: Capability>(&self)
    where
        C::Request: Serialize + for<'de> Deserialize<'de> + Clone + Any + Send + Sync,
        C::Response: Serialize + for<'de> Deserialize<'de> + Any + Send + Sync,
    {
        let type_id = TypeId::of::<C>();
        self.codecs
            .insert(type_id, Arc::new(TypedCodec::<C>::new()));
    }

    fn ensure_not_bound(&self) -> Result<(), ActantError> {
        if self.actor_system.read().is_some() {
            return Err(ActantError::Internal(
                "CapabilityRuntime already bound to ActorSystem: register/chain must be called before bind_actor_system".into(),
            ));
        }
        Ok(())
    }

    /// 注册一个 capability layer。
    ///
    /// 必须在 `bind_actor_system` 之前调用；绑定后再调用会返回错误。
    pub fn register<C: Capability>(&self, layer: Layer<C>) -> Result<(), ActantError> {
        self.ensure_not_bound()?;
        let type_id = layer.meta().type_id;
        let meta = layer.meta().clone();
        let list = layer.into_list();
        debug!(
            capability = meta.name,
            handlers = list.len(),
            "registering capability layer"
        );
        self.metas.write().push(meta);

        self.layers.insert(type_id, list);
        Ok(())
    }

    /// 确保 capability 有 layer entry，无则创建空 layer（无 handler）。
    ///
    /// 与 `register` 不同，此方法：
    /// - 幂等：已存在 layer 时不覆盖
    /// - 无 `ensure_not_bound` 检查：可在 `bind_actor_system` 后调用
    ///
    /// 用于 `register_defaults` 确保所有内置 capability 都有 layer，
    /// 使 `bind_actor_system` 为它们 spawn CapabilityActor（即使无 handler）。
    pub fn ensure_layer<C: Capability>(&self, meta: CapabilityMeta) {
        let type_id = meta.type_id;
        if self.layers.contains_key(&type_id) {
            return;
        }
        self.metas.write().push(meta.clone());
        self.layers.entry(type_id).or_insert(HandlerList::new(meta));
    }

    /// 向已注册的 capability 追加一个 handler。
    ///
    /// 必须在 `bind_actor_system` 之前调用；绑定后再调用会返回错误。
    pub fn chain<C: Capability>(&self, handler: Arc<dyn ErasedHandler>) -> Result<(), ActantError> {
        self.ensure_not_bound()?;
        let type_id = TypeId::of::<C>();
        let mut entry = self.layers.get_mut(&type_id).ok_or_else(|| {
            ActantError::Internal(format!(
                "capability {:?} not registered",
                std::any::type_name::<C>()
            ))
        })?;
        entry.push(handler);
        Ok(())
    }

    pub async fn ask<C: Capability>(
        &self,
        req: C::Request,
    ) -> Result<Option<C::Response>, ActantError>
    where
        C::Request: Any + Clone + Send + Sync,
        C::Response: Any + Send + Sync,
    {
        let type_id = TypeId::of::<C>();

        // 统一走 Actor 路径。未绑定 ActorSystem 或未注册 codec 视为配置错误。
        let actor_id = self.actor_ids.get(&type_id).map(|e| e.clone()).ok_or_else(|| {
            ActantError::Internal(format!(
                "ask: capability {:?} not bound to actor system (register codec + bind_actor_system first)",
                std::any::type_name::<C>()
            ))
        })?;
        let codec = self
            .codecs
            .get(&type_id)
            .map(|e| e.clone())
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "ask: capability {:?} has no codec",
                    std::any::type_name::<C>()
                ))
            })?;
        let actor_system = self
            .actor_system
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| ActantError::Internal("actor system not bound".into()))?;

        let req_arc: Arc<dyn Any + Send + Sync> = Arc::new(req);
        let req_bytes = codec
            .serialize_request(req_arc)
            .map_err(|e| ActantError::Internal(format!("ask codec: {}", e)))?;
        let local_envelope = CapabilityEnvelope {
            kind: EffectKind::Ask,
            payload: req_bytes.clone(),
        };
        let bytes = postcard::to_allocvec(&local_envelope)
            .map_err(|e| ActantError::Serialization(e.to_string()))?;
        let result = actor_system
            .call(&actor_id, "ask", bytes)
            .await
            .map_err(|e| ActantError::Actor(e.to_string()))?;
        if let Some(err) = result.error {
            return Err(ActantError::from(err));
        }
        let local_bytes = if result.payload.is_empty() || result.payload[0] == 0 {
            None
        } else {
            Some(result.payload)
        };

        let response_bytes = local_bytes;

        let response_bytes = match response_bytes {
            Some(b) if !b.is_empty() && b[0] != 0 => b,
            _ => return Ok(None),
        };

        let resp = codec
            .deserialize_response(&response_bytes[1..])
            .map_err(|e| ActantError::Internal(format!("ask decode: {}", e)))?;
        let resp = resp
            .downcast::<C::Response>()
            .map_err(|_| ActantError::Internal("ask: response type mismatch".into()))?;
        Ok(Some(*resp))
    }

    pub async fn perform<C: Capability>(&self, req: C::Request) -> Result<C::Response, ActantError>
    where
        C::Request: Any + Clone + Send + Sync,
        C::Response: Any + Send + Sync,
    {
        let type_id = TypeId::of::<C>();

        let actor_id = self
            .actor_ids
            .get(&type_id)
            .map(|e| e.clone())
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "perform: capability {:?} not bound to actor system",
                    std::any::type_name::<C>()
                ))
            })?;
        let codec = self
            .codecs
            .get(&type_id)
            .map(|e| e.clone())
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "perform: capability {:?} has no codec",
                    std::any::type_name::<C>()
                ))
            })?;
        let actor_system = self
            .actor_system
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| ActantError::Internal("actor system not bound".into()))?;

        let req_arc: Arc<dyn Any + Send + Sync> = Arc::new(req);
        let req_bytes = codec
            .serialize_request(req_arc)
            .map_err(|e| ActantError::Internal(format!("perform codec: {}", e)))?;

        let envelope = CapabilityEnvelope {
            kind: EffectKind::Perform,
            payload: req_bytes,
        };
        let bytes = postcard::to_allocvec(&envelope)
            .map_err(|e| ActantError::Serialization(e.to_string()))?;
        let result = actor_system
            .call(&actor_id, "perform", bytes)
            .await
            .map_err(|e| ActantError::Actor(e.to_string()))?;
        if let Some(err) = result.error {
            return Err(ActantError::from(err));
        }
        let response_bytes = result.payload;

        let resp = codec
            .deserialize_response(&response_bytes)
            .map_err(|e| ActantError::Internal(format!("perform decode: {}", e)))?;
        let resp = resp
            .downcast::<C::Response>()
            .map_err(|_| ActantError::Internal("perform: response type mismatch".into()))?;
        Ok(*resp)
    }

    pub async fn emit<C: Capability>(&self, req: C::Request) -> Result<(), ActantError>
    where
        C::Request: Any + Clone + Send + Sync,
    {
        let type_id = TypeId::of::<C>();

        let actor_id = self
            .actor_ids
            .get(&type_id)
            .map(|e| e.clone())
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "emit: capability {:?} not bound to actor system",
                    std::any::type_name::<C>()
                ))
            })?;
        let codec = self
            .codecs
            .get(&type_id)
            .map(|e| e.clone())
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "emit: capability {:?} has no codec",
                    std::any::type_name::<C>()
                ))
            })?;
        let actor_system = self
            .actor_system
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| ActantError::Internal("actor system not bound".into()))?;

        let local_handlers = self.handler_count_by_type_id(type_id).unwrap_or(0);
        let req_arc: Arc<dyn Any + Send + Sync> = Arc::new(req);
        let req_bytes = codec
            .serialize_request(req_arc)
            .map_err(|e| ActantError::Internal(format!("emit codec: {}", e)))?;

        if local_handlers > 0 {
            let envelope = CapabilityEnvelope {
                kind: EffectKind::Emit,
                payload: req_bytes,
            };
            let bytes = postcard::to_allocvec(&envelope)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            let result = actor_system
                .call(&actor_id, "emit", bytes)
                .await
                .map_err(|e| ActantError::Actor(e.to_string()))?;
            if let Some(err) = result.error {
                return Err(ActantError::from(err));
            }
        }
        Ok(())
    }

    pub fn capability_count(&self) -> usize {
        self.layers.len()
    }

    pub fn handler_count<C: Capability>(&self) -> usize {
        self.layers
            .get(&TypeId::of::<C>())
            .map(|e| e.handlers.len())
            .unwrap_or(0)
    }

    pub fn handler_count_by_type_id(&self, type_id: TypeId) -> Option<usize> {
        self.layers.get(&type_id).map(|e| e.handlers.len())
    }

    pub fn capabilities(&self) -> Vec<CapabilityMeta> {
        self.metas.read().clone()
    }

    /// 更新指定远端节点的 capability 元信息。
    ///
    /// 通常由 `CapabilityGossipActor::handle_gossip` 在收到邻居广播后调用。
    pub fn update_peer_capabilities(
        &self,
        node_id: NodeId,
        capabilities: Vec<GossipCapabilityMeta>,
    ) {
        // 维护反向索引：先移除该节点在旧 capability 集合中的全部条目，
        // 再按新 capability 列表重新插入。避免 stale 条目累积。
        for entry in self.cap_to_peers.iter() {
            entry.value().remove(&node_id);
        }
        for cap in &capabilities {
            self.cap_to_peers
                .entry(cap.name.clone())
                .or_default()
                .insert(node_id.clone());
        }
        self.peer_capabilities.insert(node_id, capabilities);
    }

    /// 返回指定远端节点已知的 capability 元信息。
    pub fn peer_capabilities(&self, node_id: &NodeId) -> Option<Vec<GossipCapabilityMeta>> {
        self.peer_capabilities
            .get(node_id)
            .map(|e| e.value().clone())
    }

    /// 返回所有已知有 capability 元信息的远端节点 id。
    pub fn peer_nodes(&self) -> Vec<NodeId> {
        self.peer_capabilities
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }
}

pub mod actor;
pub mod builtins;
pub mod gossip;

pub use builtins::*;

#[cfg(test)]
#[path = "../../tests/rust/unit/runtime/capability.rs"]
mod tests;
