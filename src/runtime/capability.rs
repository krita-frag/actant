//! Capability-Resource-Handler 统一扩展架构（ADR 0001）。
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
        let _ = Handler::handle(&self.handler, req_ref.clone()).await;
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
            return Err(ActantError::Internal(err));
        }
        let local_bytes = if result.payload.is_empty() || result.payload[0] == 0 {
            None
        } else {
            Some(result.payload)
        };

        let response_bytes = match local_bytes {
            Some(b) => Some(b),
            None => self.route_remote::<C>(EffectKind::Ask, req_bytes).await?,
        };

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

        let local_handlers = self.handler_count_by_type_id(type_id).unwrap_or(0);
        let req_arc: Arc<dyn Any + Send + Sync> = Arc::new(req);
        let req_bytes = codec
            .serialize_request(req_arc)
            .map_err(|e| ActantError::Internal(format!("perform codec: {}", e)))?;

        let response_bytes = if local_handlers == 0 {
            // 本地无 handler：尝试路由到具备该 capability 的对等节点。
            self.route_remote::<C>(EffectKind::Perform, req_bytes)
                .await?
                .ok_or_else(|| {
                    ActantError::Internal(
                        "perform: no local handler and no peer with capability".into(),
                    )
                })?
        } else {
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
                return Err(ActantError::Internal(err));
            }
            result.payload
        };

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
                payload: req_bytes.clone(),
            };
            let bytes = postcard::to_allocvec(&envelope)
                .map_err(|e| ActantError::Serialization(e.to_string()))?;
            let result = actor_system
                .call(&actor_id, "emit", bytes)
                .await
                .map_err(|e| ActantError::Actor(e.to_string()))?;
            if let Some(err) = result.error {
                return Err(ActantError::Internal(err));
            }
        }

        // 同时路由到所有声明该 capability 的对等节点（尽力而为，不阻塞错误）。
        self.route_emit_to_peers::<C>(req_bytes)?;
        Ok(())
    }

    /// 将 capability 请求路由到声明了该能力的远端节点。
    ///
    /// 当前策略：选择声明了该 capability 的节点中 `node_id` 字典序最小的一个，
    /// 通过 `ActorSystem::call_remote` 调用其 `CapabilityActor`。
    /// 返回 `Ok(None)` 表示没有已知节点具备该能力。
    async fn route_remote<C: Capability>(
        &self,
        kind: EffectKind,
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ActantError>
    where
        C::Request: Any + Clone + Send + Sync,
        C::Response: Any + Send + Sync,
    {
        let type_id = TypeId::of::<C>();
        let meta = self
            .metas
            .read()
            .iter()
            .find(|m| m.type_id == type_id)
            .cloned()
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "remote route: capability {:?} meta not found",
                    std::any::type_name::<C>()
                ))
            })?;
        let actor_system = self
            .actor_system
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| ActantError::Internal("actor system not bound".into()))?;

        let target_node = {
            // P1-P3: 使用反向索引 cap_to_peers，O(1) 查找而非 O(N×M) 全表扫描。
            let mut target: Option<NodeId> = None;
            if let Some(peers) = self.cap_to_peers.get(meta.name) {
                for node in peers.iter() {
                    let candidate = node.clone();
                    match target {
                        None => target = Some(candidate),
                        Some(ref current) if candidate.0 < current.0 => target = Some(candidate),
                        _ => {}
                    }
                }
            }
            target
        };

        let target_node = match target_node {
            Some(n) => n,
            None => return Ok(None),
        };

        let actor_id = ActorId::capability(meta.name);
        let envelope = CapabilityEnvelope { kind, payload };
        let bytes = postcard::to_allocvec(&envelope)
            .map_err(|e| ActantError::Serialization(e.to_string()))?;
        let result = actor_system
            .call_remote(&target_node, actor_id, "invoke".to_string(), bytes)
            .await
            .map_err(|e| ActantError::Actor(e.to_string()))?;
        if let Some(err) = result.error {
            return Err(ActantError::Internal(err));
        }
        Ok(Some(result.payload))
    }

    /// 将 emit 事件尽力路由到所有声明了该 capability 的对等节点。
    fn route_emit_to_peers<C: Capability>(&self, payload: Vec<u8>) -> Result<(), ActantError>
    where
        C::Request: Any + Clone + Send + Sync,
        C::Response: Any + Send + Sync,
    {
        let type_id = TypeId::of::<C>();
        let meta = self
            .metas
            .read()
            .iter()
            .find(|m| m.type_id == type_id)
            .cloned()
            .ok_or_else(|| {
                ActantError::Internal(format!(
                    "remote emit: capability {:?} meta not found",
                    std::any::type_name::<C>()
                ))
            })?;
        let actor_system = self
            .actor_system
            .read()
            .as_ref()
            .cloned()
            .ok_or_else(|| ActantError::Internal("actor system not bound".into()))?;

        // P1-P3: 使用反向索引 cap_to_peers，O(1) 查找而非 O(N×M) 全表扫描。
        let target_nodes: Vec<NodeId> = self
            .cap_to_peers
            .get(meta.name)
            .map(|peers| peers.iter().map(|n| n.clone()).collect())
            .unwrap_or_default();

        if target_nodes.is_empty() {
            return Ok(());
        }

        let actor_id = ActorId::capability(meta.name);
        let envelope = CapabilityEnvelope {
            kind: EffectKind::Emit,
            payload,
        };
        let bytes = postcard::to_allocvec(&envelope)
            .map_err(|e| ActantError::Serialization(e.to_string()))?;

        for target_node in target_nodes {
            let bytes = bytes.clone();
            let actor_system = actor_system.clone();
            let actor_id = actor_id.clone();
            tokio::spawn(async move {
                if let Err(e) = actor_system
                    .call_remote(&target_node, actor_id, "invoke".to_string(), bytes)
                    .await
                {
                    tracing::warn!(
                        target_node = %target_node.as_str(),
                        capability = %meta.name,
                        error = %e,
                        "remote emit failed"
                    );
                }
            });
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
mod tests {
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
        let layer =
            Layer::<Store>::for_capability("Store", EffectKind::Perform).chain_erased(handler);
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
}
