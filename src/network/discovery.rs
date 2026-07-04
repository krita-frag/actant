//! Peer 发现策略抽象。
//!
//! [`Discovery`] 抽象了 iroh endpoint 如何发现 peer。Rust 核心保持通用；
//! 具体策略在启动时通过 [`crate::common::DiscoveryMode`] 字符串经
//! [`discovery_from_name`] 选择。
//!
//! # 内置策略
//!
//! | 名称       | 类型                  | 状态 | 用途                                               |
//! |-----------|------------------------|------|----------------------------------------------------|
//! | `none`    | [`NoDiscovery`]        | ✅ | 测试 / 固定拓扑；显式拨号 peer。                       |
//! | `local`   | [`LocalDiscovery`]    | ✅ | 默认。n0 preset（DNS Pkarr + relay 回退）。          |
//! | `mdns`    | [`MdnsDiscovery`]     | ✅ | LAN。n0 preset 禁用 relay。                          |
//! | `dns`     | [`DnsDiscovery`]      | 🚫 0.1.0 | 预留，启动时返回 `ConfigError`（见 [#0.2]）。      |
//! | `relay`   | [`RelayDiscovery`]    | 🚫 0.1.0 | 预留，启动时返回 `ConfigError`（见 [#0.2]）。      |
//!
//! `dns` 与 `relay` 在 0.1.0 中**不作为有效 preset 注册**：
//! 结构体与构造器保留在源码中供 0.2 实现，但启动配置若指定这两个名称，
//! [`DiscoveryMode::parse`] 会明确返回 `ConfigError`，避免用户误以为
//! 配置的 `domain` / `relay_url` 已生效。完整自定义 DNS 解析与自托管
//! relay 配置为 0.2 特性。
//!
//! 额外策略可通过 [`register_discovery`] 在运行时注册，
//! 允许 Python 层（或任何嵌入应用）引入自定义发现而无需触碰 Rust 核心。
//!
//! # 性能埋点
//!
//! 每个策略的 `apply()` 都包装在 `tracing` span 中，
//! `RUST_LOG=actant::network::discovery=trace` 可在 endpoint 构建期间
//! 揭示 per-strategy 开销。

use std::sync::{Arc, OnceLock};

use iroh::endpoint::presets::Preset;
use iroh::endpoint::Builder;

use crate::common::discovery_mode;

/// 在 iroh endpoint 上配置 peer 发现的 trait。
///
/// 实现接收一个最小配置的 iroh `Builder`（已设置 crypto provider），
/// 返回应用了发现机制的 builder。
pub trait Discovery: std::fmt::Debug + Send + Sync + 'static {
    /// 将此发现机制应用到 iroh endpoint builder。
    ///
    /// 输入 builder 已设置 crypto provider；实现负责添加地址查找服务和 relay 配置。
    fn apply(&self, builder: Builder) -> Builder;

    /// 此发现策略的可读名称。
    fn name(&self) -> &'static str;
}

/// 无自动发现。
///
/// Peer 必须通过 `bootstrap_nodes` 或 `dial()` 显式连接。
/// 适用于测试、CI 和所有 peer 地址已预先可知的受控环境。
/// endpoint 仅绑定 loopback，从不联系任何外部服务 — 启动时间亚毫秒级。
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDiscovery;

impl Discovery for NoDiscovery {
    #[tracing::instrument(name = "discovery.none", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        // Minimal 仅设置 crypto provider — 无地址查找，无 relay。
        iroh::endpoint::presets::Minimal.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::NONE
    }
}

/// 使用 iroh n0 preset 的本地发现。
///
/// 配置通过 Pkarr 发布到 n0 DNS 服务器的 DNS 地址查找和 relay 服务器回退。
/// 适用于面向互联网的节点和大多数生产部署。
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalDiscovery;

impl Discovery for LocalDiscovery {
    #[tracing::instrument(name = "discovery.local", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        iroh::endpoint::presets::N0.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::LOCAL
    }
}

/// 基于 mDNS 的本地网络发现。
///
/// 使用 iroh n0 preset 禁用 relay，适用于节点共享本地网段的 LAN 部署。
#[derive(Debug, Clone, Copy, Default)]
pub struct MdnsDiscovery;

impl Discovery for MdnsDiscovery {
    #[tracing::instrument(name = "discovery.mdns", level = "debug", skip_all)]
    fn apply(&self, builder: Builder) -> Builder {
        iroh::endpoint::presets::N0DisableRelay.apply(builder)
    }

    fn name(&self) -> &'static str {
        discovery_mode::MDNS
    }
}

/// [`DnsDiscovery`] 查询的 DNS 记录类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsRecordType {
    /// 查询 `_actant._tcp.<domain>` 形式的 SRV 记录。
    Srv,
    /// 查询 `<domain>` 的 A/AAAA 记录。适用于解析为多个 pod IP 的 Kubernetes Headless Service。
    A,
    /// 查询 `<domain>` 的 TXT 记录。TXT 值应携带 iroh `EndpointId`（十六进制编码）。
    Txt,
}

impl std::fmt::Display for DnsRecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DnsRecordType::Srv => f.write_str("srv"),
            DnsRecordType::A => f.write_str("a"),
            DnsRecordType::Txt => f.write_str("txt"),
        }
    }
}

/// 面向企业 / Kubernetes 部署的 DNS 发现（0.2 预留）。
///
/// 从 DNS 记录（SRV / A / TXT）解析 peer 地址。与 K8s Headless Service 配合良好：
/// 每个 pod 通过 TXT 记录发布其 iroh `EndpointId`，service DNS 名解析 A 记录为 pod IP。
///
/// # 0.1.0 状态
///
/// 此类型在 0.1.0 中**不作为有效发现 preset 注册**，也不实现 [`Discovery`]：
/// 若启动配置指定 `preset = "dns"`，[`DiscoveryMode::parse`] 会返回
/// `ConfigError`。结构体保留在源码中，供 0.2 注入自定义 `DnsResolver`
/// 后实现 [`Discovery`] trait。
#[derive(Debug, Clone)]
pub struct DnsDiscovery {
    /// 要查询的 DNS 域名（如 `actant-nodes.default.svc.cluster.local`）。
    pub domain: String,
    /// 使用的 DNS 记录类型。
    pub record_type: DnsRecordType,
}

impl DnsDiscovery {
    /// 创建查询 `domain` 下 SRV 记录的 DNS 发现。
    pub fn srv(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            record_type: DnsRecordType::Srv,
        }
    }

    /// 创建查询 `domain` A/AAAA 记录的 DNS 发现。
    pub fn a(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            record_type: DnsRecordType::A,
        }
    }

    /// 创建查询 `domain` TXT 记录的 DNS 发现。
    pub fn txt(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            record_type: DnsRecordType::Txt,
        }
    }
}

// 0.1.0 不实现 `Discovery`：避免生产代码中存在未消费配置字段的桩实现。
// 结构体保留，供 0.2 注入自定义 DnsResolver 后重新实现该 trait。

/// 仅 relay 发现（0.2 预留）。
///
/// 配置单个自定义 relay URL，不使用 n0 的 DNS Pkarr 发布。
/// 适用于以自托管 relay 为唯一会合机制的跨 NAT 部署。
///
/// # 0.1.0 状态
///
/// 此类型在 0.1.0 中**不作为有效发现 preset 注册**，也不实现 [`Discovery`]：
/// 若启动配置指定 `preset = "relay"`，[`DiscoveryMode::parse`] 会返回
/// `ConfigError`。结构体保留在源码中，供 0.2 注入自定义 `RelayMap`
/// 后实现 [`Discovery`] trait。
#[derive(Debug, Clone)]
pub struct RelayDiscovery {
    /// relay 服务器 URL（如 `https://relay.mycompany.com`）。
    pub relay_url: String,
}

// 0.1.0 不实现 `Discovery`：避免生产代码中存在未消费配置字段的桩实现。
// 结构体保留，供 0.2 注入自定义 RelayMap 后重新实现该 trait。

/// 装箱的类型擦除发现策略。
#[derive(Debug, Clone)]
pub struct BoxedDiscovery(Arc<dyn Discovery>);

impl BoxedDiscovery {
    pub fn new<D: Discovery>(discovery: D) -> Self {
        Self(Arc::new(discovery))
    }
}

impl Discovery for BoxedDiscovery {
    fn apply(&self, builder: Builder) -> Builder {
        self.0.apply(builder)
    }

    fn name(&self) -> &'static str {
        self.0.name()
    }
}

/// 创建发现策略的工厂函数类型。
pub type DiscoveryFactory = Arc<dyn Fn() -> BoxedDiscovery + Send + Sync>;

/// 全局注册表，将发现模式名映射到工厂函数。
///
/// 新发现策略可在启动时通过 [`register_discovery`] 注册，
/// 允许 Python 层引入自定义发现机制而无需触碰 Rust 核心代码。
fn registry() -> &'static parking_lot::RwLock<std::collections::HashMap<String, DiscoveryFactory>> {
    static REGISTRY: OnceLock<
        parking_lot::RwLock<std::collections::HashMap<String, DiscoveryFactory>>,
    > = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut map = std::collections::HashMap::new();
        // 注册内置发现。
        map.insert(
            discovery_mode::NONE.to_string(),
            Arc::new(|| BoxedDiscovery::new(NoDiscovery)) as DiscoveryFactory,
        );
        map.insert(
            discovery_mode::LOCAL.to_string(),
            Arc::new(|| BoxedDiscovery::new(LocalDiscovery)) as DiscoveryFactory,
        );
        map.insert(
            discovery_mode::MDNS.to_string(),
            Arc::new(|| BoxedDiscovery::new(MdnsDiscovery)) as DiscoveryFactory,
        );
        // 0.1.0 不注册 `dns` / `relay`：二者为预留实现，apply() 尚未消费配置字段。
        // 指定这两个名称会在 DiscoveryMode::parse 处返回 ConfigError，避免用户误用。
        // 0.2 将在此注册完整实现。
        parking_lot::RwLock::new(map)
    })
}

/// 以给定名称注册自定义发现策略。
///
/// 若同名策略已存在，将被替换。
/// 允许 Python 层（或任何嵌入应用）引入新发现机制而无需修改 Rust 核心。
#[allow(dead_code)] // 公共扩展点 — 供外部 / 嵌入调用方使用
pub fn register_discovery(name: impl Into<String>, factory: DiscoveryFactory) {
    let mut guard = registry().write();
    guard.insert(name.into(), factory);
}

/// 若名为 `name` 的发现策略已注册则返回 `true`。
///
/// 由 [`crate::common::DiscoveryMode::validate`] 使用，在启动时拒绝未知
/// 发现名称，而非静默回退。
pub fn is_registered(name: &str) -> bool {
    registry().read().contains_key(name)
}

/// 返回已注册发现名称的排序列表。
///
/// 用于配置错误消息中枚举有效选项。
pub fn registered_names() -> Vec<String> {
    let mut names: Vec<String> = registry().read().keys().cloned().collect();
    names.sort();
    names
}

/// 从字符串名创建发现策略。
///
/// 在全局注册表中查找名称。若未注册返回
/// [`crate::common::ActantError::Config`] — 不静默回退。
/// 接收不可信来源配置值的调用方应先通过
/// [`crate::common::DiscoveryMode::validate`] 验证。
pub fn discovery_from_name(name: &str) -> Result<BoxedDiscovery, crate::common::ActantError> {
    let _span = tracing::debug_span!("discovery.resolve", name = name).entered();
    let guard = registry().read();
    if let Some(factory) = guard.get(name) {
        Ok(factory())
    } else {
        Err(crate::common::ActantError::Config(format!(
            "unknown discovery mode '{}': expected one of {}",
            name,
            registered_names().join(", ")
        )))
    }
}
