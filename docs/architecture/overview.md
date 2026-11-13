# 架构总览

## 四盒 + ERH

Actant 的身份 = **P2P 无中心 fabric + ERH 能力分发 + 进程级隔离 + 持久化状态机**。
这里的「能力分发」指 `runtime` 域的用户态扩展；Rust 引擎内部的可替换策略另由
`RuntimeBuilder` 的编译期 trait 缝承担，两者见[框架契约](framework-contract.md)的扩展缝一节。

| 盒 | 内容 |
| --- | --- |
| Actor | Actor 运行时与 mailbox |
| State | LMDB 持久化、HLC、事件日志、快照与重放 |
| Effect / Capability | ERH 扩展层（`runtime` 域）与构造期定稿的 trait 缝（`build_time` 域） |
| Iroh | P2P 网络、发现、传输、blob |

## 三层代码结构

| 层级 | 位置 | 职责 | 边界约束 |
| --- | --- | --- | --- |
| 第 1 层 Rust 核心 | `crates/actant-common` + `crates/actant-core` | 共享类型 + 框架主体（Actor、DAG、调度、持久化、网络、Capability、事件总线）；载荷为不透明 `Vec<u8>` | 不感知任何 Python 概念 |
| PyO3 边界 | `src/`（`src/py/`） | Rust 与 Python 的唯一通道 | 封装 Rust 原语；maturin 编译入口 |
| 第 2 层 Python 封装 | `actant/` | `Runtime` / `Layer` / effect 原语 / capability 声明 / `@task` / `@flow` / 异常镜像 | cloudpickle 序列化 payload，Rust 不解析语义 |
| 第 3 层 用户扩展 | 仓库外 | 业务路由、定时任务、资源管理、监控、DSL 变体 | 由用户自行实现 |

依赖方向单向：`common ← core ← 门面`。CI 强制 `core` / `common` 零绑定层引用，`--no-default-features` 构建是一等公民。

## 扩展面全景

全部扩展共用 capability / handler / effect 词汇；统一的是词汇与边界，不是调用路径。按解析时机分两个执行域，细节见[框架契约](framework-contract.md)：

| 执行域 | 解析时机 | 典型扩展面 | 运行期成本 |
| --- | --- | --- | --- |
| 构造期定稿 | `build_time` | `RuntimeBuilder::with_*` 注入的 trait 缝、`PyRoutePolicy` 桥 | 一次 vtable 调用；无 PyO3 setter，不可热替换 |
| 运行期解析 | `runtime` | `rt.layer(...).chain(...)` 的 capability handler | handler 链 + 注册表查表 / 编解码 / Actor 邮箱往返 |

路由、调度、任务分发、传输广播与事件日志落盘位于每任务 / 每消息路径（`hot_path=true`），一律在构造期定稿；`ask` / `perform` / `emit` 只用于数据面与观测面。具体条目、验收强度与未覆盖方向以清单为准，边界与理由见[框架契约](framework-contract.md)。

## 核心原则

简单是可靠的前提。新功能引入前必须能回答“为什么放这一层”：

- 策略 → 第 3 层。
- 能用现有原语组合实现 → 不新增。
- 只对部分用户有用 → 第 3 层。
- 绕过路径：用户可直接基于 `actant.actant`（PyO3 导出）写自己的封装，第 2 层不是必经之路。

## Rust 模块地图

`actant-core` 内部按职责划分：

| 模块 | 职责 |
| --- | --- |
| `runtime::actor` | Actor trait、邮箱、ActorSystem facade |
| `runtime::state` | LMDB store、HLC 与事件日志 |
| `runtime::capability` | Capability / Handler / Layer（`runtime` 域；Rust 核心内无生产调用者，由 PyO3 桥发起） |
| `runtime::network` | iroh 传输、gossip topic、直连请求响应 |
| `runtime::workflow` | DAG、Orchestrator、Worker、调度、故障转移与 gossip |
| `runtime::builder` | 将上述子系统按依赖顺序装配成 `Runtime` |
| `metrics` / `observability` | 指标与 tracing 初始化 |

`runtime::workflow` 的子模块：`dag`（DAG 与执行状态）、`orchestrator`（状态机与事件溯源）、`scheduler`（调度策略，见[调度器](scheduler.md)）、`failover`（租约故障转移，见[故障转移](failover.md)）、`gossip`（DAG 状态复制）、`actor`（Actor 化封装）、`messaging`（消息编解码）、`runtime`（Worker 执行循环）。

### Actor 运行时

Actor 是统一执行引擎：DAG、状态机、事件溯源都是 Actor 特例。

| 类型 | 职责 |
| --- | --- |
| `Actor` trait | 系统 Actor 实现的唯一抽象 |
| `ActorContext` | Actor 实例生命周期上下文 |
| `MailboxRegistry` | Actor 邮箱注册表（纯内存投递） |
| `ActorSystem` | 对外 facade：spawn / send / call / stop |

子模块：`runtime`（trait + context）、`mailbox`（注册表）、`system`（facade + 私有运行循环）。

定位：**系统 Actor 专用的本地运行时**。不做任何持久化——工作流恢复由 orchestrator 的统一工作流历史唯一承载，不存在 mailbox 重放等第二恢复路径。

## 绑定壳约束

`src/`（`src/py/`）把 `actant-core` 的原语封装为 Python 对象，导出模块名为 `actant.actant`（maturin `module-name`）。约束：

- `actant-core` 不依赖 Python 类型或 GIL；绑定壳单向依赖它。
- Python handler 只存在于 Python 侧分发链，不进入 Rust capability / dispatcher（见[框架契约](framework-contract.md)）。
- 跨节点消息必须经过 `common::WireEnvelope` 或 payload signing / verification。

## 目录结构

```text
actant/
├── crates/
│   ├── actant-common/      # 共享类型：协议、ID、配置、wire、错误
│   └── actant-core/        # 框架主体：actor / workflow / state / network / capability
├── src/                    # PyO3 绑定壳（module-name = actant.actant）
├── actant/                 # Python 公共 API 与 ERH 扩展模型
├── extensions/             # 可选扩展包（act-service：L2 服务层；act-ext-testkit：测试台）
├── examples/               # 可运行示例
├── benches/                # Python 与 Rust 基准测试
├── tests/python/           # Python 测试（unit / integration / e2e）
└── tests/rust/             # Rust 测试（unit / property）
```

## 技术栈

Rust：tokio、iroh、iroh-blobs、heed/LMDB、rkyv、postcard、PyO3。Python：>= 3.10、cloudpickle。

## 下一步

- [ERH 扩展模型](erh.md)
- [框架契约](framework-contract.md)：扩展缝、执行域与性能边界
- [Worker 运行模型](worker-model.md)
- [调度器](scheduler.md)
- [故障转移](failover.md)
- [恢复语义](recovery.md)
- [服务层](service-layer.md)
