# Rust API

Rust 公共 API 由 `///` 注释生成 rustdoc，不在本站重复书写签名。

## 生成

```bash
# 核心 crate
cargo doc --no-deps -p actant-core -p actant-common

# 浏览器打开
cargo doc --no-deps -p actant-core -p actant-common --open
```

输出位于 `target/doc/actant_core/` 与 `target/doc/actant_common/`。

## 公共扩展缝

Rust 侧公开 trait 共 9 个，它们是扩展缝的**实现面**；每个 trait 的注入入口、能否跨节点与运行期可否替换，见[框架契约](../architecture/framework-contract.md)的扩展缝表格。

- `Capability` / `CapabilityCodec` / `Handler`：ERH 能力声明、编解码与 handler 链
- `RoutePolicy`：放置策略（唯一有 Python→Rust 桥的缝，见 `src/py/route_policy.rs`）
- `Scheduler`、`TaskDispatcher`
- `Transport`、`Discovery`、`EventLog`

用 `RuntimeBuilder` 组装 Runtime。见 [框架契约](../architecture/framework-contract.md) 与 `tests/rust/unit/runtime/builder.rs`。Rust 缝一律在构造期注入，运行期没有 setter、不可替换；`RoutePolicy` 是唯一有 Python→Rust 桥的缝（callable，构造期二选一），`Scheduler` 仅向 Python 开放内置名称 `priority` / `fifo`，其余需 Rust 侧注入。

## 稳定性

Rust 嵌入是**实现细节**，无稳定性承诺，不做 dylib / WASM 插件。任务执行默认绑定 Python 进程池；Rust 原生执行器需自行实现 `TaskDispatcher`。
