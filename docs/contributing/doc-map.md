# 文档地图

本页定义“某个事实应该写在哪里”。原则：**同一事实只允许一个权威源，其他位置只链接或生成。**

## 权威源

| 事实 | 唯一权威源 | 展示位置 | 禁止 |
| --- | --- | --- | --- |
| Python 签名、默认值、异常 | 源码 docstring + `actant/actant.pyi` | [Python API](../reference/python-api.md) · [PyO3 绑定 API](../reference/pyo3-api.md)（生成） | 手写签名表 |
| Rust 公共 API | `///` 注释 | rustdoc（见 [Rust API](../reference/rust-api.md)） | 在 Markdown 复写 trait 签名 |
| 配置项与默认值 | `_ActantConfig` / `_NetworkConfig` 签名 | [配置项](../reference/configuration.md)（生成） | 多处维护参数表 |
| CLI 参数 | `actant/cli.py` 的 argparse | [CLI](../reference/cli.md)（生成） | 口头描述 |
| 扩展点条目、解析时机、热路径标记、验收等级 | `actant/extension_points.json` | [扩展点清单](../reference/extension-points.md)（生成）· [扩展点维护](extensions.md) | 在架构页 / 指南页复写扩展点表格或字段取值 |
| 架构决策与边界 | `docs/architecture/*.md` | 本站架构章节 | README / AGENTS 复述 |
| 行为承诺 | 测试断言 | [SLA 基线](../operations/sla-baseline.md) | 只写数字不写来源 |
| 协议版本 | wire 常量 + `CHANGELOG.md` | [框架契约](../architecture/framework-contract.md) | 分散在注释里 |
| 变更历史 | `CHANGELOG.md` | 发布说明 | 每个页面各写一份 |

## 新增内容放哪里

- **入门/教程** → `docs/start/`
- **使用方式、最佳实践** → `docs/guide/`
- **由代码生成的内容** → `docs/reference/`，只放生成指令，不放手写签名
- **分层、协议、持久化、扩展缝** → `docs/architecture/`；执行域边界与性能裁决只写[框架契约](../architecture/framework-contract.md)
- **扩展点条目本身** → 只改 `actant/extension_points.json`，字段含义、生成链与验收流程见[扩展点维护](extensions.md)
- **SLA、混沌、排障** → `docs/operations/`
- **开发流程、测试、风格** → `docs/contributing/`
- **内部计划、一次性分析** → 留在 `plans/` 或 `.trae/`，不进入站点

## 评审检查

1. 这个事实是否已经在别处出现？如果是，改成链接。
2. 改的是 API 吗？如果是，改 docstring / `.pyi`，不要改参考页。
3. 改的是架构决策吗？如果是，只改一个架构页，并检查其他页面是否引用了旧结论。
4. 改的是扩展点吗？如果是，走[扩展点维护](extensions.md)的四步流程，确认生成页、`actant extensions` 与门禁同步，且没有在别处复写清单表格。
5. 链接是否可达？`mkdocs build --strict` 会拦截死链。

## Rust 文档覆盖现状

`missing_docs` 尚未作为 CI 门禁启用：当前公开项缺口为 `actant-common` 227 处、`actant-core` 791 处，一次性补齐会产生无法评审的改动。分阶段计划：

1. 先补 `actant-common`（`common/model.rs`、`common/wire.rs`、`common/config.rs` 占多数），补齐后在 `crates/actant-common/src/lib.rs` 加 `#![warn(missing_docs)]`。
2. 再按模块推进 `actant-core`（`runtime/capability`、`runtime/workflow`、`metrics` 优先），全部清零后启用 crate 级 lint。
3. 最后把 `RUSTFLAGS="-D missing_docs"` 加入 CI 的 clippy 步骤，防止回退。

统计命令：

```bash
RUSTFLAGS="-W missing_docs" cargo check -p actant-common -p actant-core --no-default-features 2>&1 \
  | grep "missing documentation" | wc -l
```
