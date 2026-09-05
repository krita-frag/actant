<!--
本仓库遵循"减法守则"（docs/ROADMAP.md §二）：公开 API 面只许收缩不许净扩张，
每阶段交付物必须同时包含"删了什么"与"加了什么"。请如实填写删加对称检查，
门禁清单全部通过后再请求 review。
-->

## 变更摘要

<!-- 一段话说清这个 PR 做了什么、为什么。 -->

## 删加对称检查（ROADMAP 减法守则 2/3）

| 项 | 值 |
|----|-----|
| 删除行数 / 新增行数 | `git diff --stat` 摘录：____ 删 / ____ 增 |
| 公开 API 面变化 | 无 / 收缩（列出删除项）/ 扩张（**需说明等量删除的对价**） |
| 是否需要 CHANGELOG 破坏性变更条目 | 是（已置顶）/ 否 |
| 本 PR 删除了什么 | <!-- 无删除的 PR 原则上不予合并（重构/修复类酌情） --> |

## 变更分层

<!-- 对应"这段必须进核心吗"自查（ROADMAP §五.5）。 -->

- [ ] 第 1 层（src/，Rust 核心）：不感知 Python 概念 / 本 PR 未触及
- [ ] 第 2 层（actant/，Python 封装）/ 本 PR 未触及
- [ ] 第 3 层（examples/ 或仓库外，策略与扩展）/ 本 PR 未触及
- [ ] 纯测试 / CI / 文档

## 门禁清单（docs/PLAN.md §5.3）

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets -- -D warnings`（零告警）
- [ ] `cargo test`
- [ ] `cargo test --no-default-features`
- [ ] `uv run pytest tests/python/ -n 4`
- [ ] `uv run ruff check actant tests/python benches/`
- [ ] `uv run mypy actant`
- [ ] 涉及核心增量时：PR 内显式拆分"核心原语 + 外围能力"并附删除清单

## 质量自查

- [ ] 新增测试确定性（无固定 sleep 时序；外部依赖已 mock；轮询断言 + 总超时）
- [ ] 无桩函数 / 占位实现 / `pass` / `NotImplementedError`，错误不吞
- [ ] Rust 侧无生产 `unwrap()`/`expect()`，无 `let _ =` 丢弃 Result
- [ ] 注释只描述当前实现与设计理由，不含变更历史/备忘
- [ ] 文档同步（CHANGELOG.md / docs/PLAN.md / AGENTS.md，如适用）
