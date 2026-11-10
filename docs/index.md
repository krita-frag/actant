# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

Actant 用 Rust + iroh 构建核心运行时，通过 PyO3 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理与持久化能力。

!!! warning "实验性项目"
    API 尚未冻结，1.0.0 前不保证向后兼容。任何 0.x 版本升级都应视为潜在破坏性变更，请先阅读变更日志。

## 从哪里开始

| 我想… | 去哪里 |
| --- | --- |
| 安装并跑通第一个任务 | [快速开始](start/quickstart.md) |
| 理解 `Runtime` / `@task` / `@flow` | [核心概念](start/concepts.md) |
| 查某个函数或类的签名 | [Python API](reference/python-api.md) · [PyO3 绑定 API](reference/pyo3-api.md) · [CLI](reference/cli.md) |
| 扩展 capability 或写自定义 handler | [能力扩展](guide/capabilities.md) |
| 查有哪些扩展点、哪些能从 Python 覆盖 | [扩展点清单](reference/extension-points.md) · `actant extensions` |
| 部署多节点、配置发现与信任 | [部署与发现](guide/deployment.md) |
| 理解分层、ERH、Worker 与持久化 | [架构总览](architecture/overview.md) |
| 排查超时、取消、重启恢复问题 | [排障](operations/troubleshooting.md) |
| 参与开发 | [开发环境](contributing/dev-setup.md) |

## 文档结构

- **入门**：安装、5 分钟上手、核心概念。
- **使用指南**：任务、工作流、能力扩展、部署。
- **API 参考**：Python / Rust 公共接口、CLI、配置，由源码生成；[扩展点清单](reference/extension-points.md)由清单生成。
- **架构**：分层、ERH 扩展模型、Worker 运行模型、调度器、故障转移、恢复语义、框架契约（扩展面边界与执行域）。
- **运维**：SLA 行为基线、排障。
- **贡献**：开发环境、测试、代码约定、文档地图、[扩展点维护](contributing/extensions.md)。

## 最小示例

```python
from actant import Runtime, task

@task
def add(a: int, b: int) -> int:
    return a + b

with Runtime() as rt:
    print(add.submit(2, 3).result())  # 5
```

下一步：[快速开始](start/quickstart.md)。
