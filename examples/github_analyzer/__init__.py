"""Actant 演示示例：多仓库 GitHub issues/PRs 分析器。

.. warning::
    **本示例仅供演示 ERH 架构用法，不构成生产就绪代码。** 具体限制：

    - **无认证管理**：仅使用环境变量 ``GITHUB_TOKEN``，无 token 轮换、过期处理
    - **无持久化恢复**：缓存为本地文件，崩溃后无法恢复中间状态
    - **无错误隔离**：单个仓库拉取失败会触发重试但不会跳过继续后续仓库
    - **无指标导出**：``Metrics`` capability 仅在内存累计，不对接 Prometheus
    - **无并发控制**：未限制并发 HTTP 请求数（依赖 GitHub API 速率限制）
    - **无安全审计**：``Audit`` handler 仅追加写入文件，无日志轮转/压缩

    如需生产部署，应补充上述缺失项并参考 Actant 的 failover、WAL、
    capacity_callback 等机制。

用真实的 GitHub REST API 数据验证 ERH 架构在场景下的可行性，覆盖：

- ``ask`` 决策型：``Routing``（按仓库 owner 路由到分析节点）、``Scheduling``（PR 优先于 issue）、
  ``RetryPolicy``（HTTP 失败指数退避重试）、自定义 ``Throttle``（GitHub 速率限制保护）
- ``perform`` 副作用型：自定义 ``Fetch``（urllib 真实 HTTP + 本地缓存）、``Analyze``（分仓库聚合）、
  ``ResultStore``（报告落盘 report.txt + report.json）
- ``emit`` 反应型：自定义 ``Metrics``（计数器采集）、``Audit``（审计事件落盘）
- handler 内组合 effect：重试决策 handler 触发审计事件
- 多 handler 链：自定义 handler 覆盖默认（后注册=高优先级）

所有数据真实来自 GitHub API（首次拉取，本地缓存避免触发 60 req/hour 未认证速率限制），
无桩函数、无 mock、无虚假数据。

运行::

    uv run python -m examples.github_analyzer

可选环境变量::

    GITHUB_TOKEN=ghp_xxx      # 提高 API 速率限制到 5000 req/hour（可选）
    ACTANT_GH_REPOS=owner1/repo1,owner2/repo2   # 覆盖默认分析仓库列表
"""

from __future__ import annotations

from .pipeline import main

__all__ = ["main"]
