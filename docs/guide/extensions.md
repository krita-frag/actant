# 扩展作者指南

本页面向用 Python 编写 Actant 扩展的开发者。它只说明**通用契约与装配方式**；
扩展缝的完整边界、注入入口与执行域裁决以[框架契约](../architecture/framework-contract.md)为唯一权威源。

## 定位

`actant.ext` 是稳定、类型化、可测试的便利契约：只做适配与显式装配，不新增
Rust 缝，也不做插件自动发现或热插拔。完整边界见[框架契约](../architecture/framework-contract.md)。

## 选择扩展点

按顺序选择，能组合就不新增抽象：

1. **任务/工作流**：能否表达为一个 `@task` 或 `@flow`？
2. **标签与配额**：能否用现有放置词汇表达节点诉求？
3. **capability handler**：是否需要新的 `ask` / `perform` / `emit`？
4. **`route_policy`**：是否需要自定义候选排序/选择？
5. **Rust 宿主**：以上都无法满足且不改 `actant-core` 时，才在独立 Rust 宿主实现 trait。

## 类型化放置策略

`route_policy` 的 callable 接收裸 `dict`。用 `actant.ext` 获得稳定类型：

```python
from actant import Runtime
from actant.ext import (
    PlacementContext,
    PlacementDecision,
    NodeView,
    bind_placement_policy,
)


def prefer_least_loaded(
    ctx: PlacementContext, nodes: list[NodeView]
) -> PlacementDecision:
    if ctx.local_draining or not nodes:
        return PlacementDecision.defer("local draining or no candidates")
    target = max(nodes, key=lambda node: node.available_slots)
    return PlacementDecision.remote(target.node_id)


rt = Runtime(route_policy=bind_placement_policy(prefer_least_loaded))
```

约束：

- 标签与配额是 Rust 侧的基础约束；放置策略只做优化，**正确性不能依赖它**。
- Rust 热路径拿不到 GIL、策略抛异常或返回非法值时，会回退内置策略。
- `PlacementDecision` 只表达 `auto` / `local` / `remote` / `defer`，与 Rust 决策一一对应。

## 事件信封

`TaskLifecycle` / `WorkflowLifecycle` 事件可直接消费，也可以归一化为
`EventEnvelope`：

```python
from actant.ext import EventAdapter, EventEnvelope

adapter = EventAdapter(source="node-a")
subscription = adapter.subscribe(rt, lambda envelope: print(envelope.kind))
```

- `kind` 形如 `task.completed` / `workflow.started`。
- `event_id` 由事件内容确定性生成，可用于消费端幂等。
- `payload` 已递归转换为 JSON 友好值；`bytes` 编码为 `{"__bytes__": "<base64>"}`。
- 事件是通知，不是 exactly-once；状态事实源仍应由使用方自己的存储承担。
- 在 `Runtime.start()` 之后订阅会收到告警；需要完整生命周期时应在 start 前装配。

`subscription.close()` 幂等移除 handler。

## 集群视图

`ClusterView` 只读封装 `Runtime.peers()`：

```python
from actant.ext import ClusterView

view = ClusterView(rt)
for node in view.nodes():
    print(node.node_id, node.labels, node.available_slots)

for change in view.watch(interval_s=1.0):
    print("joined", change.joined, "left", change.left, "changed", change.changed)
```

- `active_workflows` 是在途工作流 ID 元组；`available_quotas` 是剩余容量。
- `watch()` 只报告成员与容量/标签/角色变化，不把每次心跳当作变化。
- 轮询间隔与停止由调用方控制；这是便利层，不改变成员协议。

## 显式组件装配

`compose` 只负责启动/逆序停止，不做自动发现：

```python
from actant.ext import Component, compose


class MetricsBridge:
    def start(self, runtime) -> None:
        ...

    def stop(self) -> None:
        ...


composition = compose(rt, MetricsBridge())
try:
    ...
finally:
    composition.stop()
```

启动失败时会逆序停止已启动组件，并重抛原始异常。

## 公共配置构建器

避免直接依赖 `_ActantConfig` / `_NetworkConfig`：

```python
from actant.ext import runtime_config

cfg = (
    runtime_config()
    .role("worker")
    .labels({"pool": "a"})
    .quotas({"slot": 4})
    .network(preset="none", listen_port=41000)
    .worker(max_concurrent_tasks=4)
    .build()
)
```

构建器只包装现有字段并做早失败校验；不新增运行时语义。

## 边界与版本

- `actant.ext` 是便利契约，不是新的核心扩展缝；真正的缝见[框架契约](../architecture/framework-contract.md)。
- `EventEnvelope.schema_version` 用于消费端演进，未知字段应忽略。
- 扩展应显式声明依赖的 `actant` 版本并做启动检查，不要依赖下划线模块。
- 一致性测试建议复用 `act-ext-testkit`，覆盖装配顺序、事件可见性与多后端语义。
