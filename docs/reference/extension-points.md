# 扩展点清单

> 本页由 `scripts/gen_api_reference.py` 从源码生成，请勿手工编辑。

单一事实源：`actant/extension_points.json`。生成器、CLI 与 `tests/python/unit/test_extension_contract.py` 都读这份数据。

本页列出**当前可用**的扩展点，不表示某个 Runtime 实例已注入某个实现。Python 无从得知 Rust 编译期注入了哪个 trait 实现；命令行用 `actant extensions` 查询同一份清单。

## 登记判据

每个条目必须同时具备七项，缺一不允许登记：

1. `entrypoint` 与 `production_call_site` 指向真实存在的生产符号（不得是测试或示例）；`production_call_site` 记录**装配 / 绑定位置**，不表示运行期调用频率；
2. `cross_node_safe` 按层固定：rust 为 `true`，bridge / python 为 `false`；
3. `acceptance_test` 指向真实存在的测试或由 CI 运行的 example；
4. `acceptance_level` 显式声明验收强度，未达到 behavior 的条目不得宣称已通过完整生产路径验证；
5. 接线类验收不直接调用被接线函数；
6. `resolution` 与 `hot_path` 组合合法：热路径点必须是 `build_time`；
7. 热路径白名单文件不得出现 capability 动态分派调用或 GIL 附加，由 `test_extension_contract.py` 静态扫描。

验收等级：`behavior` / `wiring` / `unit`。`behavior` = 真实驱动生产路径的 e2e，断言注入实现影响了引擎行为；`wiring` = 经装配或公共入口证明注入实现被调用，未跑完整任务链路；`unit` = 只验证实现自身，不经注入路径。低于 behavior 的条目仍是可用扩展点，但差距在「尚未提供契约的扩展意图」里显式列出。

## 执行域

统一的是**契约**（清单、词汇、门禁、验收），不是调用路径。

`resolution` 取值：`build_time` = 构造 / 装配期定稿为 trait 句柄，运行期不再查注册表；`runtime` = 运行期经 handler 链动态解析，可增删覆盖。

`hot_path` 表示实现位于 Rust 引擎的每任务 / 每消息路径：路由、调度、任务分发、传输广播、事件日志落盘。这类点在构造期二选一并冻结为 `Arc<dyn Trait>` 句柄，运行期只有一次 vtable 调用，不生成 PyO3 setter；Python 侧仍可在 `actant extensions` 查到它们，但不能在运行期替换。构造期定稿后不再解析实现，换实现必须重建 Runtime：「查得到」不等于「运行期装得上」。

`CapabilityRuntime` 的 `ask` / `perform` / `emit` 需经注册表查表、编解码与 Actor 邮箱往返，只用于 `resolution=runtime` 的数据面与观测面，不进入热路径。这一边界由静态扫描守住，不依赖性能阈值断言。

## `rust` 层

编译期 trait，经 `RuntimeBuilder::with_*` 注入；实现随引擎部署，failover 后仍在本节点可用。

| id | 入口 | 生产调用点 | 跨节点安全 | 解析时机 | 热路径 | 验收强度 | 验收 | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `scheduler` | `crates/actant-core/src/runtime/builder.rs::with_scheduler` | `crates/actant-core/src/runtime/builder.rs::init_worker` | 是 | `build_time` | 是 | `wiring` | `tests/rust/unit/runtime/builder.rs::build_with_injected_scheduler_is_authoritative` | 替换内置 priority / fifo 调度器 |
| `route_policy` | `crates/actant-core/src/runtime/builder.rs::with_route_policy` | `crates/actant-core/src/runtime/workflow/runtime.rs::route_task` | 是 | `build_time` | 是 | `behavior` | `tests/python/integration/test_route_policy_python.py::TestPythonRoutePolicy::test_callable_is_invoked_and_task_completes` | 替换远端放置策略（RouteDecision 三态） |
| `task_dispatcher` | `crates/actant-core/src/runtime/builder.rs::with_task_dispatcher` | `crates/actant-core/src/runtime/workflow/runtime.rs::run_task_execution_loop` | 是 | `build_time` | 是 | `behavior` | `examples/rust_embed.rs::main` | 替换任务执行体分发（进程内 / shell / 任意语言 worker） |
| `transport` | `crates/actant-core/src/runtime/builder.rs::with_transport` | `crates/actant-core/src/runtime/builder.rs::build` | 是 | `build_time` | 是 | `wiring` | `tests/rust/unit/runtime/builder.rs::build_with_injected_transport_uses_it_as_live_network` | 替换内置 NetworkManager 传输层 |
| `discovery` | `crates/actant-core/src/runtime/builder.rs::with_discovery` | `crates/actant-core/src/runtime/builder.rs::build` | 是 | `build_time` | 否 | `wiring` | `tests/rust/unit/runtime/builder.rs::build_applies_injected_discovery` | 替换 config.network.discovery_mode 选择的内置发现策略 |
| `event_log` | `crates/actant-core/src/runtime/builder.rs::with_event_log` | `crates/actant-core/src/runtime/builder.rs::init_orchestrator` | 是 | `build_time` | 是 | `behavior` | `tests/rust/unit/runtime/builder.rs::injected_event_log_receives_workflow_submission` | 替换按 store 有无选择的 LMDB / Memory 事件日志 |

## `bridge` 层

Python 代码到 Rust 热路径或提交路径的桥；实现是进程内对象，failover 后不随工作流迁移。

| id | 入口 | 生产调用点 | 跨节点安全 | 解析时机 | 热路径 | 验收强度 | 验收 | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `py_route_policy` | `src/py/route_policy.rs::PyRoutePolicy` | `src/py/runtime.rs::new` | 否 | `build_time` | 是 | `behavior` | `tests/python/integration/test_route_policy_python.py::TestPythonRoutePolicy::test_callable_is_invoked_and_task_completes` | Python callable 接入 Rust 热路径；必须 try_attach 避开 GIL / tokio 互等死锁 |
| `submit_backend` | `actant/_internal/tasks/backend.py::use_submit_backend` | `actant/_internal/tasks/task.py::_submit` | 否 | `runtime` | 否 | `wiring` | `tests/python/unit/test_task_obj.py::test_task_submit_uses_registered_backend` | 替换 Task.submit 的提交后端（Python 侧唯一提交路径替换点） |

## `python` 层

用户态 capability handler，经 `rt.layer(name).chain(handler)` 登记；只作用于 Python 侧发起的 effect。

| id | 入口 | 生产调用点 | 跨节点安全 | 解析时机 | 热路径 | 验收强度 | 验收 | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Store` | `actant/_internal/runtime.py::layer` | `actant/_internal/runtime_base.py::_store_put` | 否 | `runtime` | 否 | `behavior` | `tests/python/integration/test_store_capability_override.py::test_engine_persistence_goes_through_python_store_handler` | 覆盖 Python 侧发起的 KV 写入；不改变 Rust 内部直接使用 LMDB 的路径 |
| `ValueStore` | `actant/_internal/runtime.py::layer` | `actant/_internal/tasks/ref.py::_value_store` | 否 | `runtime` | 否 | `unit` | `tests/python/unit/test_ref.py::test_value_store_handler_override` | 覆盖大结果 blob 桥的默认 handler |
| `TaskLifecycle` | `actant/_internal/runtime.py::layer` | `actant/_internal/tasks/helpers.py::_emit_task_event` | 否 | `runtime` | 否 | `wiring` | `tests/python/unit/test_task.py::TestCancellation::test_cancel_emits_task_lifecycle_event` | 订阅引擎在 Python 侧发射的任务生命周期事件；无订阅时静默 |
| `WorkflowLifecycle` | `actant/_internal/runtime.py::layer` | `actant/_internal/flow.py::_safe_emit` | 否 | `runtime` | 否 | `wiring` | `tests/python/unit/test_task.py::TestFlow::test_flow_emits_lifecycle_events` | 订阅引擎在 Python 侧发射的工作流生命周期事件；无订阅时静默 |

## 尚未闭合的差距与方向

`wiring_level_acceptance_upgrade` 记录已有扩展点的验收强度差距，
其余两项是尚缺外部契约、目前**未接线**的方向。它们不参与门禁的符号校验，
也不得出现在 `actant extensions` 的可用列表里。

| 意图 | 说明 | 未覆盖原因 |
| --- | --- | --- |
| `wiring_level_acceptance_upgrade` | 把 wiring / unit 级验收升级为完整生产路径 e2e | scheduler、transport、discovery、submit_backend、两个 lifecycle 目前只有装配级证据，ValueStore 只有单测；它们已可用，但验收强度低于 behavior 级条目 |
| `external_lifecycle_subscription` | 跨进程稳定订阅 / 介入任务与工作流生命周期 | TaskLifecycle / WorkflowLifecycle 目前是进程内 emit handler，failover 后进程内闭包不在新节点上；尚无外部事件流契约 |
| `external_scheduling_and_storage` | 外部调度器 / 外部存储接管编排决策 | Rust 侧有 RoutePolicy / Scheduler trait，但跨进程调用需要独立的外部协议，本仓库不提供 |

新增、删除或修改扩展点的流程见[贡献指南：扩展点维护](../contributing/extensions.md)。
