# Worker 运行模型

## 对等节点

Actant 是 P2P 对等混合架构：每个节点同时是编排器与执行器。启动一个 `Runtime` 即在该节点自动启动一个 Worker（含 `SchedulerActor`，负责并发槽位、任务超时、优雅 drain）。显式指定 `data_dir` 时节点默认使用 `preset="local"`（公网 DNS + relay 发现）；未指定时使用临时目录并默认 `preset="none"`（仅 loopback）。内网部署应显式设置 `preset="none"` 并配置 bootstrap。发现模式见[部署指南](../guide/deployment.md)。

`rt.serve()` 在 tokio 后台 spawn worker 守护循环（订阅 P2P topic + 任务执行循环），非阻塞。

## 进程级任务隔离

任务经 `ProcessTaskDispatcher` 在 **worker 子进程**中执行（`python -m actant._internal.tasks.worker`），进程池是唯一执行后端。

- 每个 worker 进程同一时刻执行一个任务。
- 超时/取消时 Rust `terminate()` / `kill()` 对应 worker，精确终止单任务并即时释放槽位，自动拉起替补进程。
- 任务崩溃（segfault / `os._exit`）只失败该任务，节点存活。
- worker 仅通过 stdio 长度前缀二进制帧与 Rust IPC 通信（cloudpickle 载荷），不持有 Rust 核心。
- 父进程 `sys.path` 透传为 `PYTHONPATH`，保证模块级任务函数可导入。

`ProcessTaskDispatcher` 不感知 worker 的实现语言：可执行文件、入口参数与环境变量全部由 `WorkerLaunchSpec` 在构造时提供。Python 绑定层注入解释器路径与 `-m actant._internal.tasks.worker`，Rust 嵌入方可注入任意可执行程序，核心侧不出现语言专有字面量；核心只做字节校验与搬运，不解析载荷正文。

## IPC 帧协议

Rust `ProcessTaskDispatcher` 与 worker 子进程通过 stdio 长度前缀二进制帧通信。帧格式、帧类型、Dispatch v2 载荷布局、stderr 边带与版本承诺的权威定义见[框架契约](framework-contract.md)第三节。

本节只记录 Python worker 侧的实现约定：

- 单任务单线程：模块级复用 cloudpickle `Pickler` 绑定的写缓冲，每次 dump 前 `clear_memo()` 防 memo 串扰；Unpickler 不复用（跨 pickle 状态不安全），仅复用输入缓冲。
- `stdout` 是帧通道的专属资源：任务内 `print` 被代理为 stderr 文本行，帧写入使用启动时捕获的 raw buffer，避免损坏帧流。
- 帧上限 256 MiB（`MAX_FRAME_BYTES`，Python 侧定义在 `actant/_internal/tasks/helpers.py`，Rust 侧为 `runtime/dispatcher.rs` 中的同值常量）：长度字段含类型字节。Rust 写帧、Rust 读结果帧、Python 提交侧构造 envelope、Python 读 Dispatch 帧四处都按此上限校验，超限只可能是长度字段被腐蚀或载荷失控膨胀，按协议损坏处理。读端必须在分配正文缓冲**之前**校验长度（长度字段来自子进程 stdout）。

## 主循环与关闭顺序

`Worker` 是每个节点上的任务执行循环：从调度器拉取 `TaskDefinition`，决定本地执行还是转发远端，再经 dispatcher 调用 handler。它不理解 payload 的业务语义，只处理签名后的字节、超时、取消、并发槽位与结果投递。

主循环：

1. 启动网络事件循环，订阅 task / heartbeat / DAG state / cancel 等 topic。
2. 启动远端结果重试循环与取消 TTL 清理循环。
3. 获取 `TaskEnqueued` 专用 `Notify` 信号——`SchedulerActor` 在 `enqueue` 后触发 `notify_task_enqueued()` 唤醒 Worker 拉取任务。
4. 为每个任务获取 semaphore 槽位，注册 cancel flag，执行或转发任务。
5. 将结果投递回 origin 节点或本地 `WorkflowActor`。

关闭顺序：`Worker::shutdown` 只发送取消信号；真正的 drain、任务线程池关闭与 iroh endpoint 关闭由 `Runtime::shutdown` 编排，保证 Actor、dispatcher、网络按固定顺序退出，避免 Drop 阶段互相等待。

## 指标与日志边带

边带行的格式定义见[框架契约](framework-contract.md)第三节；本节说明消费语义：

- 子进程自身计时经 `actant_metric:` 上报，Rust `drain_stderr` 汇入对应的 OTel histogram。
- 任务日志（`logging` 与 `print`）经 `actant_log:` 上报，父进程发布为 `TaskLog` 事件（tap 语义，可丢）。
- 任务成功 / 失败计数由 Rust 在父进程依据 `Result` 帧与超时判定，子进程不上报。

## 取消模型

读 stdin 的线程持续处理 `Cancel` 帧：到达即为当前任务（或下一个待分配任务）设置取消事件。执行线程仅在 `_execute_with_retries` 的尝试间检查点与 `_interruptible_sleep` 段内协作退出。硬超时后的强杀回收由 Rust 负责，子进程只做协作配合。

## 崩溃故障转移

worker 进程崩溃（非逻辑失败、非硬超时）属基础设施级失败：任务清空 `target_node` 重新入队，由路由器重选本地或远端节点执行。重路由受 `crash_failover_max_attempts`（默认 3）上限约束，达到上限才降级为正常失败路径。超时与业务失败不参与该上限。

派发前会探测空闲 worker 是否仍存活；若 Dispatch 帧写入失败（worker 恰在领用后退出），dispatcher 换一个 worker 重试一次，只有重试仍失败才把错误交给上层，因此这类基础设施抖动不消耗 `crash_failover_max_attempts`。

## 可调参数

Worker 行为由 `actant.actant._ActantConfig` 控制。高层 `Runtime` 暴露 `config` 入口与一组便捷参数（`max_concurrent_tasks` / `default_task_timeout_ms` / `drain_timeout_secs` / `remote_fallback_delay_ms` / `scheduler`），二者互斥：提供 `config` 时便捷参数被忽略。

常用字段：`max_concurrent_tasks`、`num_worker_processes`、`crash_failover_max_attempts`、`default_task_timeout_ms`、`drain_timeout_secs`、`remote_fallback_delay_ms`、`starvation_timeout_ms`、`node_quotas`、`scheduler`。完整字段、类型与默认值见 [配置项](../reference/configuration.md)。

`num_worker_processes`、`crash_failover_max_attempts`、`starvation_timeout_ms`、`node_quotas` 仅可通过 `_ActantConfig` 配置。

**配置面结论**：`_ActantConfig` / `_NetworkConfig` / `_GossipConfig` / `_FailoverConfig` / `_StoreConfig` 的属性访问器是扁平数据搬运，不引入行为分派；当前规模（约 59 个）不再拆分或抽象，新增配置项必须同时提供默认值、docstring 与 `actant.pyi` 声明，并按 `advanced` 级别进入 API 快照。

任务超时、重试、崩溃故障转移、远程回退由 Worker 与 `FailoverActor`、`SchedulerActor` 协同处理；重试语义由 Rust 侧配置（`retries` / `retry_delay_ms`）驱动。

**执行行为不能**通过 Python 层 `Execute` capability 覆盖。`Runtime.layer(...).chain(handler)` 只登记在 Python 侧，不参与 Rust 内部 dispatch；要改执行行为须走 Rust 侧 `RuntimeBuilder`。

## 相关页面

- [框架契约](framework-contract.md)：注入缝与 Rust 嵌入。
- [部署与发现](../guide/deployment.md)：节点标签与配额。
