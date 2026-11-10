# 执行模型与契约

本文定义 Actant 的执行语义与不可回退的契约，供 driver、服务层与 L3 平台共同遵守。
架构分层见 [总览](overview.md)，持久化与恢复见 [恢复语义](recovery.md)。

## 分层

| 层 | 职责 | 不负责 |
| --- | --- | --- |
| Kernel（Rust） | journal、DAG 状态机、等待点、租约、任务分发接口 | Python 代码执行、业务语义 |
| Driver（Python） | 执行 flow 函数体、重放、驱动 parked workflow | 状态持久化、一致性 |
| Worker | 执行单个任务、进程隔离、超时与结果回传 | 工作流决策 |
| L3 | Job/Frame/Show、事件入口、限流、公平调度、产物与血缘 | 核心状态机 |

Kernel 是事实源；Driver 与 Worker 都可以崩溃重启，不能持有唯一事实。

## 事实源与恢复

- **Journal 是唯一事实源**。快照只是加速缓存，可由 journal 重建。
- **Snapshot** 包含 DAG 节点状态、等待点、缓冲信号、上下文值与继续关系。
- **Pending signals** 在等待点注册前抵达时进入持久缓冲；重启后与等待点同批恢复。
- **任务结果**在提交方重启后仍可读取；工作流 failover 后由接管节点重调度未完成任务。
- **At-least-once**：节点崩溃、租约过期、网络分区都可能造成任务重复执行。业务必须用幂等键或幂等输出（覆盖写、原子重命名、内容寻址）保证副作用安全。
- **不提供 exactly-once**。核心不猜测业务副作用是否可重放。

## 持久层职责划分

- **Journal**：每个工作流一条 append-only 事件流，回答“工作流发生了什么”——提交履历、DAG 节点与依赖、状态迁移、等待点、信号、上下文值与终态。工作流状态判断只以它为准；快照是它的物化缓存，可随时由它重建。节点完成事件携带结果字节，因此重放不依赖结果记录。
- **Record store**：按任务 / 工作流保存终态结果帧、错误与返回值，回答“产出是什么”——供跨进程、跨重启读取；大载荷走 `ValueStore` 内容寻址。独立任务没有 journal，其结果记录是唯一持久事实。
- 两者不互为备份：不要用 record store 推断工作流状态，也不要以结果读取替代 journal 重放。工作流节点完成时，完成事件与结果记录由同一完成路径写入。

## 工作流语义

- `@flow` 的函数体是可重放的决策代码：同一 journal 前缀必须产生同一组决策。
- 已完成的节点在重放时直接命中记录，不重复执行。
- 等待点（signal / timer / suspend）持久化；信号先到不会丢。
- `continue_as_new` 为长历史续代：旧代记录下一代 id 与携带参数，新代独立终态。
- `version` + `patched()` 用于跨版本重放：旧工作流继续走旧分支，新工作流走新分支。
- 提交与执行分离：`start_flow` 只落盘工作流外壳与 invocation，函数体由 `host_flows` 驱动。提交方无需加载 flow 定义，凭显式 `version`（及可选的 `timeout_ms` / `failure_strategy`）即可提交，定义由宿主进程解析；定义已加载时显式参数须与登记值一致，不一致按定义漂移报错。
- 工作流终态为 `Completed` / `Failed` / `Cancelled`；终态不可重开。需要重试时由 L3 提交新工作流或新任务。

## 任务语义

- 任务即 activity：输入输出是 cloudpickle payload，Kernel 不解析。
- 任务有硬超时；超时会杀死 worker 进程并产生 `ActantTimeoutError`。
- 任务失败按 `RetryPolicy` 重试；worker 崩溃走 crash failover，不计入业务重试次数。
- 结果通过 `AsyncResult` 回传；大结果走 `ValueStore`，小结果走内联值。
- 任务节点完成后不会因重放再次执行；未完成节点在 failover 后可能被重新调度。

## 驱动模型（当前基线）

Rust 侧的等待点是异步的：等待不会阻塞 actor 消息循环。  
Python 侧的 `host_flows` 默认仍是**串行阻塞驱动**（`BlockingDriver`）：一个 flow 在
`wait_signal` / `sleep` 上 park 时，同一 host 的后续 workflow 不会被驱动。

驱动通过 `FlowDriver` 协议注入（`actant/driver.py`）：`host_flows` 负责扫描 journal、
跳过 `continue_as_new` 后代与裁决工作流级超时，驱动只执行重放闭包。替换驱动不改变
本文描述的 journal 与重放契约。

- `BlockingDriver`（默认）：在当前线程同步重放，并发上限为 1。
- `ThreadedDriver(max_workers=N)`：每个 flow 一个线程，park 不再阻塞其他 flow；
  并发上限为 N，超限显式抛 `InvalidStateError`，不无限建线程。`once=True` 返回时
  只保证重放已派发，调用方需在停止 Runtime 前 `driver.join()`。
- `AsyncDriver`：单个 event loop 承载全部 `@aio.flow`，park 只挂起协程、不占线程；
  同步 `@flow` 交给有界线程池，避免阻塞 loop。每次 `spawn` 后须 `join()`，停止前
  `close()`（或使用 `with AsyncDriver() as driver:`）。
- 线程数与内存随并发数线性增长，千级是线程模型的实际上界；万级并发使用 `AsyncDriver`。

绑定层另提供 `Runtime.wait_wait_point_async`：返回 asyncio awaitable，等待点唤醒时
通过 `call_soon_threadsafe` 完成 Future，语义与阻塞版 `wait_wait_point` 相同。它复用
现有等待点注册与 oneshot，不新增 journal 或状态机原语；async flow API 在此基础上构建。

该限制位于驱动层，不是 kernel 的语义限制。

## 异步工作流

`@aio.flow` 与 `@flow` 共享**同一套** journal、重放与等待点语义，差别只是函数体为
`async def`，等待时 `await` 而不是阻塞线程。函数体首个参数是 `AsyncFlowContext`：

```python
import actant
from actant.aio import AsyncDriver
from actant.aio import AsyncFlowContext

@actant.aio.flow
async def approve(ctx: AsyncFlowContext, doc: str) -> str:
    await ctx.sleep(60)                                  # timer 等待点
    decision = (await ctx.wait_signal("approval")).decode()
    await ctx.submit(write_audit, doc, decision)         # 节点先落盘再派发
    return decision

with actant.Runtime() as rt:
    handle = actant.start_flow("approve", "doc-1", runtime=rt)
    with AsyncDriver() as driver:
        actant.host_flows(runtime=rt, once=True, driver=driver)
        rt.signal_wait_point(handle.workflow_id, "approval", b"ok")
        driver.join()
    print(handle.result())
```

约束与语义：

- 等待点键与序号规则与同步 API 一致（`timer-{n}`、信号名），因此信号先到缓冲、
  重放命中与跨重启恢复完全复用既有实现；异步驱动不新增 journal 原语。
- `ctx.submit` 提交后 `await` 任务结果：参数中的 `AsyncResult` 用 `await` 解析，
  上游 `task_id` 仍作为 DAG 依赖传给 orchestrator；完成后节点不因重放再执行。
- 流程上下文用 `ContextVar` 隔离：同一 event loop 上的多个协程在 `await` 之间不会
  互相污染线程局部状态。
- `AsyncDriver` 是单一 loop + 惰性线程；10,000 个 parked flow 的线程数保持恒定，
  结果与唤醒延迟见 `tests/python/integration/test_async_driver.py`。

## 基线测量

`benches/python/flow_concurrency.py` 用于量化该基线：

```bash
uv run --extra dev python benches/python/flow_concurrency.py --mode threads --flows 200
uv run --extra dev python benches/python/flow_concurrency.py --mode detached --flows 200
```

`benches/python/task_throughput.py` 用于量化任务吞吐、超时与崩溃恢复：

```bash
uv run --extra dev python benches/python/task_throughput.py --tasks 500 --workers 8 --task-ms 5
uv run --extra dev python benches/python/task_throughput.py --tasks 20 --timeout-case --crash-case
```

## 契约测试索引

| 契约 | 测试 |
| --- | --- |
| 已完成节点重放不重复执行 | `tests/python/integration/test_step_memoization.py` |
| 信号先于等待点注册不丢失 | `tests/python/integration/test_flow_waitpoints.py` |
| 信号缓冲跨重启存活 | `tests/python/integration/test_execution_model_contract.py` |
| 已完成 workflow 再次驱动是 no-op | `tests/python/integration/test_execution_model_contract.py` |
| `continue_as_new` 分代与携带参数 | `tests/python/integration/test_flow_continue_as_new.py` |
| `patched()` 跨版本重放 | `tests/python/integration/test_flow_versioning.py` |
| journal 事件契约 | `tests/python/integration/test_journal_contract.py` |
| worker 崩溃恢复 | `tests/python/e2e/test_multi_node_reliability.py`（`TestWorkerKillIsolation`；心跳与判死单测见 `tests/rust/unit/runtime/workflow/failover.rs`） |
| async flow 复用等待点 / 重放 | `tests/python/integration/test_async_flow.py` |
| 万级 async flow 并发与恒定线程数 | `tests/python/integration/test_async_driver.py` |

新增或修改上述语义时，必须同时更新对应测试；测试是契约的可执行定义。

## 与 L3 的边界

Kernel 只保证：

- 决策日志、状态机、等待点、租约与任务分发的一致性；
- 任务至少执行一次；
- 崩溃后可以从 journal 恢复。

L3 必须自行保证：

- 作业/帧/产物的业务状态机与账本；
- 幂等键的生成与副作用去重；
- 公平调度、抢占、限流、配额分摊；
- 可见性索引、搜索、UI、账单与血缘；
- 多租户授权与命名空间隔离。

上述能力属于上层系统的职责，Kernel 不提供对应抽象。
