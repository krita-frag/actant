# 工作流

## 定义与生命周期

```python
from actant import flow, task

@flow
def pipeline(a: int, b: int) -> int:
    x = add.submit(a, b)
    return double.submit(x).result()
```

`@flow` 广播 `WorkflowLifecycle` 事件：`submitted` → `started` → `completed` / `failed`。

工作流**惰性创建**：函数体第一次 `Task.submit` 或进入等待点时建槽。`submitted` / `started` 在真正建槽后广播，因此事件里的 `workflow_id` 立即可用。若函数体从未创建工作流，只广播终态事件。

## 等待点

```python
from actant.workflow import sleep_until, wait_signal, suspend

sleep_until(deadline_ms)          # 定时
payload = wait_signal("approval") # 等待外部信号，返回递交的 bytes
suspend()                         # 等待操作员恢复
```

外部递交：

```python
rt.signal_wait_point(workflow_id, "approval", payload=b"ok")
rt.resume_suspended(workflow_id)
```

信号是**闭锁**：同一 `name` 重复 `await` 返回首次递交的 payload，重复递交不覆盖。信号可以先于等待点抵达（注册时命中缓冲即返回）。等待点、信号缓冲与 payload 随 `orch:wait:` / `orch:sigbuf:` 跨重启存活。

## 确定性上下文

flow 体内的 wall-clock、随机数与 UUID 必须经 `flow_ctx` 获取，否则重放会产生不同的提交序列：

```python
from actant.workflow import flow_ctx

now = flow_ctx.now_ms()
token = flow_ctx.uuid()
n = flow_ctx.random()
```

首次调用生成值并写入工作流历史；重放按 `ctx-{n}` 序号返回记录值。同序号调用不同种类的方法抛 `FlowReplayError`。上游结果已内联进下游 payload，因此要求任务函数本身也是确定性的。

## 代码演进

```python
from actant import flow

@flow(version=2)
def pipeline(a: int, b: int) -> int:
    ...  # 函数体可随版本演进；版本号随工作流持久化
```

flow 定义版本随工作流持久化。重放时历史版本更高（代码回滚）抛 `FlowReplayError`；更低（代码升级）允许续跑。

`patched(change_id, enabled=True)` 首次执行把分支决策写入历史，之后重放返回记录值而忽略候选默认值：

```python
from actant.workflow import patched

if patched("new-routing"):
    ...
else:
    ...
```

同一 `change_id` 在单次执行内传入矛盾默认值抛 `FlowReplayError`。

## 长生命周期与 continue_as_new

`actant.workflow.continue_as_new(*args, **kwargs)` 封口当前代，并以新工作流 id 从头执行同一函数体。DAG、执行表与历史随之重置；新旧代 id 映射与入参先落盘，崩溃后可由 `host_flows` 恢复。

## 提交与执行分离

`start_flow(name, *args, **kwargs)` 只把工作流外壳与 invocation（flow 名、版本、`cloudpickle` 入参）写入 journal 就返回 `FlowHandle`，**不在调用方执行函数体**。任何持有相同 `data_dir` 且导入了相同 flow 定义的进程调用 `host_flows()` 即可重放并完成它：

```python
from actant import Runtime, host_flows, start_flow

with Runtime() as rt:
    # 提交方：只把 invocation 落盘，不执行函数体，提交进程可立即退出
    handle = start_flow("pipeline", 1, 2, runtime=rt)

    # 执行方：通常是持有相同 data_dir 的另一个进程，扫描一轮并重放
    driven = host_flows(once=True, runtime=rt)
```

需要结果时调用 `handle.result(timeout_ms=60_000)`；在另一个进程里可凭 `workflow_id` 直接构造 `FlowHandle` 或调用 `Runtime.get_workflow_result`。

`host_flows(once=False)` 持续轮询新工作流，构成常驻 host 服务；`actant worker --host-flows` 就是这样启动的。定义与入参都来自 journal，因此提交方与执行方不需要共享进程内存。

`@flow` 函数的直接调用是“提交 + 本进程立即驱动”的嵌入式语法糖；跨进程解耦请用 `start_flow`。

## 崩溃恢复

`Runtime.host_flows(once=True, timeout_ms=...)` 重放本 `data_dir` 中尚未完成、且带 invocation 的工作流——已完成节点不重跑，缺口节点补提交，等待点与信号原样恢复。缺失 flow 定义或 invocation 的工作流会被跳过并记录 warning。`Runtime.start(auto_resume=True)` 在后台线程执行同一逻辑；默认关闭，因为重放会执行用户 flow 代码并重新派发任务。

## 超时

`@flow(timeout_ms=...)` 是工作流级 deadline，由 Orchestrator 超时 watcher 轮询执行。到期后工作流标为 `Failed`，并取消本节点与远端在途任务。调用方抛 `ActantTimeoutError`。

注意：

- 超时是**唤醒后抛出**，不是立即返回：函数体在 deadline 之后才被唤醒，返回时刻在 `deadline + state_poll_interval_ms`（默认 500ms）量级。
- 函数体本身不可抢占；若函数体已返回而 deadline 已过，调用方仍抛 `ActantTimeoutError`。
- 被挂起点或任务等待包住的纯 CPU 段不会被中断（Python 无法中断线程），长循环应拆成 `Task.submit`。
- 不含任何 `Task.submit` 的 flow 不创建编排外壳，deadline 无宿主、不生效。

## 返回值与终态

flow 函数体的返回值**不回灌 DAG**：工作流终态由最后一个任务完成触发（全部任务终态 → 工作流终态）。返回值单独写入本节点 Store 的 `__actant__:flow-result:{workflow_id}`，可经 `Runtime.get_workflow_result` 跨重启查询。

## 提交与重放

函数体内每次 `task.submit()` 经 Runtime 内部增量接口 `_add_workflow_node` 把节点**先持久化、再派发**，之后才由 Orchestrator 走既有调度路径——ready 条件、fail-fast、重试、fencing、恢复全部真实生效。

重放时第 n 次 `submit` 查工作流历史：节点已完成 → 返回记录结果（不重跑）；进行中 → 返回绑定句柄；不存在 → 新提交。节点标识由 `(任务名, 序号, workflow_id)` 确定性生成，跨重放稳定。

## 重试归属

flow 节点的唯一重试执行者是 Orchestrator（节点 `RetryPolicy`，由 `@task(retries=...)` 映射）。派发给 worker 的 payload 头部 `retries` 置 0，worker 层不再重试。独立 `@task` 直调（不在 flow 内）仍保留 worker 层重试。flow 级不做整体重试——重放模型下函数体的重试由续跑机制承载。

## 挂起与恢复

`suspend()` 追加一个挂起条件等待点并 park，等待 `Runtime.resume_suspended(workflow_id)`。它与 `wait_signal` 的区别在语义来源：signal 等具名业务事件，suspend 等操作员动作，因此 `resume` 只唤醒挂起点，不会冒名顶替业务信号。

工作流进入终态时，Orchestrator 释放该工作流的全部 park 等待者；被唤醒的函数体抛出与终态相符的异常（`actant.exceptions.WorkflowCancelledError` / `WorkflowFailedError` / `ActantTimeoutError`）。

## 等待点的持久化窗口

等待点、信号缓冲与 payload 随 `orch:wait:` / `orch:sigbuf:` 跨重启存活。跨重启恢复 = **快照 + 其后事件重放**：先加载快照，再按 `orch:eventseq:` 水位重放增量，因此已注册等待点与水位之后的信号都不会丢。

“信号先于等待点抵达”在运行期（内存缓冲）与重放期（`apply_replayed_event` 入缓冲）都成立。唯一会丢的窗口是“缓冲写入后、尚未落盘就崩溃”。详见[恢复语义](../architecture/recovery.md)。

## 步骤记忆契约

flow 节点结果按 `{任务名}-{提交序号}-{workflow_id}` 记录。重放到同一序号时命中记录结果、不再派发执行；节点定义指纹（payload/依赖/超时/优先级/重试）不一致抛 `FlowReplayError`。重试是执行期语义，每次尝试都真实执行，与重放命中互不影响。

## 下一步

- [任务](tasks.md)
- [排障](../operations/troubleshooting.md)：超时、取消、重启恢复。
