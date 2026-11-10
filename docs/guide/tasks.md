# 任务

## 定义任务

```python
from actant import task

@task(retries=3, retry_delay_ms=100, timeout_ms=5000, tags=["gpu"], quota="gpu")
def train(path: str) -> dict:
    ...
```

| 参数 | 说明 |
| --- | --- |
| `retries` | 失败后的最大重试次数 |
| `retry_delay_ms` | 重试基础间隔 |
| `timeout_ms` | 任务级硬超时；`0` 表示落到 `default_task_timeout_ms` |
| `tags` | 标签选择器（`key` 或 `key=value`），与节点 `node_labels` 匹配后才允许放置 |
| `quota` | 单维命名配额（如 `gpu`），执行节点按 `node_quotas` 本地原子扣减 |

## 提交与等待

```python
handle = train.submit("data.bin")   # 异步提交
value = handle.result()             # 阻塞等待
```

直接调用 `train(...)` 是同步执行，不经过执行池；`submit()` 返回 `AsyncResult`。

`AsyncResult` 的方法：

- `result()` / `exception()`
- `state` / `done()`
- `ref()`：大结果的内容寻址引用

`gather(...)` / `await aio.gather(...)` 用于并行等待一组任务。

### 等待与回调

同一 `AsyncResult` 的所有消费者共享一个完成 future：`result()` / `wait()` 一次阻塞等待，不轮询。`add_done_callback` 以侵入式链表串接在 future 上，完成时只遍历一次。`gather(N)` 把 N 个句柄的完成信号汇聚到单个共享事件，只等待一次，避免 N 次 `wait_for` 累加延迟。

### 大结果与 Ref

结果单字段两态：小结果直接缓存对象，`result()` 直接返回；结果帧超过 `REF_INLINE_THRESHOLD` 时只保留 `Ref`，`result()` 经 ValueStore 透明拉取并反序列化。`ref()` 暴露该句柄，便于跨节点传递大结果。

## 执行位置

`submit()` 默认进入本节点的 Rust 进程池（`ProcessTaskDispatcher`），任务在 worker 子进程 `actant._internal.tasks.worker` 中执行，实现进程级隔离；并发上限由 `max_concurrent_tasks` 控制。需要强制远端执行时用 `task.submit_to(node_id, ..., endpoint_addr=...)`。进程模型与 IPC 细节见 [Worker 运行模型](../architecture/worker-model.md)。

## 依赖解析

把 `AsyncResult` 作为下游 `submit()` 的参数即可建立依赖；`list` / `tuple` / `dict` 会递归解析。提交方父进程会把依赖结果内联进 payload，worker 不感知网络。

## 失败传播

任务异常序列化进结果字节：`result()` 在提交方重新抛出同一异常（跨节点安全），`exception()` 只取异常对象。任务被取消时抛 `TaskCancelledError`。

## 超时与取消

任务级超时是**硬超时**：Rust 在超时后立即终止对应 worker 子进程并释放槽位，无需业务代码配合。

若希望在强杀前优雅收尾，可在任务内使用协作式检查点：

```python
from actant.tasks import get_task_context
from actant.exceptions import TaskCancelledError

@task(timeout_ms=5000)
def long_pipeline(items: list) -> list:
    ctx = get_task_context()
    out = []
    for i, x in enumerate(items):
        if ctx is not None and ctx.is_cancelled():
            raise TaskCancelledError(f"cancelled at item {i}")
        out.append(expensive_step(x))
    return out
```

`Runtime.cancel_task(task_id)` 可取消已提交任务。取消是协作式的；进程强杀始终兜底。

## 提交幂等

```python
a = add.submit(1, 2, idempotency_key="order-42")
b = add.submit(1, 2, idempotency_key="order-42")  # 返回同一个 AsyncResult
```

同一 Runtime 内同键只入队一次。键随 `data_dir` 持久化；重启后同键再提交抛 `AlreadyExistsError`（消息含首次 `task_id`），不会二次执行。用 `Runtime.forget("idempotency_key", key)` 清除登记。

## 批量提交

```python
rt.submit_batch([(add, (1, 2)), (add, (3, 4))], batch_id="b1")
progress = rt.batch_progress("b1")
```

进度是内存态快照，不落盘；终态由 Python 结果泵上报。

## 逐任务日志

```python
rt.on_task_log(callback, task_id="t-123")  # 省略 task_id 接收全部任务日志
```

日志通道是 tap 语义，满即丢，不承载正确性语义。

## 生命周期事件批处理

每个任务产生 `started` + `completed` 两次 `TaskLifecycle` 事件。高吞吐场景下逐条 publish 会成为瓶颈，因此可通过 `_EventBatcherScope` 在作用域内启用批处理：事件累积到 buffer，按 1ms 窗口或 100 条阈值 flush，emit 次数降为约 N/100。未启用时直接同步 emit，零额外开销；单条 emit 失败按 `log` 策略隔离，不阻塞同批其他事件。

## 下一步

- [工作流](flows.md)：等待点、信号、恢复。
- [部署与发现](deployment.md)：标签、配额与节点配置。
