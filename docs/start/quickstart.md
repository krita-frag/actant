# 快速开始

## 定义与提交任务

```python
import actant
from actant import Runtime, task

@task
def add(a: int, b: int) -> int:
    return a + b

@task(retries=3, retry_delay_ms=100, timeout_ms=5000)
def fetch(url: str) -> str:
    ...  # 自动重试 3 次，每次间隔 100ms，单次超时 5s

with Runtime() as rt:
    handle = add.submit(2, 3)   # 异步提交，返回 AsyncResult
    print(handle.result())       # 5（阻塞等待结果）

    # 直接调用（同步，不走执行池）
    print(add(2, 3))             # 5
```

## 任务依赖

`AsyncResult` 作为下游任务的参数时自动阻塞等待，依赖解析会递归处理 `list` / `tuple` / `dict`：

```python
@task
def double(x: int) -> int:
    return x * 2

with Runtime():
    a = add.submit(10, 1)  # 11
    b = double.submit(a)   # 自动等待 a 完成，传入 11，得 22
    print(b.result())      # 22
```

## 工作流编排

```python
from actant import flow

@flow
def pipeline(a: int, b: int) -> int:
    result = add.submit(a, b)
    return double.submit(result).result()

with Runtime():
    print(pipeline(2, 3))  # (2+3)*2 = 10
```

`@flow` 广播生命周期事件（`submitted` → `started` → `completed` / `failed`），可通过 `rt.layer("WorkflowLifecycle").chain(handler)` 订阅。

## 下一步

- [核心概念](concepts.md)：理解 Runtime、任务、工作流与 capability。
- [任务](../guide/tasks.md)：重试、超时、幂等、批量与取消。
- [工作流](../guide/flows.md)：等待点、信号、恢复与代码演进。
