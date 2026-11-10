# 服务层（L2）

服务层把已有的 Runtime 原语映射为**进程边界**，不新增执行语义。它回答的问题是：
调用方如何在不共享 `data_dir`、不要求当前进程导入 flow 代码的前提下，提交、
查询、取消、发送信号并订阅工作流。

> 一句话：核心负责「崩溃、重放、failover 之后仍然正确」；服务层负责「跨进程
> 调用如何抵达这些原语」。调度平台、控制面 UI、租户与鉴权仍属第 3 层。

## 包布局

| 文件 | 职责 |
| --- | --- |
| `actant/service/definitions.py` | `DefinitionRef`、内容指纹、宿主侧定义解析与稳定错误码 |
| `actant/service/protocol.py` | HTTP/JSON + SSE 协议 v1，以及无第三方依赖的客户端 |

`worker_client.py`（pull worker）与 `readmodel.py`（跨节点读模型）尚未落地：
它们依赖任务租约语义，须待租约机制稳定后再实现。当前不创建占位实现。

## 定义寻址

提交方只需要一个引用：

```python
from actant.service import DefinitionRef, DefinitionResolver

ref = DefinitionResolver().reference("etl-daily")   # 本进程已导入定义时
# 或由部署清单提供：
ref = DefinitionRef(name="etl-daily", version=3, hash="<sha256>")
```

宿主进程用 `DefinitionResolver.resolve(ref)` 校验三件事：

1. 名字已登记 → 否则 `DefinitionNotFoundError`（`kind="definition_not_found"`）；
2. 版本一致 → 否则 `DefinitionVersionMismatchError`（`kind="definition_version_mismatch"`）；
3. 内容指纹一致 → 否则 `DefinitionHashMismatchError`（`kind="definition_hash_mismatch"`）。

错误码经协议原样透传，客户端以 `kind` 分支，不依赖消息文本。

### 指纹口径

`definition_hash()` 对以下内容做 SHA-256：

- flow 名、版本、`timeout_ms`、`failure_strategy`、同步/异步标记；
- 函数字节码、常量与嵌套代码。

计算前把 `co_filename` 与 `co_firstlineno` 归一化，因此同一份代码部署在不同
路径、不同行偏移下指纹一致。**被 flow 体引用的模块级全局实现不在指纹内**：
那是导入图的事实，不是定义本身；若全局逻辑变化会影响重放，应提升 `version`。
Python 主/次版本升级可能改变字节码，跨版本部署同样应提升 `version`。

## 协议 v1

服务端包装一个已启动的 `Runtime`：

```python
from actant import Runtime
from actant.service import ServiceServer

with Runtime() as rt:
    with ServiceServer(rt) as server:
        print(server.base_url)   # 默认 127.0.0.1 + 内核分配端口
        ...
```

客户端只依赖标准库：

```python
from actant.service import DefinitionRef, ServiceClient

client = ServiceClient("http://127.0.0.1:8000")
workflow_id = client.submit(ref, 41)       # 参数经 cloudpickle 传输
state = client.state(workflow_id)
result = client.result(workflow_id)
client.signal(workflow_id, "approve", b"ok")
client.cancel(workflow_id)
for event in client.subscribe(workflow_id):
    print(event["kind"])
```

### 端点

| 方法 | 路径 | 语义 |
| --- | --- | --- |
| `POST` | `/v1/workflows` | 按 `DefinitionRef` 提交 |
| `GET` | `/v1/workflows` | 列出工作流（`include_archived=1` 含已归档） |
| `GET` | `/v1/workflows/{id}` | 状态快照（不存在时 `state=null`） |
| `GET` | `/v1/workflows/{id}/result` | 持久化返回值 |
| `POST` | `/v1/workflows/{id}/cancel` | 取消 |
| `POST` | `/v1/workflows/{id}/signal` | 递交等待点信号 |
| `GET` | `/v1/workflows/{id}/journal` | 稳定 journal |
| `GET` | `/v1/workflows/{id}/events` | SSE 订阅 journal 增量，终态后发送 `event: end` |

### 载荷编码

- 提交参数：`cloudpickle.dumps((args, kwargs))` 后 base64；核心仍把载荷视为
  不透明字节。
- 状态、结果、事件：JSON；其中 `bytes` 编码为 `{"$bytes": "<base64>"}`，
  JSON 无法表达的 Python 对象编码为 `{"$pickle": "<base64>"}`。

### 边界与限制

- **无鉴权**：协议 v1 只适合可信网络。鉴权、租户隔离、审计属于第 3 层。
- **反序列化即执行**：服务端会 `cloudpickle.loads` 客户端载荷；不要把端口暴露
  给不可信来源。
- **服务不接管 Runtime 生命周期**：`ServiceServer` 只调用 `start_flow` /
  `get_workflow_*` / `cancel_workflow` / `signal_wait_point` 等既有方法；执行仍由
  `Runtime.host_flows` 在持有定义的进程内驱动。
- **非集群调度器**：没有队列实体、公平调度、抢占或 worker 拉取；这些不在服务层
  的职责内。

## 与直接提交的关系

`Runtime.start_flow(..., version=...)`（见[执行模型](execution-model.md)）是
**同进程/可信调用方**的直接提交：允许只凭版本提交，定义校验由宿主在重放时完成。
服务协议是**跨进程入口**：在提交时就用 `DefinitionRef` 做存在性、版本与内容
校验，把定义漂移前置为稳定错误。
