# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

Actant 采用 **Rust + iroh** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。P2P 对等节点混合架构，每个节点同时具备编排、执行、Actor 管理、持久化完整能力。

> ⚠️ **实验性项目** — API 尚未冻结，**1.0.0 前不保证向后兼容**。每次更新都可能造成破坏性变更，请不要用于生产环境。版本号 0.x 的任何升级都应视为潜在破坏性变更，需查阅变更日志并相应调整调用方代码。

---

## 从源码构建

```bash
# 安装 Rust 与 uv
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
curl -LsSf https://astral.sh/uv/install.sh | sh

# 克隆并构建
git clone https://github.com/krita-frag/actant.git
cd actant
uv sync
uv run maturin develop
```

---

## 快速开始

```python
import asyncio
import actant
from actant import flow, parallel

# 1. 定义任务（自动注册到全局表，所有节点可发现）
@actant.task
def add(x, y):
    return x + y

@actant.task
def multiply(x, y):
    return x * y

# 2. 定义工作流 — Future 自动建立 DAG 依赖
@flow
def chain():
    a = add(1, 2)        # a = 3
    b = multiply(a, 3)   # b = 9（依赖 a）
    return b

# 3. 提交工作流（actant.submit 自动管理瞬态节点）
result = actant.submit(chain, signing_key="your-secret").get_sync(timeout=15)
print(result.value)  # 9

# 并行 / map-reduce
@flow
def parallel_flow():
    a, b = parallel(add(1, 2), multiply(3, 4))  # 并行
    return a, b

@flow
def chord():
    results = add.map([(1, 2), (3, 4)])   # 并行 map
    return multiply.reduce(results)         # 聚合 reduce

# 任务级配置：重试、超时、优先级
@actant.task(max_retries=5, retry_delay=2.0, timeout=60.0, priority="high")
def reliable_task(data):
    ...

# 工作流级配置：失败策略
@flow(failure_strategy="continue_on_failure")
def tolerant_flow():
    ...
```

### 启动常驻 Worker 节点

`actant.submit()` 会自动创建瞬态提交节点（不执行任务，提交完即清理）。
若希望本机也参与任务执行，启动常驻 worker：

```python
import actant

node = actant.start("worker-1", max_concurrent_tasks=4, signing_key="your-secret")
# ... 提交工作流 ...
actant.stop()
```

### Actor 与监督

```python
import asyncio
import actant
from actant import Actor

class Counter(Actor):
    def __init__(self):
        self.count = 0
    def increment(self, n: int) -> int:
        self.count += n
        return self.count

node = actant.start("actor_demo", signing_key="your-secret")
counter = node.create_actor(Counter)
asyncio.run(counter.increment(5))  # 5

# 监督策略：permanent / temporary / never
from actant.supervision import ActorSupervisor

supervisor = node.supervise(
    Counter,
    policy="permanent",
    max_restarts=3,
    backoff={"base_delay_ms": 100, "max_delay_ms": 5000, "multiplier": 2.0},
)
actant.stop()
```

### 条件分支

```python
from actant.flow import branch, switch

@flow
def conditional(x):
    result = check(x)
    # 二元分支
    branch(result, lambda r: r > 0, process_positive(result), process_negative(result))
    # 多路分支
    switch(result,
        ("fast", process_fast(result), lambda r: r == "fast"),
        ("slow", process_slow(result), lambda r: r == "slow"),
    )
```

---

## CLI

Actant CLI 仅用于节点生命周期管理与集群/workflow 观测。工作流提交通过 Python API（`actant.submit`）完成。

```bash
# Worker 管理
actant worker start --daemon          # 后台启动
actant worker start                   # 前台启动
actant worker start --port 4001 --node-id worker-1
actant worker stop                    # 停止 daemon
actant worker status                  # 查看状态
actant worker list                    # 列出所有本地 daemon

# 集群与 workflow 观测
actant status peers                   # 查看集群拓扑
actant status workflows               # 列出活跃 workflow
actant status workflow <id>           # 查看 workflow 详情
actant status cancel <id>             # 取消 workflow

# 高级选项
actant worker start --metrics-port 9090
actant worker start --discovery mdns  # mDNS 局域网发现
actant worker start --bootstrap /ip4/A_IP/udp/4001  # 加入已有集群
```

> **任务自动发现**：Worker 启动时自动加载 `@actant.task` 全局注册表中的任务。业务模块需通过 `PYTHONPATH` 或 site-packages 可导入。即使模块未预加载，worker 也能通过 `__actant_generic__` handler 执行任意 `cloudpickle` 序列化的 callable，功能完整无降级。

---

## 节点生命周期

| 类型 | 启动方式 | 用途 |
|------|----------|------|
| **常驻节点（Worker）** | `actant.start(name)` 或 `actant worker start` | 接收并执行任务，长期运行 |
| **瞬态提交节点** | `actant.submit()` 自动创建 | 仅提交工作流，提交完即清理 |

瞬态节点不执行任务（`_executing=False` → `max_concurrent_tasks=0`，router 永不派任务给它），使用临时 `data_dir` + 随机端口避免与已有 worker 冲突。**P2P 对等架构**：所有节点具备完整能力（编排、执行、Actor 管理、持久化），不存在 master/slave 区分，常驻与瞬态仅是生命周期差异。

---

## 网络配置

节点发现模式通过 `--discovery` 选项或 `NetworkConfig.discovery` 字段配置。

| 模式 | 0.1.0 状态 | 说明 |
|------|------------|------|
| `none` | ✅ 完全可用 | 不启用任何发现，仅手动 `--bootstrap` 加入集群 |
| `local` | ✅ 完全可用 | n0 preset，适用于互联网和大多数部署 |
| `mdns` | ✅ 完全可用 | mDNS 局域网发现，适用于同网段多节点 |
| `dns` | ❌ 0.1.0 不可用 | 0.2 预留，启动时返回 `ConfigError` |
| `relay` | ❌ 0.1.0 不可用 | 0.2 预留，启动时返回 `ConfigError` |

**说明**：`dns` 和 `relay` 的实现依赖 iroh 自定义 `DnsResolver` / `RelayMap`，在 0.1.0 中尚未完成。为避免用户误以为配置已生效，这两个 preset 已从发现注册表中移除；指定它们会在节点启动时得到明确的 `ConfigError`。

**企业内网部署建议**：0.1.0 推荐使用 `--discovery none --bootstrap <peer-multiaddr>` 或 `--discovery mdns`；通过 Python API 设置 `NetworkConfig(preset="dns")` / `preset="relay"` 会报错。

---

## 特性

- **Rust 运行时** + Python API，DAG 工作流编排（chain / map / reduce / parallel / 条件分支）
- **Actor 模型**：Mailbox + 监督策略（permanent / temporary / never）
- **P2P 对等节点**：iroh QUIC + mDNS + Gossipsub，动态扩缩容
- **故障接管**：心跳检测 + 副本升级
- **任务路由**：least_loaded（可扩展 TaskRouter）
- **持久化**：LMDB + WAL + HLC
- **载荷安全**：BLAKE3 keyed hash MAC 签名/校验

---

## 扩展点与 0.1.0 限制

Actant 的 Rust 核心层定义了多个 trait 作为扩展点，Python 封装层也提供了 ABC。但 **0.1.0 版本中，部分 Rust trait 的自定义实现无法从 Python 注入**，仅能在纯 Rust 嵌入场景下使用。下表列出了各扩展点的当前可用性：

| 扩展点 | 语言 | 0.1.0 可从 Python 注入？ | 说明 |
|---|---|---|---|
| `TaskRouter`（任务路由策略） | Python ABC | ✅ 是 | 传入 `actant.start(router=...)` |
| `CapacityProvider`（容量提供者） | Python ABC | ✅ 是 | 传入 `actant.start(capacity_provider=...)` |
| `PayloadSerializer`（payload 序列化） | Python ABC | ✅ 是 | 传入 `actant.start(serializer=...)` |
| `Scheduler`（任务调度策略） | Rust trait | ❌ 否 | 仅内置 `PriorityScheduler` / `FifoScheduler`；自定义实现需纯 Rust 嵌入 |
| `Transport`（网络传输） | Rust trait | ❌ 否 | 仅 iroh 实现；自定义传输需纯 Rust 嵌入 |
| `TaskDispatcher`（任务分发器） | Rust trait | ❌ 否 | 仅内置实现 |
| `Actor`（Actor handler） | Rust trait | ❌ 否 | Python 用户通过 `actant.Actor` 基类实现，无需实现 Rust trait |

> ℹ️ **0.1.0 设计权衡**：Rust trait 的 Python 注入入口计划在 0.2 版本通过 PyO3 暴露。当前若需自定义 `Scheduler`/`Transport`，请直接基于 `actant.actant`（PyO3 导出）或 Rust 核心库构建自己的封装（见 AGENTS.md"绕过路径"）。

### 条件分支的求值器限制

`@flow` 中的 `branch()` / `switch()` 条件求值器是 Python lambda，存储在 `FlowContext._condition_evaluators` 中由 Python 编排循环调用。这意味着**纯 Rust 嵌入场景无法使用条件分支**（除非自行实现 Python 编排循环的等价逻辑）。0.2 计划在 Rust 核心层引入 condition evaluator trait 以解除此限制。

---

## API 参考

### 模块级 API

| 函数 | 说明 |
|---|---|
| `actant.task` | 任务装饰器（自动注册到全局表） |
| `actant.flow` | 工作流装饰器 |
| `actant.submit(flow, *args)` | 提交工作流，返回 `AsyncResult` |
| `actant.start(name, **kwargs)` | 启动常驻节点 |
| `actant.stop()` | 停止当前常驻节点 |
| `actant.list_workflows()` | 列出活跃 workflow |
| `actant.workflow_status(wf_id)` | 查询 workflow 状态 |
| `actant.workflow_state(wf_id)` | 查询 workflow 状态字符串 |
| `actant.cancel(wf_id)` | 取消 workflow |
| `actant.cancel_task(wf_id, task_id)` | 取消单个任务 |

### 核心类

| 类 | 说明 |
|---|---|
| `Flow` | 工作流定义（`@flow` 装饰器） |
| `Task` | 任务定义（`@actant.task` 装饰器） |
| `TaskRef` | 延迟计算结果引用（DAG 节点） |
| `AsyncResult` | 异步工作流结果查询 |
| `WorkflowResult` | 工作流结果包装 |
| `Actor` | Actor 基类 |
| `NetworkConfig` | 网络配置 |

### DAG 构造函数

| 函数 | 说明 |
|---|---|
| `parallel` | 声明并行执行 |
| `branch` | 二元条件分支 |
| `switch` | 多路条件分支 |

---

## 安全部署

Actant 使用 `cloudpickle` 序列化任务参数，其反序列化**等价于执行任意代码**。每个节点必须配置共享的 `signing_key`，Rust 核心会在 payload 上计算 BLAKE3 MAC 并校验签名，防止恶意节点投递伪造 payload。

> ⚠️ **`signing_key` 是集群的唯一信任边界**。任何持有 key 的节点都能签发任意任务并在集群内执行。0.1.0 不提供 per-node 权限隔离；请通过运维手段（密钥分发、网络隔离）限制 key 的传播范围。

```python
import actant
node = actant.start("worker", signing_key="your-shared-secret")
# 或通过环境变量（推荐，避免明文出现在代码中）
# export ACTANT_SIGNING_KEY="your-shared-secret"
node = actant.start("worker")
```

CLI 推荐从文件或环境变量读取，避免明文暴露在进程列表：

```bash
echo -n "your-shared-secret" > /etc/actant/signing.key
chmod 600 /etc/actant/signing.key

actant worker start --signing-key-file /etc/actant/signing.key
# 或
ACTANT_SIGNING_KEY="your-shared-secret" actant worker start
```

> ⚠️ `--signing-key <KEY>` 已弃用，因为它会把密钥暴露在 `ps` 中。

### P2P 节点白名单（生产必读）

> ⚠️ **默认开放模式**：`NetworkConfig.allowed_peer_ids` 默认为空元组，表示**接受任意 iroh peer 加入集群**。这在本地开发/测试场景下方便，但在生产环境是严重安全隐患——任意可达的 iroh 节点都能提交任务并消耗集群资源。

生产部署**必须**显式配置白名单，仅允许可信节点直连：

```bash
# CLI：可重复 --allowed-peer-id 指定允许的 iroh EndpointId
actant worker start \
    --allowed-peer-id "EndpointId-A" \
    --allowed-peer-id "EndpointId-B" \
    --signing-key-file /etc/actant/signing.key
```

```python
# Python API
import actant
node = actant.start(
    "worker",
    network=actant.NetworkConfig(
        preset="local",
        allowed_peer_ids=("EndpointId-A", "EndpointId-B"),
    ),
    signing_key="your-shared-secret",
)
```

**其他网络安全建议**：
- 最小权限运行 worker：不要用 root 运行 worker。
- 不要向未认证入口暴露 payload：禁止把 task payload 直接交给 HTTP API 或消息队列。
- 限制网络可达性：在不信任网络中使用 `preset="local"` 或 `preset="mdns"`。

---

## 部署

```bash
# 节点 A（常驻 worker）
actant worker start --port 4001 --daemon --signing-key-file /etc/actant/signing.key

# 节点 B（加入集群）
actant worker start --port 4002 \
    --bootstrap /ip4/A_IP/udp/4001 --daemon \
    --signing-key-file /etc/actant/signing.key

# Prometheus 指标
actant worker start --metrics-port 9090 --signing-key-file /etc/actant/signing.key

# 查看集群拓扑 / 活跃 workflow
actant status peers
actant status workflows

# 提交工作流（通过 Python API）
python -c "import actant; from myapp.flows import my_workflow; actant.submit(my_workflow, signing_key='your-shared-secret').get_sync(timeout=60)"
```