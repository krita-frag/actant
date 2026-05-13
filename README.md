# Actant

> 基于 Actor 模型的跨平台通用分布式任务编排引擎

---

## 项目简介

Actant 是一个高性能的分布式任务编排引擎，采用 **Rust + libp2p** 构建核心运行时，通过 **PyO3** 暴露给 Python 用户。

**核心特性：**
- **Actor 模型运行时**：状态ful 计算单元，支持动态扩缩容和故障迁移
- **工作流编排**：链式(Chain)、并行组(Group)、回调(Chord)、条件分支、循环等组合语义
- **事件溯源持久化**：基于 WAL + EventLog 的容错恢复机制
- **分布式对象存储**：零拷贝数据传输，引用计数自动回收
- **Python 原生体验**：符合 Python 习惯的 API，支持 async/await

---

## 技术栈

| 层级 | 技术 | 用途 |
|------|------|------|
| 用户层 | Python 3.10+ | 工作流定义、业务逻辑 |
| 绑定层 | PyO3 + maturin | Rust ↔ Python 桥接 |
| 核心层 | Rust + Tokio | 高性能异步运行时 |
| 网络层 | libp2p | 点对点通信、节点发现 |
| 存储层 | Heed (LMDB) | 嵌入式事件日志与 Checkpoint |
| 序列化 | rkyv + serde | 零拷贝高性能 + 人类可读互操作 |

---

## 快速开始

> ⚠️ **API 设计阶段声明**：以下语法为开发设计预览，实际实现可能调整

### 安装

```bash
pip install actant
```

### 启动 Worker

```python
import actant

# 方式1: 本地模式（开发调试）
# 自动启动嵌入式 Worker，任务本地执行
app = actant.ActantApp("my_app")

# 方式2: 集群模式（生产环境）
# 连接到现有集群，SDK 作为纯客户端
app = actant.ActantApp("my_app", address="p2p://QmNodeId...")
```

```bash
# 方式3: 独立 Worker 进程（分布式部署）
# 启动 Worker 加入集群
actant worker \
    --node-id worker-1 \
    --listen /ip4/0.0.0.0/tcp/0 \
    --bootstrap /dnsaddr/bootstrap.actant.io

# 查看集群状态
actant status
```

### 定义任务

```python
from actant import ActantApp, chain, group

app = ActantApp("my_app")

@app.task
def fetch(url: str) -> str:
    return f"data from {url}"

@app.task
def analyze(data: str) -> dict:
    return {"sentiment": 0.8, "topics": ["tech", "ai"]}

@app.task
def report(result: dict) -> str:
    return f"Report: sentiment={result['sentiment']}"

# 链式工作流：数据自动传递
workflow = chain(
    fetch.s("https://example.com"),
    analyze.s(),   # 自动接收 fetch 的输出
    report.s()     # 自动接收 analyze 的输出
)
result = app.submit(workflow).get()
print(result)  # Report: sentiment=0.8
```

### 定义 Actor

```python
from actant import ActantApp

app = ActantApp("my_app")

@app.actor
class ModelRegistry:
    def __init__(self):
        self.models = {}
    
    def register(self, name: str, version: str):
        self.models[name] = version
        return f"Registered {name}@{version}"
    
    def list(self) -> dict:
        return self.models.copy()

# 创建 Actor 实例
registry = app.create_actor(ModelRegistry)

# 调用方法（异步执行，自动序列化）
registry.register("gpt", "v1.0").get()
registry.register("bert", "v2.0").get()

print(registry.list().get())  # {'gpt': 'v1.0', 'bert': 'v2.0'}
```

---

## 文档

- [系统架构](docs/architecture.md) - 详细架构设计与 API 参考
- [需求规格](docs/specification.md) - 功能需求与验收标准
- [任务规划](docs/task-plan.md) - 开发路线图

---

## 开发状态

### Phase 0: 基础设施 ✅
- [x] Rust 核心项目脚手架
- [x] 通用模块 (配置、错误、模型、密钥)
- [x] 存储层 (WAL, EventLog, Backend)

### Phase 1: 单节点运行时 🚧
- [ ] Actor 运行时 (Mailbox, 状态隔离)
- [ ] 本地任务调度器
- [ ] 工作流引擎 (DAG 执行)

### Phase 2: 网络与分布式
- [ ] libp2p 网络层 (传输、发现)
- [ ] 分布式任务调度
- [ ] 集群成员管理

### Phase 3: Python 生态
- [ ] PyO3 核心绑定
- [ ] Python SDK (API 封装)
- [ ] 示例与文档
