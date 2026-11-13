# act-ext-testkit

Actant 的分布式 / P2P 测试台：把「故障是常态」变成可复现的测试流程。

- 编排同进程或独立进程的多节点集群，每个节点有独立 `data_dir`、`TMPDIR` 与 iroh endpoint；
- 确定性注入分区、崩溃、冻结、心跳静默、时钟漂移；
- 在 Linux（`tc`）与 macOS（`dummynet`）上做真实 UDP 弱网，不需要 Docker；
- 用 trace + 不变量断言检查唯一终态、无重复执行与收敛；
- 提供 pytest 插件与本机多进程 soak（夜间长稳压测）。

包元数据中 `dependencies = []`，Actant 是可选 extra `actant`；但顶层
`import act_ext_testkit` 会加载内置 Actant 后端，因此实际使用时需要安装 Actant。
其他 P2P 运行时可以通过后端注册表接入同一套编排与断言。

## 安装

在本仓库内开发（推荐）：

```bash
uv pip install -e "extensions/act-ext-testkit[actant,dev]"
```

只跑测试、不做安装时，用 `PYTHONPATH` 即可：

```bash
PYTHONPATH=extensions/act-ext-testkit uv run pytest extensions/act-ext-testkit/tests
```

独立安装：

```bash
pip install "act-ext-testkit[actant]"
```

## 快速开始

```python
import actant
from actant.tasks import task
from act_ext_testkit import TestCluster, assert_all_terminal

with TestCluster("demo", seed=7) as cluster:
    cluster.add_node("submit", labels={"role": "submit"}, max_concurrent_tasks=0)
    cluster.add_node("w0", labels={"engine": "worker"})
    cluster.add_node("w1", labels={"engine": "worker"})
    cluster.start(topology="mesh")

    @task(tags=["engine=worker"], timeout_ms=20_000)
    def add(x: int, y: int) -> int:
        return x + y

    with actant.use_runtime(cluster.node("submit")):
        handle = add.submit(20, 22)
        assert handle.result(timeout=30) == 42
```

`TestCluster` 退出上下文时逆序停止节点、清理沙箱；`keep_data=True` 或
`data_root=...` 可保留现场。`seed` 会写进报告，用于重放随机故障。

## 核心概念

### 后端与能力协商

`Backend` 负责「如何拉起节点、如何解码事件、支持哪些故障」，编排逻辑只依赖
`NodeHandle` 控制面。内置 `actant` 后端有三种模式：

| 模式 | 隔离 | 用途 |
| --- | --- | --- |
| `in-process` | 无 | 功能/一致性测试，快、易调试 |
| `process` | 独立进程组 | 真实 SIGSTOP/SIGKILL、独立环境、硬超时 |
| `container` | 容器 | 需要容器边界的重场景（需 Docker） |

测试声明的隔离等级、网络保真度与故障类型由 `BackendCapabilities` 协商。
能力不足时抛 `CapabilityError`，不会静默降级。

### 隔离等级与网络保真度

| `IsolationLevel` | 含义 |
| --- | --- |
| `none` | 同进程 |
| `process` | 独立进程/进程组 |
| `container` | 容器 |
| `netns` | 网络命名空间（需 Linux） |

| `NetworkFidelity` | 含义 |
| --- | --- |
| `none` | 不模拟 |
| `semantic` | 控制面语义近似（心跳暂停/逻辑分区） |
| `proxy` | 代理层 |
| `kernel` | 内核级（`tc` / dummynet） |

### Profile 预设

| 名称 | 隔离 | 网络 | 时钟 | 资源限制 |
| --- | --- | --- | --- | --- |
| `dev` | `none` | `semantic` | HLC | 否 |
| `ci` | `process` | `semantic` | HLC | 否 |
| `staging` | `container` | `proxy` | REAL | 是 |
| `prod_like` | `netns` | `kernel` | REAL | 是 |

`TestCluster(profile="ci")` 会在启动时校验后端能力；`strict_profile=False`
可关闭硬校验（仅建议用于排查）。

## 集群操作

```python
cluster.add_node("a", labels={"engine": "a"}, mode="process")
cluster.start(topology="mesh")
cluster.wait_converged(expected_peers={"a": {"b"}, "b": {"a"}})

cluster.kill("a")            # 关闭节点；进程模式为 SIGKILL
cluster.freeze("b")          # 进程模式 SIGSTOP
cluster.thaw("b")            # 进程模式 SIGCONT
cluster.partition("a", "b")  # 有向分区
cluster.heal("a", "b")
cluster.isolate("a")         # 切断 a 与所有 peer
cluster.split([["a"], ["b", "c"]])

from act_ext_testkit import Topology

cluster.apply_topology(Topology.from_name("ring", cluster.node_names()))
cluster.join("c", labels={"engine": "c"})
cluster.leave("a")
```

`freeze` / `thaw` 只有进程模式支持；同进程节点会明确报错，而不是假装冻结。

拓扑只描述连接意图。Actant 的 gossip 会把可达节点的信息传播到同一连通分量，
因此「没有直连边」不等于「彼此不可见」；需要真正隔离时用 `split()` / `isolate()`。

## 确定性故障脚本

故障脚本是数据，不是线程副作用。相同 `seed` + 相同节点集得到相同计划。

```python
from act_ext_testkit import FaultScript, Partition, Heal, Kill, PauseHeartbeat

script = FaultScript(seed=7)
script.add(Partition("a", "c", at_s=0.5))
script.add(Heal("a", "c", at_s=2.0))
script.add(Kill("b", at_s=3.0))
script.add(PauseHeartbeat("c", ms=1_500, at_s=4.0))

fault_run = script.run(cluster, strict_capabilities=True)
fault_run.wait(timeout_s=10)
print(script.fingerprint())  # 计划指纹，可写进报告
```

`FaultScript.flap()` 可批量生成「断开-恢复」抖动。`ChaosPlan` 用于按概率生成
随机故障序列；所有故障都只调用 `TestCluster` 的公开原语。

## 追踪与断言

```python
from act_ext_testkit import (
    TraceRecorder,
    assert_all_terminal,
    assert_lifecycle_valid,
    assert_no_duplicate_execution,
)

trace = TraceRecorder(partial=True)
trace.attach_cluster(cluster)

# ... 执行任务 ...

assert_all_terminal(trace)
assert_lifecycle_valid(trace)
assert_no_duplicate_execution(trace)
```

其余断言：`assert_eventually`、`assert_converged`、`assert_trace_order`、
`assert_single_execution`、`assert_invariants`。trace 只记录生命周期事件，
不承载任务 payload。

## 弱网与网络故障

### SemanticNetem（控制面近似）

不碰内核，把网络参数映射到已有故障原语：延迟/抖动 → 周期暂停心跳；
丢包 → 概率性短暂分区。`duplicate` 与 `bandwidth_kbps` 无法在控制面表达，
构造时直接抛 `NetemUnsupported`。

### TcNetemFabric（Linux）

```python
from act_ext_testkit import NetworkProfile, TcNetemFabric

fabric = TcNetemFabric(
    {"worker-0": "eth0", "worker-1": "eth0"},
    peer_ips={"worker-0": "10.0.0.2", "worker-1": "10.0.0.3"},
)
fabric.apply(NetworkProfile(latency_ms=80, jitter_ms=10, loss=0.05))
fabric.partition("worker-0", "worker-1")
fabric.heal("worker-0", "worker-1")
fabric.clear()
```

需要 root（或 `CAP_NET_ADMIN`）、`tc` 与 `iptables`。没有独立网络命名空间时，
对共享接口执行 `tc` 会影响所有节点，应配合 netns 使用。

### DnctlFabric（macOS）

```python
from act_ext_testkit import DnctlFabric, NetworkProfile

fabric = DnctlFabric({"worker-0": 40001, "worker-1": 40002})
fabric.apply(NetworkProfile(latency_ms=80, loss=0.05, bandwidth_kbps=2048))
fabric.partition("worker-0", "worker-1")
fabric.heal("worker-0", "worker-1")
fabric.clear()
```

用系统自带的 `dnctl` + `pfctl` 对 UDP 做真实延迟、丢包与限速，不需要 Docker
或虚拟机。端口必须与节点 `NodeConfig(network={"listen_port": ...})` 一致；
规则写入 `com.apple/actant-testkit` anchor，不改主规则集。需要 root，不自动
`sudo`；`jitter` / `duplicate` / `reorder` 在 dummynet 下不可表达，`supports()`
会明确返回。`shaping_supported()` 可做启动前探活。

两种 Fabric 都可以直接传给 `TestCluster(fabric=...)`，或通过
`cluster.use_fabric(fabric)` / `cluster.shape_network(profile)` 使用。

### 自定义命令与 NAT

`CommandFabric` 接受调用方提供的命令模板（`tc`、`iptables` 或自建代理），
testkit 负责确定性执行与错误传播；`dry_run=True` 只记录不执行。

真实 NAT 场景用 `nat.NatTopology` 构造配置；未设置 `ACTANT_TEST_NAT=1` 时
`ensure_available()` 抛 `NatUnavailable`，pytest 插件会转换为显式 skip。

> 注意：Actant 走 iroh/QUIC（UDP）。只支持 TCP 的代理工具（如 toxiproxy）
> 无法代理这些流量，不能用于分区/弱网。

## soak：本机多进程长稳压测

不引入容器、虚拟机或自托管 runner，只用 POSIX 进程边界构造「本机多机」：

```bash
PYTHONPATH=extensions/act-ext-testkit uv run python -m act_ext_testkit.soak \
  --namespace soak-local \
  --workers 2 \
  --duration-seconds 60 \
  --concurrency 4 \
  --rate 20 \
  --fault-interval 10 \
  --report /tmp/soak.json
```

- 提交节点在父进程内，Worker 是独立进程组；
- 周期注入 SIGSTOP/SIGCONT、单向分区、心跳延迟与 SIGKILL + 补位；
- 结束校验收敛、进程清理与失败率，输出 JSON 报告；
- `--no-kill` 关闭 SIGKILL 补位，`--keep-data` 保留沙箱。

夜间任务配置见 `.github/workflows/nightly.yml`（默认 30 分钟）。soak 只支持
POSIX；它不是安全沙箱，也不替代真实跨机网络。

## pytest 集成

安装后 pytest 自动加载 `pytest11` entry point；也可在 `conftest.py` 中显式：

```python
pytest_plugins = ["act_ext_testkit.pytest_plugin"]
```

Fixtures：

| Fixture | 说明 |
| --- | --- |
| `testkit_seed` | 本次测试的确定性种子 |
| `sandbox` | 函数级隔离目录，失败时按需保留 |
| `cluster_factory` | 创建并自动清理 `TestCluster`，失败时落盘报告 |

命令行选项：

| 选项 | 说明 |
| --- | --- |
| `--testkit-seed` | 随机种子（默认读 `ACTANT_TEST_SEED` 或 `0`） |
| `--testkit-backend` | 节点后端名称（默认 `actant`） |
| `--testkit-app` | 从 `module:factory` 加载外部后端 |
| `--testkit-profile` | `dev` / `ci` / `staging` / `prod_like` |
| `--testkit-require` | 追加场景最低要求，如 `isolation=process` |
| `--testkit-artifacts` | 失败现场输出目录 |
| `--testkit-keep-sandbox` | 失败时保留沙箱目录 |

Markers：`testkit`、`p2p`、`deterministic`、`chaos`、`consistency`、`recovery`、
`performance`、`topology`、`netem`、`nat`、`adversarial`、`slow`。

## 沙箱与进程隔离

`Sandbox` 是测试资源边界，不是安全边界：

- 每节点独立根目录，`data_dir` 一律落在根内；
- 子进程 `TMPDIR` / `TEMP` / `TMP` 指向沙箱；
- `child_env()` 返回可审计的环境快照，不修改当前进程 `os.environ`；
- `cleanup()` 幂等，`leftovers()` / `assert_clean()` 用于发现残留。

`ProcessNode` 在此之上提供真实崩溃/冻结：独立进程组、rlimit、SIGSTOP/SIGCONT、
SIGKILL；IPC 是 stdin/stdout 上的 JSON 行协议，仅用于控制面。

## 接入其他被测系统

推荐通过 entry point 注册，无需修改 testkit 源码：

```toml
[project.entry-points."act_ext_testkit.backends"]
myapp = "myapp.testing:MyBackend"
```

或在测试进程内直接注册：

```python
from act_ext_testkit import register_backend

register_backend("myapp", MyBackend)
```

后端需实现 `Backend`：`launch(spec, context) -> NodeHandle`、`capabilities`、
`codec()`。`NodeHandle` 只要求 dial/peers/close_peer/heartbeat/metrics/
stop/kill/freeze/thaw 等控制操作；负载与业务语义留在被测系统内。

## 已知边界

- **不是安全沙箱**：不做 syscall 级隔离；需要强边界时用容器/虚拟机，且 testkit
  跑的是可信代码。
- **平台**：`process` 模式与 soak 仅支持 POSIX；Windows 没有 `SIGSTOP`/进程组语义。
- **弱网权限**：Linux `tc` 与 macOS dummynet 都需要管理员权限；CI 中必须
  探活并允许 skip，不能作为默认门禁。
- **拓扑与可见性**：gossip 会跨非直连边传播，真正隔离要用 `split()` / `isolate()`。
- **暂无 microVM 后端**：当前不提供 microsandbox/smolvm 后端；本机多机用进程
  边界，内核级弱网用 netns/tc 或 dummynet。

## 目录结构

| 模块 | 职责 |
| --- | --- |
| `contract.py` | 后端/句柄/能力/Profile 契约 |
| `backends/` | 后端注册表与 `actant` 后端 |
| `cluster.py` | 多节点编排、拓扑、收敛与故障原语 |
| `node.py` | `InProcessNode` / `ProcessNode` 与 `NodeConfig` |
| `sandbox.py` | 文件系统/环境/临时目录隔离 |
| `topology.py` | 有向拓扑与动态变更 |
| `faults.py` | 确定性故障脚本与 `ChaosPlan` |
| `trace.py` / `invariants.py` / `assertions.py` | 追踪、不变量与断言 |
| `netem.py` / `fabric.py` | 控制面弱网、`tc` / dummynet / 自定义命令 |
| `nat.py` / `adversary.py` | NAT 门禁与信任域/对抗场景 |
| `soak.py` | 本机多进程 soak |
| `report.py` / `metrics.py` | JSON 报告与性能采样 |
| `pytest_plugin.py` | fixtures、markers 与命令行选项 |

## 开发与测试

```bash
uv sync --extra dev
PYTHONPATH=extensions/act-ext-testkit uv run pytest extensions/act-ext-testkit/tests -q
uv run ruff check extensions/act-ext-testkit
```

仓库内测试的 `conftest.py` 会注册 pytest 插件；安装后则由 `pytest11` entry point
自动加载。测试不依赖真实跨机网络；需要外部 NAT 环境时设置 `ACTANT_TEST_NAT=1`。
