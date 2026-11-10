"""act-ext-testkit：面向分布式 P2P 系统（Actant 及其衍生项目）的测试台。

定位：**故障是常态**。测试不是"验证正常路径"，而是在确定性、可复现的
前提下持续注入分区、崩溃、冻结、时钟漂移与弱网，观察系统是否仍然收敛、
是否只执行一次、能否在故障恢复后继续推进。``actant`` 是默认后端；其他
P2P 系统或衍生运行时可通过后端注册表接入同一套编排与断言。

分层覆盖：

- **契约**：``Backend`` / ``NodeHandle`` / ``EventCodec`` / ``NodeSpec`` /
  ``BackendCapabilities`` / ``Profile``（被测系统无关）；
- **隔离**：``Sandbox``（目录/环境/临时目录）+ ``ProcessNode``（真进程
  崩溃/冻结/资源限制）+ ``ActantBackend`` 的容器模式；
- **确定性模拟**：``Topology``、``FaultScript``/``ChaosPlan``（seed 可复现）；
- **混沌故障注入**：分区、崩溃、冻结、心跳静默、时钟漂移，按后端能力协商；
- **弱网 / NAT**：``SemanticNetem``（控制面语义）+ ``CommandFabric`` /
  ``TcNetemFabric``（代理/内核级）；``nat`` 模块提供真实 NAT/relay 门禁；
- **追踪驱动断言**：``TraceRecorder`` + ``invariants`` + ``TracePolicy``，
  覆盖一致性（唯一终态、无重复执行）与收敛；
- **P2P 特化**：拓扑动态变更（join/leave/split/heal）、信任域与对抗行为；
- **性能观测**：``PerformanceSampler``（只采样、不设阈值）。

隔离契约（同进程多集群时必须遵守）：

- **节点命名空间**：每个集群用唯一 ``namespace`` 前缀命名 Runtime。
- **data_dir 隔离**：每节点独立目录，禁止共享 LMDB。
- **端口池**：节点 endpoint 由 iroh 随机分配，不手工绑定固定端口。
- **事件隔离**：断言按节点/namespace 过滤，避免多集群事件互扰。
"""

from .adversary import (
    AdversaryError,
    TrustDomain,
    assert_peer_absent,
    assert_peer_visible,
    assert_rejected,
    generate_signing_key,
    key_fingerprint,
    mismatched_domains,
    restricted_domain,
)
from .assertions import (
    assert_all_terminal,
    assert_converged,
    assert_eventually,
    assert_lifecycle_valid,
    assert_no_duplicate_execution,
    assert_trace_order,
)
from .backends import get_backend, list_backends, register_backend, reset_backends
from .cluster import (
    ClusterError,
    TestCluster,
    cleanup_root,
    cluster,
    wait_until,
)
from .contract import (
    PROFILES,
    Backend,
    BackendCapabilities,
    BackendError,
    BackendUnavailable,
    CapabilityError,
    ClockMode,
    EventCodec,
    FaultKind,
    IsolationLevel,
    LaunchContext,
    NetworkFabric,
    NetworkFidelity,
    NodeHandle,
    NodeSpec,
    Profile,
    ResourceLimits,
    ScenarioRequirements,
    TestkitError,
    resolve_profile,
)
from .fabric import CommandFabric, FabricError, TcNetemFabric
from .faults import (
    ChaosPlan,
    ClockDrift,
    Crash,
    CustomFault,
    Fault,
    FaultError,
    FaultRun,
    FaultScript,
    Freeze,
    Heal,
    Isolate,
    Kill,
    Partition,
    PauseHeartbeat,
    PlannedFault,
    Thaw,
    apply_fault,
)
from .invariants import (
    InvariantViolation,
    Violation,
    assert_invariants,
    assert_single_execution,
    check_cross_node_duplicates,
    check_duplicate_execution,
    check_trace,
)
from .metrics import PerformanceError, PerformanceSampler
from .nat import (
    NAT_ENV,
    NatError,
    NatMode,
    NatTopology,
    NatUnavailable,
    direct,
    hole_punch,
    nat_enabled,
    relay_only,
)
from .netem import (
    NetemAction,
    NetemError,
    NetemRun,
    NetemUnsupported,
    NetworkProfile,
    SemanticNetem,
    build_schedule,
    schedule_fingerprint,
    shaping_supported,
)
from .node import (
    InProcessNode,
    NodeConfig,
    NodeError,
    ProcessNode,
)
from .report import ClusterReport
from .sandbox import Sandbox, SandboxError, sandbox
from .topology import Topology, TopologyChange, diff_topology
from .trace import (
    DEFAULT_TRACE_POLICY,
    TERMINAL_KINDS,
    TraceEvent,
    TracePolicy,
    TraceRecorder,
)

__version__ = "0.2.0"

__all__ = [
    "DEFAULT_TRACE_POLICY",
    "NAT_ENV",
    "PROFILES",
    "TERMINAL_KINDS",
    "AdversaryError",
    "Backend",
    "BackendCapabilities",
    "BackendError",
    "BackendUnavailable",
    "CapabilityError",
    "ChaosPlan",
    "ClockDrift",
    "ClockMode",
    "ClusterError",
    "ClusterReport",
    "CommandFabric",
    "Crash",
    "CustomFault",
    "EventCodec",
    "FabricError",
    "Fault",
    "FaultError",
    "FaultKind",
    "FaultRun",
    "FaultScript",
    "Freeze",
    "Heal",
    "InProcessNode",
    "InvariantViolation",
    "Isolate",
    "IsolationLevel",
    "Kill",
    "LaunchContext",
    "NatError",
    "NatMode",
    "NatTopology",
    "NatUnavailable",
    "NetemAction",
    "NetemError",
    "NetemRun",
    "NetemUnsupported",
    "NetworkFabric",
    "NetworkFidelity",
    "NetworkProfile",
    "NodeConfig",
    "NodeError",
    "NodeHandle",
    "NodeSpec",
    "Partition",
    "PauseHeartbeat",
    "PerformanceError",
    "PerformanceSampler",
    "PlannedFault",
    "ProcessNode",
    "Profile",
    "ResourceLimits",
    "Sandbox",
    "SandboxError",
    "ScenarioRequirements",
    "SemanticNetem",
    "TcNetemFabric",
    "TestCluster",
    "TestkitError",
    "Thaw",
    "Topology",
    "TopologyChange",
    "TraceEvent",
    "TracePolicy",
    "TraceRecorder",
    "TrustDomain",
    "Violation",
    "apply_fault",
    "assert_all_terminal",
    "assert_converged",
    "assert_eventually",
    "assert_invariants",
    "assert_lifecycle_valid",
    "assert_no_duplicate_execution",
    "assert_peer_absent",
    "assert_peer_visible",
    "assert_rejected",
    "assert_single_execution",
    "assert_trace_order",
    "build_schedule",
    "check_cross_node_duplicates",
    "check_duplicate_execution",
    "check_trace",
    "cleanup_root",
    "cluster",
    "diff_topology",
    "direct",
    "generate_signing_key",
    "get_backend",
    "hole_punch",
    "key_fingerprint",
    "list_backends",
    "mismatched_domains",
    "nat_enabled",
    "register_backend",
    "relay_only",
    "reset_backends",
    "resolve_profile",
    "restricted_domain",
    "sandbox",
    "schedule_fingerprint",
    "shaping_supported",
    "wait_until",
]
