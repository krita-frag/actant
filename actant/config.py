"""Actant 公共配置类型与领域常量。

这些类型用于在不暴露 Rust 内部枚举的情况下配置运行时。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum

PriorityInput = int | str | None


class TaskPriority(IntEnum):
    """任务优先级枚举。

    Rust 核心层使用 i32 数值表示优先级，数值越大越紧急。
    Python 层定义具体含义，可自由扩展而无需修改 Rust。

    内置常量：
        LOW = -10
        NORMAL = 0   (默认)
        HIGH = 10
        CRITICAL = 20

    用户也可直接传入任意整数（如 -5, 15）实现自定义优先级体系。
    IntEnum 使得优先级成员本身就是 int，可直接参与数值比较与算术。
    """

    LOW = -10
    NORMAL = 0
    HIGH = 10
    CRITICAL = 20


# 模块级缓存：优先级名称 → 数值。仅在模块加载时构建一次，
# 避免每次 normalize() 调用都重建字典。
_PRIORITY_NAME_TO_INT: dict[str, int] = {
    member.name.lower(): member.value for member in TaskPriority
}


def normalize_priority(value: PriorityInput) -> int:
    """将各种优先级输入归一化为整数。

    接受 IntEnum 成员、整数或字符串（"low"/"normal"/"high"/"critical"，大小写不敏感）。
    None 视为 NORMAL(0)。

    Raises:
        ValueError: 字符串无法识别时。
        TypeError: 类型不支持时。
    """
    if value is None:
        return int(TaskPriority.NORMAL)
    if isinstance(value, str):
        key = value.lower()
        if key not in _PRIORITY_NAME_TO_INT:
            raise ValueError(
                f"invalid priority {value!r}; expected one of {sorted(_PRIORITY_NAME_TO_INT)}"
            )
        return _PRIORITY_NAME_TO_INT[key]
    if isinstance(value, int):
        return int(value)
    raise TypeError(f"priority must be int, str, or None; got {type(value).__name__}")


class WorkflowState:
    """Workflow 状态字符串常量（与 Rust Phase 枚举的字符串表示一一对应）。

    集中管理避免跨文件硬编码字符串比较的拼写错误。
    """

    PENDING = "Pending"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    TIMEOUT = "Timeout"

    #: 所有终态：到达后不再变化。
    TERMINAL: frozenset[str] = frozenset({COMPLETED, FAILED, CANCELLED, TIMEOUT})


FailureStrategyInput = str | None


class FailureStrategy:
    """工作流失败策略常量。

    Rust 核心层使用字符串标签表示失败策略，仅对 ``"fail_fast"`` 做特殊处理
    （任意任务失败立即标记整个工作流为失败）。其他标签表示"单个任务失败
    不立即终止工作流"，由 Python 编排循环决定工作流级完成语义。

    内置常量：
        FAIL_FAST = "fail_fast"            （默认，快速失败）
        CONTINUE_ON_FAILURE = "continue_on_failure"  （失败继续）

    用户可传入任意字符串实现自定义策略，仅需在 Python 编排循环中处理。
    """

    FAIL_FAST: str = "fail_fast"
    CONTINUE_ON_FAILURE: str = "continue_on_failure"

    @staticmethod
    def normalize(value: FailureStrategyInput) -> str:
        """将各种失败策略输入归一化为字符串标签。

        接受字符串或 None（视为 FAIL_FAST）。
        """
        if value is None:
            return FailureStrategy.FAIL_FAST
        if isinstance(value, str):
            return value
        raise TypeError(
            f"failure_strategy must be str or None; got {type(value).__name__}"
        )


@dataclass(frozen=True)
class NetworkConfig:
    """网络与节点发现配置。

    Args:
        preset: 发现预设。任意非空字符串均可，Rust 端通过注册表查找具体实现。
            内置预设:
            - "local": iroh n0 预设（DNS + relay），适合大多数部署。
            - "mdns": 本地网络发现，禁用 relay，适合 LAN。
            - "none": 无自动发现，需通过 bootstrap_nodes 显式连接。
            未知名称不会回退，Rust 端会在启动时返回 ConfigError。
            自定义发现策略可在 Rust 端通过 ``register_discovery`` 注册后
            在此传入对应名称。
        bootstrap_nodes: 显式引导节点地址列表。frozen dataclass 中以 tuple
            存储，构造时接受 list 或 tuple，自动归一化为 tuple 防止外部修改。
        max_message_size: 单条网络消息的最大字节数。
        allowed_peer_ids: P2P 节点认证白名单（iroh EndpointId 字符串）。
            空（默认）= 开放模式，接受任意对端的入站直连请求；
            非空 = 仅接受 EndpointId 在此列表中的对端的入站直连请求
            （任务分发 / 结果交付路径），其余连接在握手后即被关闭。
            注意：gossip 广播不受此白名单管辖。
        direct_request_timeout_ms: 单次直连请求-响应调用的超时（毫秒）。
            覆盖 connect + open_bi + 读写全过程，超时返回 ``TimeoutError``。
            默认 30000（30s）。
        gossip_bootstrap_peers: gossip mesh 引导 peer 列表（iroh EndpointId
            字符串）。节点在订阅 gossip 话题时将这些 peer 作为初始邻居，
            立即建立 gossip mesh。与 ``bootstrap_nodes``（仅建立直连）配合
            使用，确保 DAG 状态复制和容量广播能跨节点传播。
            适用于 ``preset="none"`` 场景：worker 需显式指定 orchestrator 的
            endpoint_id 作为 gossip 引导 peer，否则 gossip 状态无法同步。

    .. note::

        LMDB 持久化说明：当配置了 data_dir 时，Actant 启用 LMDB 进程级读写锁
        （非 NO_LOCK 模式）。这意味着同一时间只有一个进程可以打开同一个 data_dir。
        使用相同 data_dir 运行多个 _Node 实例将返回 ``DatabaseIsOpen`` 错误。
        每个节点必须使用唯一的 data_dir，或者省略 data_dir 以仅在内存模式下运行。
    """

    preset: str = "local"
    bootstrap_nodes: tuple[str, ...] = field(default_factory=tuple)
    max_message_size: int = 16 * 1024 * 1024
    allowed_peer_ids: tuple[str, ...] = field(default_factory=tuple)
    gossip_bootstrap_peers: tuple[str, ...] = field(default_factory=tuple)
    direct_request_timeout_ms: int = 30_000
    listen_port: int = 0
    listen_ip: str = ""

    def __post_init__(self) -> None:
        if not self.preset:
            raise ValueError("network preset must not be empty")
        # 归一化 list → tuple，确保 frozen 实例不可变。
        if isinstance(self.bootstrap_nodes, list):
            object.__setattr__(self, "bootstrap_nodes", tuple(self.bootstrap_nodes))
        if isinstance(self.allowed_peer_ids, list):
            object.__setattr__(self, "allowed_peer_ids", tuple(self.allowed_peer_ids))
        if isinstance(self.gossip_bootstrap_peers, list):
            object.__setattr__(
                self, "gossip_bootstrap_peers", tuple(self.gossip_bootstrap_peers)
            )
