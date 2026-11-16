"""LAN 内网节点发现（纯 Python 参考实现）。

在 n0/DNS 自动发现受阻的内网环境（组播受限、无 DNS、无 relay）中，
用「自建发现源 + ``rt.dial(addr)``」组合完成组网：Rust 核心改动为零，
全部依赖已接线的公开 API——``listen_addresses()``（取得可拨号地址）、
``dial()``（直连并自动加入对端 gossip）、``discover_peers()``（重连探测）。

结构上拆成两层，便于替换发现源：

- :class:`Dialer`：地址簿 + 去重 + 拨号重试 + 断线重连。唯一的网络动作是
  ``rt.dial(addr)``；任何来源（本模块的 beacon、k8s API、中心登记服务、
  静态清单文件）都可以向它投喂地址。
- :class:`PeerBeacon`：UDP 组播信标，周期性广播本机 ``endpoint_addr`` 并
  监听对端信标，投喂给 :class:`Dialer`。组播仅覆盖同一二层域（TTL=1）。

用法（所有节点都启动 beacon 即自动组网）::

    rt = Runtime(data_dir="./node-a")   # 显式 data_dir 启用网络
    rt.start()
    beacon = PeerBeacon(rt, interface="192.168.1.10")  # LAN 网卡 IP
    beacon.start()

安全边界：组播信标是明文 JSON 且同一二层域内任何人可加入；它只暴露
``endpoint_addr``（拨号凭据）。需要准入控制时用 ``Runtime`` 配置的
``allowed_peer_ids``（Rust 侧连接过滤）兜底，而不是加密信标。

dial 失败的异常与组播收发的 OSError 经 ``on_error(endpoint_id, addr, exc)``
回调上报，不向调用方线程抛出。
"""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

__all__ = ["Dialer", "PeerBeacon", "parse_announcement"]

#: 信标载荷版本；版本不匹配的载荷直接忽略（允许跨小版本共存时前向兼容）。
_ANNOUNCE_VERSION = 1

#: 地址条目状态。``pending``＝待拨号/重试中；``connected``＝最近一次 dial 成功；
#: ``failed``＝重试耗尽（不再自动重试，等待新的 announce 刷新）；``stale``＝
#: 曾连接但对端已从 ``discover_peers()`` 消失，按新条目重拨。
_PENDING = "pending"
_CONNECTED = "connected"
_FAILED = "failed"
_STALE = "stale"


def parse_announcement(data: bytes) -> dict[str, str] | None:
    """解析一条信标载荷；非法或不兼容的载荷返回 ``None``。

    返回 dict 含 ``endpoint_id`` / ``endpoint_addr`` / ``name`` 三键。
    """
    try:
        obj = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(obj, dict) or obj.get("v") != _ANNOUNCE_VERSION:
        return None
    endpoint_id = obj.get("endpoint_id")
    endpoint_addr = obj.get("endpoint_addr")
    if not isinstance(endpoint_id, str) or not endpoint_id:
        return None
    if not isinstance(endpoint_addr, str) or not endpoint_addr:
        return None
    name = obj.get("name")
    return {
        "endpoint_id": endpoint_id,
        "endpoint_addr": endpoint_addr,
        "name": name if isinstance(name, str) else "",
    }


@dataclass
class _Entry:
    """地址簿条目。``endpoint_id`` 是去重与重连探测的稳定键（随 data_dir
    持久化的节点身份，重启不变）。"""

    endpoint_addr: str
    name: str = ""
    state: str = _PENDING
    attempts: int = 0
    next_try: float = 0.0
    updated: float = field(default_factory=time.monotonic)


class Dialer:
    """把发现源投喂的地址转化为 ``rt.dial()`` 调用的后台拨号器。

    - 去重：同一 ``endpoint_id`` 只保留一条地址，重复 announce 刷新地址与
      时间戳（地址漂移时跟随最新值）。
    - 重试：dial 失败按 ``retry_interval_s`` 退避，最多 ``max_dial_attempts``
      次，之后置 ``failed`` 并回调 ``on_error``；后续 announce 重新激活。
    - 重连（``reconnect=True``）：每 ``reconnect_interval_s`` 用
      ``rt.discover_peers()`` 探测一次，已在地址簿但探测不到的
      ``connected`` 条目重拨一次。初始 dial 就失败的条目不参与重连探测
      （从未成功建立的连接不属于「断线」）。

    所有公开方法线程安全；``announce`` 可从任意线程/来源调用。
    """

    def __init__(
        self,
        runtime: Any,
        *,
        max_dial_attempts: int = 3,
        retry_interval_s: float = 5.0,
        reconnect: bool = True,
        reconnect_interval_s: float = 30.0,
        poll_interval_s: float = 0.25,
        on_error: Callable[[str, str, Exception], None] | None = None,
    ) -> None:
        if max_dial_attempts < 1:
            raise ValueError("max_dial_attempts must be >= 1")
        if retry_interval_s <= 0 or reconnect_interval_s <= 0 or poll_interval_s <= 0:
            raise ValueError("interval parameters must be positive")
        self._runtime = runtime
        self._max_attempts = max_dial_attempts
        self._retry_interval = retry_interval_s
        self._reconnect = reconnect
        self._reconnect_interval = reconnect_interval_s
        self._poll_interval = poll_interval_s
        self._on_error = on_error
        self._lock = threading.Lock()
        self._book: dict[str, _Entry] = {}
        self._queue: list[tuple[str, str, str]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_probe = 0.0

    def announce(self, endpoint_id: str, endpoint_addr: str, name: str = "") -> bool:
        """登记一个对端地址；返回是否为新增条目。

        已存在的条目按状态处理：已连接且地址未变时只刷新时间戳（信标会
        周期性重发同一地址，重置拨号会造成无谓重拨）；地址漂移或曾拨号
        失败时重新激活拨号。
        """
        if not endpoint_id or not endpoint_addr:
            raise ValueError("endpoint_id and endpoint_addr must be non-empty")
        with self._lock:
            # is_new 以「地址簿 ∪ 未消费队列」为准：后台线程尚未 tick 时
            # 连续两次 announce 必须仍判重。
            is_new = (
                endpoint_id not in self._book
                and all(eid != endpoint_id for eid, _, _ in self._queue)
            )
            self._queue.append((endpoint_id, endpoint_addr, name))
        return is_new

    def start(self) -> None:
        """启动后台拨号线程（daemon）。重复调用无效果。"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="actant-dialer", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """停止后台线程并等待退出（幂等）。地址簿保留可读。"""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def known_peers(self) -> dict[str, dict[str, Any]]:
        """返回地址簿快照：``endpoint_id -> {endpoint_addr, name, state, attempts}``。"""
        with self._lock:
            return {
                peer_id: {
                    "endpoint_addr": entry.endpoint_addr,
                    "name": entry.name,
                    "state": entry.state,
                    "attempts": entry.attempts,
                }
                for peer_id, entry in self._book.items()
            }

    # ---- 内部 ----

    def _run(self) -> None:
        while not self._stop.is_set():
            self._tick()
            self._stop.wait(self._poll_interval)

    def _tick(self) -> None:
        now = time.monotonic()
        with self._lock:
            queued, self._queue = self._queue, []
            for endpoint_id, addr, name in queued:
                entry = self._book.get(endpoint_id)
                if entry is None:
                    self._book[endpoint_id] = _Entry(
                        endpoint_addr=addr, name=name, next_try=now
                    )
                    continue
                addr_changed = entry.endpoint_addr != addr
                entry.endpoint_addr = addr
                entry.name = name or entry.name
                entry.updated = now
                # 已连接且地址未变：仅刷新时间戳（beacon 每个广播间隔都会
                # 重发同一地址，重置拨号会造成周期性无谓重拨）。
                # 地址漂移或曾失败：重新激活拨号。
                if entry.state == _FAILED or addr_changed:
                    entry.state = _PENDING
                    entry.attempts = 0
                    entry.next_try = now
            due = [
                (peer_id, entry)
                for peer_id, entry in self._book.items()
                if entry.state in (_PENDING, _STALE)
                and entry.attempts < self._max_attempts
                and entry.next_try <= now
            ]
        for peer_id, entry in due:
            self._dial(peer_id, entry, now)
        if self._reconnect and now - self._last_probe >= self._reconnect_interval:
            self._last_probe = now
            self._probe(now)

    def _dial(self, endpoint_id: str, entry: _Entry, now: float) -> None:
        entry.attempts += 1
        entry.updated = now
        try:
            self._runtime.dial(entry.endpoint_addr)
        except Exception as exc:
            if entry.attempts >= self._max_attempts:
                entry.state = _FAILED
                if self._on_error is not None:
                    self._on_error(endpoint_id, entry.endpoint_addr, exc)
            else:
                entry.state = _PENDING
                entry.next_try = now + self._retry_interval
            return
        entry.state = _CONNECTED
        entry.attempts = 0

    def _probe(self, now: float) -> None:
        try:
            known = set(self._runtime.discover_peers())
        except Exception:
            return
        with self._lock:
            # 已连接但对端从 gossip 视图消失 → 置 stale，按新条目重拨一次。
            # 每个探测周期至多重拨一次（interval 由 reconnect_interval_s 决定）。
            missed = [
                entry
                for peer_id, entry in self._book.items()
                if entry.state == _CONNECTED and peer_id not in known
            ]
            for entry in missed:
                entry.state = _STALE
                entry.attempts = 0
                entry.next_try = now


class PeerBeacon:
    """UDP 组播信标：周期广播本机地址，监听并把对端投喂给 :class:`Dialer`。

    默认组 ``239.255.43.21`` / 端口 ``43721`` 为本库私用约定；同机多实例
    可共用（socket 以 ``SO_REUSEADDR|SO_REUSEPORT`` 绑定）。

    Args:
        runtime: 已 ``start()`` 的 ``Runtime``（``listen_addresses()`` 需要）。
        group: IPv4 组播组地址（224.0.0.0/4）。
        port: 组播端口。
        interval_s: 广播间隔；监听在同一线程内以小超时轮转。
        interface: 出网/加入组播的网卡 IP；``None`` 用系统默认路由接口。
            同机测试与单机演示传 ``"127.0.0.1"``；跨机部署传 LAN 网卡 IP。
        dialer: 复用外部 :class:`Dialer`（默认自建一个）。
        on_error: 信标收发 OSError 回调 ``on_error(action, exc)``。
    """

    DEFAULT_GROUP = "239.255.43.21"
    DEFAULT_PORT = 43721

    def __init__(
        self,
        runtime: Any,
        *,
        group: str = DEFAULT_GROUP,
        port: int = DEFAULT_PORT,
        interval_s: float = 2.0,
        interface: str | None = None,
        dialer: Dialer | None = None,
        on_error: Callable[[str, Exception], None] | None = None,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        if not 0 <= port <= 65535:
            raise ValueError("port out of range")
        group_ip = socket.inet_aton(group)  # 非法地址在构造期即失败
        # IPv4 组播地址：首字节高 4 位为 1110（224.0.0.0/4）。
        if (group_ip[0] >> 4) != 0b1110:
            raise ValueError(f"{group!r} is not an IPv4 multicast address")
        if interface is not None:
            try:
                socket.inet_aton(interface)  # 校验格式
            except OSError as exc:
                raise ValueError(f"invalid interface address {interface!r}") from exc
        self._runtime = runtime
        self._group = group
        self._port = port
        self._interval = interval_s
        self._interface = interface
        self._dialer = dialer if dialer is not None else Dialer(runtime)
        self._owns_dialer = dialer is None
        self._on_error = on_error
        self._sock: socket.socket | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._self_id = ""
        self._announce_payload = b""

    @property
    def dialer(self) -> Dialer:
        """本信标投喂的拨号器（可被外部复用/观测）。"""
        return self._dialer

    def start(self) -> None:
        """绑定组播 socket 并启动收发线程（daemon）。要求 runtime 已启动。"""
        if self._thread is not None and self._thread.is_alive():
            return
        info = self._runtime.listen_addresses()
        self._self_id = info["endpoint_id"]
        self._announce_payload = json.dumps(
            {
                "v": _ANNOUNCE_VERSION,
                "endpoint_id": self._self_id,
                "endpoint_addr": info["endpoint_addr"],
                "name": getattr(self._runtime, "name", "") or "",
            },
            separators=(",", ":"),
        ).encode("utf-8")
        self._sock = self._open_socket()
        self._stop.clear()
        self._dialer.start()
        self._thread = threading.Thread(
            target=self._run, name="actant-beacon", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """关闭 socket、停止线程（幂等）。

        构造时自建的 :class:`Dialer` 一并停止；外部传入的 dialer 由其
        所有者管理生命周期。
        """
        self._stop.set()
        sock, self._sock = self._sock, None
        if sock is not None:
            sock.close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        if self._owns_dialer:
            self._dialer.stop()

    # ---- 内部 ----

    def _open_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", self._port))
        interface_ip = self._interface or "0.0.0.0"
        mreq = socket.inet_aton(self._group) + socket.inet_aton(interface_ip)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        if self._interface is not None:
            sock.setsockopt(
                socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self._interface)
            )
        return sock

    def _run(self) -> None:
        sock = self._sock
        last_send = 0.0
        while not self._stop.is_set() and sock is not None:
            try:
                sock.settimeout(min(0.2, self._interval))
                try:
                    data, _ = sock.recvfrom(4096)
                except TimeoutError:
                    data = None
                except OSError as exc:
                    if self._stop.is_set() or self._sock is None:
                        break  # stop() 关闭 socket 导致的正常退出
                    if self._on_error is not None:
                        self._on_error("recv", exc)
                    data = None
                if data is not None:
                    self._feed(data)
                now = time.monotonic()
                if now - last_send >= self._interval:
                    last_send = now
                    try:
                        sock.sendto(self._announce_payload, (self._group, self._port))
                    except OSError as exc:
                        if self._on_error is not None:
                            self._on_error("send", exc)
            except Exception:
                if self._on_error is not None:
                    self._on_error("loop", Exception("beacon loop error"))

    def _feed(self, data: bytes) -> None:
        payload = parse_announcement(data)
        if payload is None or payload["endpoint_id"] == self._self_id:
            return
        self._dialer.announce(
            payload["endpoint_id"], payload["endpoint_addr"], payload["name"]
        )
