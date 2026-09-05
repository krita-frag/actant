"""Capability handler 实现。

每个 handler 实现一种生产关注点，全部为真实逻辑（非桩、非 mock）。
按 ERH 约定：``ask`` 返回决策、``perform`` 返回副作用结果、``emit`` 无返回值。
"""

from __future__ import annotations

import json
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from actant import RetryCtx, RouteCtx, ScheduleCtx, emit

from .models import IssueRecord, RepoStats


@dataclass
class ThrottleCtx:
    """``Throttle`` capability 请求（自定义 ask）。"""

    key: str
    tokens: int = 1


@dataclass
class FetchReq:
    """``Fetch`` capability 请求（自定义 perform）：拉取仓库 issues。"""

    repo: str


@dataclass
class AnalyzeReq:
    """``Analyze`` capability 请求（自定义 perform）：聚合一条记录。"""

    record: IssueRecord
    node: str



class OwnerRouter:
    """按仓库 owner 路由到不同分析节点。

    实现 ``RoutingHandler``：``__call__(ctx) -> str | None``。
    调用方通过 ``task_name`` 传入 ``owner/repo``，按 owner 哈希到分析节点池。
    命中则返回目标节点，否则弃权（``None``）回退到默认 LocalRouter。
    """

    def __init__(self, nodes: list[str]) -> None:
        self.nodes = nodes

    def __call__(self, ctx: RouteCtx) -> str | None:
        if not self.nodes:
            return None
        owner = ctx.task_name.split("/", 1)[0]
        # M4 改进：使用 crc32 替代内置 hash()，避免受 PYTHONHASHSEED 影响
        # 导致同一 owner 在不同进程路由到不同节点。
        idx = zlib.crc32(owner.encode("utf-8")) % len(self.nodes)
        return self.nodes[idx]



class PrFirstScheduler:
    """PR 优先 + FIFO 调度。

    实现 ``SchedulingHandler``。``pending`` 中以 ``PR:`` 开头的任务优先；
    无则回退 FIFO（返回队首）。
    """

    PREFIX = "PR:"

    def __call__(self, ctx: ScheduleCtx) -> str | None:
        for task_id in ctx.pending:
            if task_id.startswith(self.PREFIX):
                return task_id
        # 无 PR → 弃权，回退默认 FIFO
        return None



class ExponentialBackoffRetry:
    """指数退避重试决策。

    ``attempt < max_retries`` 时返回 ``True``（重试），并触发一次 ``Audit``
    事件记录决策（演示 handler 内组合 effect）。超出上限返回 ``None``（放弃）。

    约定 ``attempt`` 从 1 起算（首次失败 attempt=1），与默认 ``DefaultRetryPolicy``
    的 ``<`` 语义保持一致（H1 改进）。
    """

    def __init__(self, max_retries: int = 3) -> None:
        self.max_retries = max_retries

    def __call__(self, ctx: RetryCtx) -> bool | None:
        if ctx.attempt < self.max_retries:
            emit("Audit", {
                "event": "retry_decided",
                "task": ctx.task_id,
                "attempt": ctx.attempt,
                "error": ctx.last_error,
            })
            return True
        return None


class TokenBucket:
    """令牌桶限流器（自定义 ask capability）。

    GitHub 未认证速率限制 60 req/hour，本示例设默认容量 30、补充 0.5/s
    （即每秒最多 0.5 个请求，约 30/分钟），保护 API 配额。
    """

    @dataclass
    class _Bucket:
        tokens: float
        last_refill: float

    def __init__(self, capacity: float = 30.0, refill: float = 0.5) -> None:
        self.capacity = capacity
        self.refill = refill
        self._buckets: dict[str, TokenBucket._Bucket] = {}

    def _get(self, key: str) -> _Bucket:
        now = time.monotonic()
        b = self._buckets.get(key)
        if b is None:
            b = self._Bucket(tokens=self.capacity, last_refill=now)
            self._buckets[key] = b
            return b
        elapsed = now - b.last_refill
        if elapsed > 0:
            b.tokens = min(self.capacity, b.tokens + elapsed * self.refill)
            b.last_refill = now
        return b

    def __call__(self, ctx: ThrottleCtx) -> bool:
        b = self._get(ctx.key)
        if b.tokens >= ctx.tokens:
            b.tokens -= ctx.tokens
            return True
        return False

class GithubFetcher:
    """GitHub issues 拉取器（自定义 perform capability）。

    委托 ``github_client.load_or_fetch``：缓存命中读本地，否则真实 HTTP。
    失败时直接抛出异常，交由调用方通过 ``RetryPolicy`` 决策重试。
    """

    def __call__(self, req: FetchReq) -> list[IssueRecord]:
        from .github_client import load_or_fetch

        raw = load_or_fetch(req.repo)
        return [IssueRecord.from_api(item, req.repo) for item in raw]


class RepoAnalyzer:
    """仓库分析器（自定义 perform capability）。

    每次 ``perform`` 接收一条 ``AnalyzeReq``，累积到对应节点的 ``RepoStats``。
    """

    def __init__(self) -> None:
        self._by_repo: dict[str, RepoStats] = {}

    def __call__(self, req: AnalyzeReq) -> str:
        stats = self._by_repo.get(req.record.repo)
        if stats is None:
            stats = RepoStats(repo=req.record.repo)
            self._by_repo[req.record.repo] = stats
        stats.add(req.record)
        return req.node

    def snapshot(self) -> dict[str, RepoStats]:
        return dict(self._by_repo)


class FileResultStore:
    """结果持久化（自定义 perform capability）。"""

    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def __call__(self, req: dict[str, Any]) -> Any:
        op = req["op"]
        if op == "commit":
            report = req["report"]
            text_path = self.out_dir / req.get("text_name", "report.txt")
            json_path = self.out_dir / req.get("json_name", "report.json")
            text_path.write_text(report.render(), encoding="utf-8")
            json_path.write_text(
                json.dumps(req["data"], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return text_path
        raise ValueError(f"unknown op: {op!r}")



class MetricsCollector:
    """指标采集器（自定义 emit capability）。"""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}

    def __call__(self, event: dict[str, Any]) -> None:
        name = event["name"]
        value = event.get("value", 1)
        self._counters[name] = self._counters.get(name, 0) + value

    def snapshot(self) -> dict[str, int]:
        return dict(self._counters)



class AuditLogger:
    """审计日志（自定义 emit capability），追加写入真实文件。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = path.open("a", encoding="utf-8")

    def __call__(self, event: dict[str, Any]) -> None:
        line = f"[{time.strftime('%H:%M:%S')}] {json.dumps(event, ensure_ascii=False)}"
        self._fh.write(line + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> AuditLogger:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


__all__ = [
    "AnalyzeReq",
    "AuditLogger",
    "ExponentialBackoffRetry",
    "FetchReq",
    "FileResultStore",
    "GithubFetcher",
    "MetricsCollector",
    "OwnerRouter",
    "PrFirstScheduler",
    "RepoAnalyzer",
    "ThrottleCtx",
    "TokenBucket",
]
