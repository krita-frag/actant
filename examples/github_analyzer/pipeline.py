"""流水线编排：用 actant capability 串联真实 GitHub 分析全流程。

数据流::

    真实拉取 GitHub issues（缓存优先，否则 HTTP）
        → 拉取限流（ask Throttle）
        → 拉取重试（perform Fetch + ask RetryPolicy，HTTP 失败指数退避）
        → 内容路由（ask Routing，按 owner 哈希到分析节点）
        → 优先级调度（ask Scheduling，PR 优先于 issue）
        → 分仓库聚合（perform Analyze）
        → 指标/审计多播（emit Metrics / emit Audit）
        → 报告持久化（perform ResultStore，落盘 report.txt + report.json）

运行::

    uv run python -m examples.github_analyzer

环境变量（可选）::

    GITHUB_TOKEN=ghp_xxx                          # 提升速率限制到 5000 req/hour
    ACTANT_GH_REPOS=owner1/repo1,owner2/repo2    # 覆盖默认仓库列表
"""

from __future__ import annotations

import os
import time
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from actant import (
    RetryCtx,
    RouteCtx,
    Runtime,
    ScheduleCtx,
    ask,
    emit,
    perform,
)

from .handlers import (
    AnalyzeReq,
    AuditLogger,
    ExponentialBackoffRetry,
    FetchReq,
    FileResultStore,
    GithubFetcher,
    MetricsCollector,
    OwnerRouter,
    PrFirstScheduler,
    RepoAnalyzer,
    ThrottleCtx,
    TokenBucket,
)
from .models import AnalysisReport, IssueRecord, RepoStats

# 分析节点池（演示内容路由的目标）
NODES = ["analyzer-a", "analyzer-b", "analyzer-c"]

# 默认分析仓库：选择知名活跃开源仓库，issues 量适中
DEFAULT_REPOS = [
    "tokio-rs/tokio",
    "astral-sh/uv",
]


def _resolve_repos() -> list[str]:
    """从环境变量或默认列表解析待分析仓库。"""
    env = os.environ.get("ACTANT_GH_REPOS", "").strip()
    if env:
        return [r.strip() for r in env.split(",") if r.strip()]
    return list(DEFAULT_REPOS)


def fetch_with_retry(repo: str, max_retries: int = 3) -> tuple[list[IssueRecord], str | None]:
    """通过 ``Fetch`` capability 拉取，HTTP 失败时由 ``RetryPolicy`` 决策重试。

    演示 handler 组合：``perform`` 触发真实 HTTP，``ask`` 决策是否重试，
    重试期间 ``emit`` 记录审计事件。
    """
    attempt = 0
    last_err = ""
    while True:
        attempt += 1
        try:
            records: list[IssueRecord] = perform("Fetch", FetchReq(repo=repo))
            return records, None
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, ValueError) as e:
            last_err = f"{type(e).__name__}: {e}"
            should_retry = ask(
                "RetryPolicy",
                RetryCtx(
                    task_id=f"fetch:{repo}",
                    attempt=attempt,
                    last_error=last_err,
                    max_retries=max_retries,
                ),
            )
            if not should_retry:
                emit("Audit", {"event": "fetch_giveup", "repo": repo, "error": last_err})
                return [], last_err
            # 指数退避节奏（决策与节奏分离），上限 4s。
            # 注意：此为示例简化，生产环境应在独立线程池或异步任务中执行
            # 阻塞 IO，避免阻塞 capability handler 调度线程（H6 说明）。
            time.sleep(min(0.5 * (2 ** (attempt - 1)), 4.0))


def run_pipeline(work_dir: Path, repos: list[str] | None = None) -> AnalysisReport:
    """编排完整流水线，返回报告并落盘。"""
    work_dir.mkdir(parents=True, exist_ok=True)
    audit_path = work_dir / "audit.log"
    report_txt = work_dir / "report.txt"
    report_json = work_dir / "report.json"

    # 清理上轮产物
    for p in (audit_path, report_txt, report_json):
        if p.exists():
            p.unlink()

    repos = repos or _resolve_repos()
    analyzer = RepoAnalyzer()
    metrics = MetricsCollector()
    audit = AuditLogger(audit_path)
    store = FileResultStore(work_dir)

    # 使用 try/finally 确保 AuditLogger 文件句柄在异常路径下也正确释放（H5 改进）。
    # AuditLogger 也实现了 __enter__/__exit__，可作为上下文管理器使用。
    try:
        with Runtime.with_defaults() as rt:
            # 覆盖默认策略 handler（后注册=高优先级），并注册自定义 capability
            rt.layer("Routing").chain(OwnerRouter(NODES))
            rt.layer("Scheduling").chain(PrFirstScheduler())
            rt.layer("RetryPolicy").chain(ExponentialBackoffRetry(max_retries=3))
            rt.layer("Throttle", "ask").chain(TokenBucket(capacity=30, refill=0.5))
            rt.layer("Fetch", "perform").chain(GithubFetcher())
            rt.layer("Analyze", "perform").chain(analyzer)
            rt.layer("ResultStore", "perform").chain(store)
            rt.layer("Metrics", "emit").chain(metrics)
            rt.layer("Audit", "emit").chain(audit)

            emit("Audit", {"event": "pipeline_start", "repos": repos})

            # Stage 1：限流 + 重试拉取 + 内容路由
            routed: list[tuple[IssueRecord, str]] = []  # (record, target_node)
            fetched = 0
            failed = 0
            throttled = 0
            fetch_errors: dict[str, str] = {}

            for repo in repos:
                # 限流决策（自定义 ask capability）
                if not ask("Throttle", ThrottleCtx(key="github-api", tokens=1)):
                    throttled += 1
                    emit("Audit", {"event": "throttled", "repo": repo})
                    continue

                records, err = fetch_with_retry(repo, max_retries=3)
                if err:
                    failed += 1
                    fetch_errors[repo] = err
                    emit("Metrics", {"name": "repos_failed", "value": 1})
                    continue

                fetched += len(records)
                emit("Metrics", {"name": "records_fetched", "value": len(records)})
                emit("Audit", {"event": "fetch_ok", "repo": repo, "count": len(records)})

                # 内容路由：按 owner 哈希到分析节点
                for rec in records:
                    target = ask(
                        "Routing",
                        RouteCtx(
                            task_name=repo,  # owner/repo
                            peers=NODES,
                            local_node=NODES[0],
                        ),
                    )
                    if target is None:  # OwnerRouter 对所有 repo 都有决策
                        raise RuntimeError(f"routing aborted for {rec.repo}#{rec.number}")
                    routed.append((rec, target))

            # Stage 2：优先级调度 + 聚合（PR 优先于 issue）
            pending: list[str] = []
            by_id: dict[str, tuple[IssueRecord, str]] = {}
            for rec, node in routed:
                prefix = "PR:" if rec.is_pr else "IS:"
                task_id = f"{prefix}{rec.repo}#{rec.number}"
                pending.append(task_id)
                by_id[task_id] = (rec, node)

            processed = 0
            while pending:
                next_id = ask("Scheduling", ScheduleCtx(workflow_id="wf-gh", pending=pending))
                if next_id is None:
                    break
                pending.remove(next_id)
                rec, node = by_id[next_id]
                perform("Analyze", AnalyzeReq(record=rec, node=node))
                processed += 1
                emit("Metrics", {"name": f"processed:{node}", "value": 1})

            emit("Audit", {"event": "analyze_done", "processed": processed})

            # Stage 3：构建报告并持久化
            stats_snapshot = analyzer.snapshot()
            repo_stats: dict[str, dict[str, Any]] = {}
            # 确保拉取失败的仓库也出现在报告中
            for repo in repos:
                stats = stats_snapshot.get(repo) or RepoStats(repo=repo)
                if repo in fetch_errors:
                    stats.fetch_error = fetch_errors[repo]
                repo_stats[repo] = {
                    "total": stats.total,
                    "issues": stats.issues,
                    "pulls": stats.pulls,
                    "open": stats.open,
                    "closed": stats.closed,
                    "merged": stats.merged,
                    "total_comments": stats.total_comments,
                    "top_labels": stats.top_labels(5),
                    "top_authors": stats.top_authors(5),
                    "by_month": dict(sorted(stats.by_month.items())),
                    "fetch_error": stats.fetch_error,
                }

            report = AnalysisReport(
                generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                repos=repos,
                fetched=fetched,
                failed=failed,
                throttled=throttled,
                repo_stats=repo_stats,
                metrics=metrics.snapshot(),
            )

            perform("ResultStore", {
                "op": "commit",
                "report": report,
                "data": {
                    "generated_at": report.generated_at,
                    "repos": report.repos,
                    "fetched": report.fetched,
                    "failed": report.failed,
                    "throttled": report.throttled,
                    "repo_stats": repo_stats,
                    "metrics": report.metrics,
                },
            })

            emit("Audit", {"event": "pipeline_done", "fetched": fetched, "failed": failed})
            emit("Metrics", {"name": "pipeline_complete", "value": 1})
    finally:
        audit.close()
    return report


def main() -> None:
    work_dir = Path("github_analyzer_out")
    repos = _resolve_repos()
    print(f"工作目录: {work_dir.resolve()}")
    print(f"分析仓库: {repos}")
    print("（首次运行真实拉取 GitHub API 并缓存到 ~/.cache/actant_github/）")
    print()
    report = run_pipeline(work_dir, repos=repos)
    print(report.render())
    print(f"\n报告已落盘: {work_dir / 'report.txt'}")
    print(f"审计日志:   {work_dir / 'audit.log'}")


if __name__ == "__main__":
    main()
