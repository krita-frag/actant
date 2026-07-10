"""数据模型与聚合统计。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IssueRecord:
    """从 GitHub API 响应中提取的归一化记录。"""

    number: int
    title: str
    state: str  # "open" / "closed"
    is_pr: bool
    author: str
    labels: list[str]
    created_at: str
    closed_at: str | None
    comments: int
    repo: str

    @classmethod
    def from_api(cls, item: dict[str, Any], repo: str) -> IssueRecord:
        user = item.get("user") or {}
        return cls(
            number=item["number"],
            title=item.get("title", ""),
            state=item.get("state", "open"),
            is_pr="pull_request" in item,
            author=user.get("login", "unknown"),
            labels=[lbl["name"] for lbl in item.get("labels", [])],
            created_at=item.get("created_at", ""),
            closed_at=item.get("closed_at"),
            comments=item.get("comments", 0),
            repo=repo,
        )


@dataclass
class RepoStats:
    """单仓库聚合统计。"""

    repo: str
    total: int = 0
    issues: int = 0
    pulls: int = 0
    open: int = 0
    closed: int = 0
    merged: int = 0  # PR 中 closed 且有合并标记（近似：closed PR 视为可能合并）
    by_label: dict[str, int] = field(default_factory=dict)
    by_author: dict[str, int] = field(default_factory=dict)
    by_month: dict[str, int] = field(default_factory=dict)  # 创建月份 YYYY-MM
    total_comments: int = 0
    fetch_error: str | None = None

    def add(self, rec: IssueRecord) -> None:
        self.total += 1
        if rec.is_pr:
            self.pulls += 1
        else:
            self.issues += 1
        if rec.state == "open":
            self.open += 1
        else:
            self.closed += 1
            if rec.is_pr:
                self.merged += 1  # 近似：closed PR 视为已合并
        for lbl in rec.labels:
            self.by_label[lbl] = self.by_label.get(lbl, 0) + 1
        self.by_author[rec.author] = self.by_author.get(rec.author, 0) + 1
        month = rec.created_at[:7] if rec.created_at else "unknown"
        self.by_month[month] = self.by_month.get(month, 0) + 1
        self.total_comments += rec.comments

    def top_authors(self, n: int = 5) -> list[tuple[str, int]]:
        return sorted(self.by_author.items(), key=lambda kv: kv[1], reverse=True)[:n]

    def top_labels(self, n: int = 5) -> list[tuple[str, int]]:
        return sorted(self.by_label.items(), key=lambda kv: kv[1], reverse=True)[:n]


@dataclass
class AnalysisReport:
    """流水线最终报告。"""

    generated_at: str
    repos: list[str]
    fetched: int
    failed: int
    throttled: int
    repo_stats: dict[str, dict[str, Any]]
    metrics: dict[str, int]

    def render(self) -> str:
        lines = [
            "=" * 64,
            "Actant GitHub 分析流水线 — 分析报告",
            f"生成时间: {self.generated_at}",
            "=" * 64,
            "",
            f"【摄入】  仓库数: {len(self.repos)}  拉取记录: {self.fetched}  "
            f"失败: {self.failed}  被限流: {self.throttled}",
            "",
        ]
        for repo, stats in self.repo_stats.items():
            lines.append(f"【{repo}】")
            lines.append(f"  总记录:        {stats['total']}")
            lines.append(f"  Issues / PRs:  {stats['issues']} / {stats['pulls']}")
            lines.append(f"  open / closed: {stats['open']} / {stats['closed']}")
            if stats["pulls"]:
                rate = (stats["merged"] / stats["pulls"] * 100) if stats["pulls"] else 0
                lines.append(f"  PR 合并率:     {stats['merged']}/{stats['pulls']} ({rate:.1f}%)")
            lines.append(f"  总评论数:      {stats['total_comments']}")
            if stats["top_labels"]:
                lines.append("  Top 标签:")
                for lbl, cnt in stats["top_labels"]:
                    lines.append(f"    {cnt:>4}  {lbl}")
            if stats["top_authors"]:
                lines.append("  Top 贡献者:")
                for author, cnt in stats["top_authors"]:
                    lines.append(f"    {cnt:>4}  {author}")
            if stats["by_month"]:
                recent = sorted(stats["by_month"].items())[-5:]
                lines.append("  最近月份趋势:")
                for month, cnt in recent:
                    lines.append(f"    {month}: {cnt}")
            if stats["fetch_error"]:
                lines.append(f"  ⚠ 拉取错误: {stats['fetch_error']}")
            lines.append("")

        lines.append("【指标】")
        for name, value in self.metrics.items():
            lines.append(f"  {name:<28} {value}")
        lines.append("")
        lines.append("=" * 64)
        return "\n".join(lines)
