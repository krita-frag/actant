"""GitHub REST API 客户端：真实 HTTP 拉取 + 本地缓存。

仅使用标准库 ``urllib``，无第三方依赖。未认证时 GitHub 限制 60 req/hour，
因此首次拉取后写入本地 JSON 缓存（``~/.cache/actant_github/<repo>.json``），
后续运行直接读缓存，避免重复触发速率限制。缓存数据为首次拉取时的真实快照。
"""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path
from typing import Any

GITHUB_API = "https://api.github.com"
DEFAULT_USER_AGENT = "actant-github-analyzer/0.1 (+https://github.com/actant/actant)"


def _cache_dir() -> Path:
    """返回缓存目录，自动创建。"""
    d = Path(os.environ.get("ACTANT_GH_CACHE", str(Path.home() / ".cache" / "actant_github")))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cache_path(repo: str) -> Path:
    """``owner/repo`` → 缓存文件路径（``/`` 替换为 ``__``）。"""
    return _cache_dir() / f"{repo.replace('/', '__')}.json"


def _auth_headers() -> dict[str, str]:
    """若设置了 ``GITHUB_TOKEN`` 则附加认证头，提升速率限制到 5000 req/hour。"""
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _request_json(url: str, *, timeout: float = 30.0) -> Any:
    """发起单次 GET 请求，返回解析后的 JSON。

    Raises:
        urllib.error.HTTPError: GitHub 返回非 2xx（如 403 速率限制、404 仓库不存在）。
        urllib.error.URLError: 网络层错误。
    """
    req = urllib.request.Request(url, headers=_auth_headers())
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_issues(repo: str, *, state: str = "all", per_page: int = 100) -> list[dict[str, Any]]:
    """拉取仓库的 issues（GitHub API 中 PR 也以 issue 形式返回，带 ``pull_request`` 字段）。

    单页拉取（``per_page`` 最大 100）。生产中可分页拉取全部，此处为控制速率限制消耗，
    仅取最近 100 条，足以演示分析流水线。
    """
    url = f"{GITHUB_API}/repos/{repo}/issues?state={state}&per_page={per_page}&sort=created&direction=desc"
    return _request_json(url)


def load_or_fetch(repo: str) -> list[dict[str, Any]]:
    """优先读本地缓存，缓存缺失时真实拉取 GitHub API 并写缓存。

    缓存为首次拉取的真实快照，避免重复请求触发速率限制。
    如需刷新数据，删除 ``~/.cache/actant_github/<repo>.json``。
    """
    cache = _cache_path(repo)
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))

    data = fetch_issues(repo)
    cache.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


def rate_limit_status() -> dict[str, Any]:
    """查询当前 GitHub API 速率限制状态（真实调用 ``/rate_limit``）。"""
    return _request_json(f"{GITHUB_API}/rate_limit")  # type: ignore[no-any-return]
