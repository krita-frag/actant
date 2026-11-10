"""测试沙箱：文件系统、临时目录与子进程环境的隔离边界。

分布式测试最容易出现的"假阳性/假阴性"来自共享状态：多个集群共用同一个
``data_dir``、子进程继承宿主的 ``TMPDIR``、测试失败后残留的进程或目录影响
下一次运行。``Sandbox`` 把这些边界收敛到一个显式对象：

- **目录隔离**：每个沙箱拥有独立根目录，节点 ``data_dir`` 一律落在根内。
- **临时目录隔离**：子进程的 ``TMPDIR``/``TEMP``/``TMP`` 指向沙箱内目录。
- **环境隔离**：``child_env()`` 返回可审计的环境快照（含 ``PYTHONPATH``），
  不修改当前进程的 ``os.environ``。
- **清理契约**：``cleanup()`` 幂等；``leftovers()`` 用于发现"清理后仍有残留"
  的泄漏，``assert_clean()`` 在泄漏时失败。

沙箱不是安全边界（不做 syscall 级隔离），而是测试资源边界。需要真正的
崩溃/冻结隔离时配合 :class:`act_ext_testkit.node.ProcessNode` 使用。
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

#: testkit 包根目录（``act_ext_testkit`` 的父目录），用于拼装子进程 ``PYTHONPATH``。
PACKAGE_ROOT = Path(__file__).resolve().parents[1]


class SandboxError(RuntimeError):
    """沙箱生命周期或清理失败。"""


class Sandbox:
    """隔离的测试资源根目录。

    用法::

        with Sandbox("p2p-test") as box:
            node_dir = box.node_dir("a")
            env = box.child_env()
        box.assert_clean()
    """

    __test__ = False

    def __init__(
        self,
        name: str,
        *,
        root: str | Path | None = None,
        env: dict[str, str] | None = None,
        keep: bool = False,
        strict_env: bool = False,
        env_passthrough: Sequence[str] | None = None,
        include_sys_path: bool = True,
    ) -> None:
        if not name:
            raise ValueError("sandbox name must be non-empty")
        self.name = name
        self._owns_root = root is None
        self._root = (
            Path(tempfile.mkdtemp(prefix=f"actant-testkit-{_slug(name)}-"))
            if root is None
            else Path(root)
        )
        self._root.mkdir(parents=True, exist_ok=True)
        self._keep = keep
        self._env = {str(k): str(v) for k, v in (env or {}).items()}
        self._strict_env = strict_env
        self._env_passthrough = tuple(
            env_passthrough if env_passthrough is not None else _DEFAULT_PASSTHROUGH
        )
        self._include_sys_path = include_sys_path
        self._created_dirs: list[Path] = []
        self._cleanups: list[Callable[[], None]] = []
        self._created_at = time.time()
        self._cleaned = False
        self._checkout_dirs: set[Path] = set()
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        if self._strict_env:
            self.home_dir.mkdir(parents=True, exist_ok=True)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.config_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 路径
    # ------------------------------------------------------------------

    @property
    def root(self) -> Path:
        """沙箱根目录。"""
        return self._root

    @property
    def tmp_dir(self) -> Path:
        """子进程临时目录（``TMPDIR`` 指向此处）。"""
        return self._root / "tmp"

    @property
    def log_dir(self) -> Path:
        """子进程日志目录。"""
        return self._root / "logs"

    @property
    def home_dir(self) -> Path:
        """隔离子进程的 ``HOME``。"""
        return self._root / "home"

    @property
    def cache_dir(self) -> Path:
        """隔离子进程的 ``XDG_CACHE_HOME``。"""
        return self._root / "cache"

    @property
    def config_dir(self) -> Path:
        """隔离子进程的 ``XDG_CONFIG_HOME``。"""
        return self._root / "config"

    def path(self, *parts: str) -> Path:
        """拼接沙箱内路径（不自动创建）。"""
        return self._root.joinpath(*parts)

    def node_dir(self, node: str) -> Path:
        """返回并创建某节点的 ``data_dir``。"""
        target = self._root / "nodes" / _slug(node)
        target.mkdir(parents=True, exist_ok=True)
        self._remember(target)
        return target

    def log_path(self, node: str) -> Path:
        """返回某节点的日志文件路径（父目录已创建）。"""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        return self.log_dir / f"{_slug(node)}.log"

    def _remember(self, path: Path) -> None:
        if path not in self._checkout_dirs:
            self._checkout_dirs.add(path)
            self._created_dirs.append(path)

    # ------------------------------------------------------------------
    # 子进程环境
    # ------------------------------------------------------------------

    def child_env(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """构造隔离子进程环境。

        不修改 ``os.environ``；返回的是完整环境副本，可直接传给
        ``subprocess.Popen(env=...)``。

        ``strict_env=True`` 时只保留 :attr:`env_passthrough` 白名单，并重定向
        ``HOME``/``XDG_*``/``TMPDIR``，避免宿主缓存、凭据与 pytest 变量泄漏到
        被测节点。
        """
        if self._strict_env:
            env = {
                key: os.environ[key]
                for key in self._env_passthrough
                if key in os.environ
            }
            env.setdefault("PATH", os.defpath)
        else:
            env = dict(os.environ)
        env.update(self._env)
        env["TMPDIR"] = str(self.tmp_dir)
        env["TEMP"] = str(self.tmp_dir)
        env["TMP"] = str(self.tmp_dir)
        env["ACTANT_TESTKIT_SANDBOX"] = str(self._root)
        env["ACTANT_TESTKIT_NAME"] = self.name
        if self._strict_env:
            env["HOME"] = str(self.home_dir)
            env["XDG_CACHE_HOME"] = str(self.cache_dir)
            env["XDG_CONFIG_HOME"] = str(self.config_dir)
        base_pythonpath = None if self._strict_env else env.get("PYTHONPATH")
        env["PYTHONPATH"] = _merge_pythonpath(
            base_pythonpath, include_sys_path=self._include_sys_path
        )
        if extra:
            env.update({str(k): str(v) for k, v in extra.items()})
        return env

    # ------------------------------------------------------------------
    # 清理
    # ------------------------------------------------------------------

    def track_cleanup(self, callback: Callable[[], None]) -> None:
        """登记逆序执行的清理回调（用于停止子进程等）。"""
        if self._cleaned:
            raise SandboxError("sandbox already cleaned")
        self._cleanups.append(callback)

    @property
    def keep(self) -> bool:
        """是否保留根目录（便于失败后人工排查）。"""
        return self._keep

    @keep.setter
    def keep(self, value: bool) -> None:
        self._keep = bool(value)

    @property
    def cleaned(self) -> bool:
        return self._cleaned

    def cleanup(self) -> None:
        """执行清理回调并删除根目录（幂等）。"""
        if self._cleaned:
            return
        self._cleaned = True
        errors: list[str] = []
        for callback in reversed(self._cleanups):
            try:
                callback()
            except Exception as exc:  # 清理必须继续，收集全部错误后统一抛出
                errors.append(f"{type(exc).__name__}: {exc}")
        if self._owns_root and not self._keep and self._root.exists():
            shutil.rmtree(self._root, ignore_errors=True)
        if errors:
            raise SandboxError("sandbox cleanup failed: " + "; ".join(errors))

    def leftovers(self) -> list[Path]:
        """返回清理后仍存在的沙箱内路径。

        ``keep=True`` 或根目录由调用方提供（沙箱不拥有）时不做泄漏判断。
        """
        if self._keep or not self._owns_root or not self._root.exists():
            return []
        return sorted(self._root.rglob("*"))

    def assert_clean(self) -> None:
        """断言沙箱已清理且无残留文件。"""
        if not self._cleaned:
            raise SandboxError("sandbox was not cleaned")
        remaining = self.leftovers()
        if remaining:
            listing = ", ".join(str(p) for p in remaining[:10])
            raise SandboxError(f"sandbox leaked {len(remaining)} path(s): {listing}")

    def manifest(self) -> dict[str, Any]:
        """返回可写入报告的沙箱清单。"""
        return {
            "name": self.name,
            "root": str(self._root),
            "owns_root": self._owns_root,
            "keep": self._keep,
            "cleaned": self._cleaned,
            "created_at": self._created_at,
            "env": dict(self._env),
            "strict_env": self._strict_env,
            "env_passthrough": list(self._env_passthrough),
            "include_sys_path": self._include_sys_path,
            "home_dir": str(self.home_dir),
            "node_dirs": [str(p) for p in sorted(self._checkout_dirs)],
            "tmp_dir": str(self.tmp_dir),
        }

    # ------------------------------------------------------------------
    # 上下文
    # ------------------------------------------------------------------

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._keep:
            self._cleaned = True
            return
        self.cleanup()

    def __repr__(self) -> str:
        return f"Sandbox(name={self.name!r}, root={str(self._root)!r})"


@contextmanager
def sandbox(name: str, **kwargs: Any) -> Iterator[Sandbox]:
    """``Sandbox`` 的上下文管理器快捷方式。"""
    box = Sandbox(name, **kwargs)
    try:
        yield box
    finally:
        box.cleanup()


def _slug(value: str) -> str:
    """把节点/沙箱名转换为安全的目录名片段。"""
    cleaned = "".join(ch if (ch.isalnum() or ch in "-_.") else "-" for ch in value)
    return cleaned.strip("-") or "node"


def _merge_pythonpath(existing: str | None, *, include_sys_path: bool = True) -> str:
    """构造子进程 ``PYTHONPATH``：包根 + 当前解释器 ``sys.path`` + 既有值。

    ``include_sys_path=False`` 时只保留包根与既有值，便于验证"安装包"而非
    "开发树"的行为。
    """
    parts: list[str] = [str(PACKAGE_ROOT)]
    if include_sys_path:
        for item in sys.path:
            if item and item not in parts:
                parts.append(item)
    if existing:
        for item in existing.split(os.pathsep):
            if item and item not in parts:
                parts.append(item)
    return os.pathsep.join(parts)


_DEFAULT_PASSTHROUGH = (
    "PATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TZ",
    "TERM",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
)
