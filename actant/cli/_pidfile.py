"""PID 文件管理：用于 worker daemon 进程的生命周期控制。"""

from __future__ import annotations

import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


def _default_pid_dir() -> Path:
    """返回默认 PID 文件目录。"""
    return Path.home() / ".actant"


def pid_path(name: str, pid_dir: str | None = None) -> Path:
    """返回指定 worker 的 PID 文件路径。"""
    base: Path = Path(pid_dir) if pid_dir else _default_pid_dir()
    try:
        base.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        # 回退到临时目录
        base = Path(tempfile.gettempdir()) / "actant"
        base.mkdir(parents=True, exist_ok=True)
    return base / f"{name}.pid"


def write_pid(name: str, pid_dir: str | None = None) -> Path:
    """写入当前进程 PID 到文件。如果已有同名的活跃进程则报错退出。

    使用 O_CREAT | O_EXCL | O_NOFOLLOW 原子创建文件，
    防止符号链接攻击（symlink attack）。
    """
    path: Path = pid_path(name, pid_dir)

    # 检查是否有同名 worker 已在运行
    if path.exists() or path.is_symlink():
        existing = _read_pid_safe(path)
        if existing is not None and _is_alive(existing):
            print(
                f"error: worker '{name}' is already running (pid {existing})",
                file=sys.stderr,
            )
            sys.exit(1)
        # 陈旧 PID 文件（或悬空符号链接），清理
        _safe_unlink(path)

    # 原子创建：O_CREAT | O_EXCL 确保文件不存在时才创建，
    # O_NOFOLLOW 拒绝符号链接，防止 symlink attack。
    flags = os.O_CREAT | os.O_WRONLY | os.O_EXCL | os.O_NOFOLLOW
    try:
        fd = os.open(str(path), flags)
    except FileExistsError:
        # 竞态条件：其他进程在我们检查后创建了文件
        existing = _read_pid_safe(path)
        if existing is not None and _is_alive(existing):
            print(
                f"error: worker '{name}' is already running (pid {existing})",
                file=sys.stderr,
            )
            sys.exit(1)
        _safe_unlink(path)
        fd = os.open(str(path), flags)

    with os.fdopen(fd, "w") as f:
        f.write(str(os.getpid()))
    return path


def write_pid_value(pid: int, name: str, pid_dir: str | None = None) -> Path:
    """写入指定 PID 到文件（用于父进程代子进程写入）。

    与 write_pid 不同，不检查当前进程是否已运行，因为此函数
    用于 Windows daemon 化时父进程代子进程写入 PID。
    """
    path: Path = pid_path(name, pid_dir)
    if path.exists() or path.is_symlink():
        existing = _read_pid_safe(path)
        if existing is not None and _is_alive(existing) and existing != pid:
            print(
                f"error: worker '{name}' is already running (pid {existing})",
                file=sys.stderr,
            )
            sys.exit(1)
        _safe_unlink(path)

    flags = os.O_CREAT | os.O_WRONLY | os.O_EXCL | os.O_NOFOLLOW
    try:
        fd = os.open(str(path), flags)
    except FileExistsError:
        _safe_unlink(path)
        fd = os.open(str(path), flags)

    with os.fdopen(fd, "w") as f:
        f.write(str(pid))
    return path


def remove_pid(name: str, pid_dir: str | None = None) -> None:
    """移除 PID 文件。"""
    path: Path = pid_path(name, pid_dir)
    path.unlink(missing_ok=True)


def read_worker_pid(name: str, pid_dir: str | None = None) -> int | None:
    """读取 worker 的 PID，如果进程不存在返回 None。"""
    path: Path = pid_path(name, pid_dir)
    if not path.exists():
        return None
    pid = _read_pid(path)
    if pid is None or not _is_alive(pid):
        path.unlink(missing_ok=True)
        return None
    return pid


def list_workers(pid_dir: str | None = None) -> list[dict[str, Any]]:
    """扫描 PID 目录,返回所有 worker 的状态列表。

    陈旧的 PID 文件(进程已退出)会被自动清理。
    """
    base: Path = Path(pid_dir) if pid_dir else _default_pid_dir()
    if not base.exists():
        return []

    result: list[dict[str, Any]] = []
    for pid_file in base.glob("*.pid"):
        name = pid_file.stem
        pid = _read_pid(pid_file)
        if pid is None or not _is_alive(pid):
            # 清理陈旧 PID 文件
            _safe_unlink(pid_file)
            result.append({"name": name, "running": False})
            continue
        result.append({"name": name, "running": True, "pid": pid})
    return result


def stop_worker(
    name: str, pid_dir: str | None = None, *, timeout: float = 10.0
) -> bool:
    """向 worker 进程发送终止信号，等待其退出；超时后强制终止。

    - Unix: SIGTERM → 等待 → SIGKILL 兜底
    - Windows: CTRL_BREAK_EVENT（进程组优雅关闭）→ 等待 → TerminateProcess 兜底

    返回 True 表示成功终止，False 表示进程不存在。
    """
    pid: int | None = read_worker_pid(name, pid_dir)
    if pid is None:
        return False

    # 发送优雅终止信号
    try:
        if sys.platform == "win32":
            # Windows: 进程以 CREATE_NEW_PROCESS_GROUP 创建，
            # CTRL_BREAK_EVENT 可被进程组捕获实现优雅关闭。
            os.kill(pid, signal.CTRL_BREAK_EVENT)
        else:
            os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        remove_pid(name, pid_dir)
        return False
    except (OSError, ValueError):
        # 某些信号在特定平台不可用，回退到 SIGTERM
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            remove_pid(name, pid_dir)
            return False

    # 等待进程退出
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _is_alive(pid):
            remove_pid(name, pid_dir)
            return True
        time.sleep(0.1)

    # 超时仍未退出，强制终止
    try:
        if sys.platform == "win32":
            # Windows: os.kill(pid, SIGTERM) 映射到 TerminateProcess（硬终止）
            os.kill(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    remove_pid(name, pid_dir)
    return True


def worker_status(name: str, pid_dir: str | None = None) -> dict[str, Any]:
    """返回 worker 状态信息。"""
    pid: int | None = read_worker_pid(name, pid_dir)
    if pid is None:
        return {"name": name, "running": False}
    return {"name": name, "running": True, "pid": pid}


def _read_pid(path: Path) -> int | None:
    """从文件读取 PID 值。"""
    try:
        return int(path.read_text().strip())
    except (ValueError, OSError):
        return None


def _read_pid_safe(path: Path) -> int | None:
    """从文件读取 PID 值，安全处理符号链接。"""
    if path.is_symlink():
        return None
    return _read_pid(path)


def _safe_unlink(path: Path) -> None:
    """安全删除文件或符号链接。"""
    try:
        if path.is_symlink():
            path.unlink()
        else:
            path.unlink(missing_ok=True)
    except OSError:
        pass


def _is_alive(pid: int) -> bool:
    """检查进程是否存活。"""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # 进程存在但无权限发送信号，仍然认为存活
        return True
