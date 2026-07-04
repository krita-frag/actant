"""Python 和 Rust 层的统一日志配置。

Python 模块使用标准库的 ``logging`` 模块。Rust 核心使用
``tracing``（启用了 ``log`` 特性），因此 tracing 事件被转发为
``log`` 记录。``pyo3-log`` 将 Rust 的 ``log`` 记录桥接到 Python 的
``logging`` 模块，使 Python 成为两层的单一接收端。

单次调用 :func:`configure_logging` 会完成以下设置：

1. **Python 端** — 使用流处理器配置根日志记录器。
2. **Rust 端** — 调用 ``actant.refresh_logger()`` 将 Rust 的
   ``log`` 最大级别与 Python 根日志记录器级别同步，确保
   ``tracing`` 事件在到达 ``pyo3_log`` 之前不会被过滤。

默认级别为 ``WARNING``（与标准库默认值一致）。
"""

from __future__ import annotations

import contextlib
import logging
import threading
from typing import Literal

LogLevel = Literal["error", "warn", "info", "debug", "trace"]

# 在映射到 ``logging.<LEVEL>`` 常量之前需要规范化的 Python 日志级别名称。
_LEVEL_ALIASES: dict[str, str] = {
    "warn": "WARNING",
    "trace": "DEBUG",
}

_DEFAULT_FORMAT = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

# 保护 _configured 标志与 root logger handlers 修改的锁。
# configure_logging 可能在多线程环境下被调用（如 worker 启动 + 应用代码
# 同时调用），不加锁会导致 handlers 列表短暂为空或重复添加。
_config_lock: threading.Lock = threading.Lock()
_configured: bool = False


def _to_python_level(level: str) -> int:
    """将级别名称转换为 ``logging`` 的数值级别。"""
    key = level.strip().lower()
    name = _LEVEL_ALIASES.get(key, key)
    return getattr(logging, name.upper(), logging.WARNING)


def configure_logging(
    level: str = "warn",
    *,
    format: str = _DEFAULT_FORMAT,
    datefmt: str = _DEFAULT_DATEFMT,
    force: bool = False,
) -> None:
    """配置 Python ``logging`` 并同步 Rust ``log`` 级别。

    Parameters
    ----------
    level:
        日志级别名称（``"error"``、``"warn"``、``"info"``、``"debug"``、
        ``"trace"``）。接受 tracing 风格和 Python 风格的名称。
    format:
        Python 流处理器的 ``logging.Formatter`` 格式字符串。
    datefmt:
        ``logging.Formatter`` 日期格式。
    force:
        如果为 ``True``，即使已配置过也会重新运行配置。
    """
    global _configured
    # 双重检查锁定：先无锁读 _configured（快路径），命中则直接返回；
    # 否则获取锁后再次检查，避免重复配置。
    if _configured and not force:
        return

    with _config_lock:
        if _configured and not force:
            return

        python_level = _to_python_level(level)

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format, datefmt=datefmt))
        root = logging.getLogger()
        root.setLevel(python_level)
        # 在锁内替换已存在的处理器，避免并发日志输出期间 handlers 列表
        # 短暂为空导致日志丢失，或重复添加导致日志重复打印。
        old_handlers = list(root.handlers)
        root.handlers.clear()
        root.addHandler(handler)
        # 关闭旧 handler，释放其底层资源（如文件句柄）。
        for old in old_handlers:
            with contextlib.suppress(Exception):
                old.close()

        try:
            from actant.actant import refresh_logger

            refresh_logger()
        except ImportError:
            # 原生扩展不可用 —— 纯 Python 配置对于不启动 Rust 运行时的单元测试仍然有用。
            pass

        _configured = True
