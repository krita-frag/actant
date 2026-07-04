"""Python 侧可观测性：环境变量驱动的 viztracer 集成。

默认不启用。设置环境变量 ``ACTANT_VIZTRACER`` 为输出文件路径即可在
``_Node.start()`` 时启动 viztracer，在 ``_Node.shutdown()`` 时停止并
写出报告。

支持的环境变量
~~~~~~~~~~~~~~

================================  ==============================================
``ACTANT_VIZTRACER``             输出文件路径（如 ``/tmp/actant.json``）。
                                 设置后启用 viztracer。
``ACTANT_VIZTRACER_MAX_ENTRIES``  最大事件条目数（默认 ``1000000``）。
``ACTANT_VIZTRACER_PID_SUFFIX``  设为 ``1`` 在文件名追加 PID，
                                 便于多进程并行 trace。
================================  ==============================================

示例
~~~~

.. code-block:: bash

    ACTANT_VIZTRACER=/tmp/actant.json python my_app.py
    viztracer /tmp/actant.json   # 用 vizviewer 查看

仅在 Python 层提供调用追踪（函数级）。Rust 侧的 tracing/pprof/tokio-console
由 ``src/observability.rs`` 通过独立环境变量控制，二者互不依赖。
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from viztracer import VizTracer


_tracer: VizTracer | None = None


def _maybe_create() -> VizTracer | None:
    """根据环境变量构造 VizTracer，未启用返回 None。"""
    path = os.environ.get("ACTANT_VIZTRACER")
    if not path:
        return None
    try:
        from viztracer import VizTracer
    except ImportError:
        # viztracer 未安装时静默跳过 —— 它是可选依赖。
        # 不抛异常以免阻塞运行时启动。
        return None

    max_entries = int(os.environ.get("ACTANT_VIZTRACER_MAX_ENTRIES", "1000000"))
    if os.environ.get("ACTANT_VIZTRACER_PID_SUFFIX") == "1":
        path = f"{path}.{os.getpid()}"
    return VizTracer(max_entries=max_entries, output_file=path)


def start() -> None:
    """在节点启动时调用。若环境变量已设置则启动 tracer。"""
    global _tracer
    if _tracer is not None:
        return  # 已启动
    tracer = _maybe_create()
    if tracer is None:
        return
    tracer.start()
    _tracer = tracer


def stop() -> None:
    """在节点关闭时调用。停止 tracer 并写出报告。"""
    global _tracer
    if _tracer is None:
        return
    try:
        _tracer.stop()
        _tracer.save()
    except Exception:
        # 写出失败不应影响关闭流程。
        pass
    finally:
        _tracer = None


def is_enabled() -> bool:
    """viztracer 是否已启用。"""
    return _tracer is not None


def get_tracer() -> VizTracer | None:
    """暴露底层 tracer（供用户手动添加 marker/事件）。"""
    return _tracer


def trace_event(name: str, **kwargs: Any) -> None:
    """记录自定义事件。未启用时为 no-op。"""
    if _tracer is not None:
        _tracer.add_instant(name, args=kwargs if kwargs else None)
