"""可靠性/混沌 e2e 的任务函数规范模块。

cloudpickle 按引用序列化任务函数（``module`` + ``qualname``），worker 子进程
按模块路径导入。pytest 在 ``tests/`` 缺少 ``__init__.py`` 时会把测试模块导入为
``python.e2e.*`` 这类非规范名——该名字只在 pytest 进程的 ``sys.path``（含
``tests/`` 目录）下可解析，一旦任务被路由到子进程节点（其 worker 只有仓库根
可导入路径），反序列化即报 ``No module named 'python'``。

因此所有可能跨进程执行的任务函数统一定义在本模块，并通过规范名
``tests.python._helpers.reliability_tasks`` 导入，保证任意节点/worker 进程
都能按引用反序列化。
"""

from __future__ import annotations

import os
import signal
import time


def touch_and_sleep(marker_path: str, seconds: float) -> str:
    """写 start 标记 → sleep → 写 done 标记。用于在父进程侧观察任务进度。"""
    with open(marker_path, "a", encoding="utf-8") as f:
        f.write("start\n")
        f.flush()
    time.sleep(seconds)
    with open(marker_path, "a", encoding="utf-8") as f:
        f.write("done\n")
    return "ok"


def crash_once(marker_path: str) -> str:
    """首次执行时杀死所在 worker 进程，重路由后的第二次执行返回成功。"""
    with open(marker_path, "a", encoding="utf-8") as f:
        f.write("attempt\n")
    with open(marker_path, encoding="utf-8") as f:
        attempts = f.read().count("attempt")
    if attempts == 1:
        os.kill(os.getpid(), signal.SIGKILL)
    return "recovered"


def crash_always(marker_path: str) -> str:
    """每次执行都杀死所在 worker 进程，耗尽 crash_failover 配额后终态失败。"""
    with open(marker_path, "a", encoding="utf-8") as f:
        f.write("attempt\n")
    os.kill(os.getpid(), signal.SIGKILL)
    return "never"


def quick(x: int) -> int:
    """无副作用快速任务：验证节点存活与批次收敛。"""
    return x * 2


def noop() -> int:
    """最小工作量任务：吞吐/背压基线的度量单元。"""
    return 42


def sleep_mult(x: int, seconds: float) -> int:
    """可控时长的任务：sleep 降序提交即天然形成乱序完成。"""
    time.sleep(seconds)
    return x * 10
