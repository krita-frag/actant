"""值引用（Ref）传输 e2e 的任务函数规范模块。

与 ``reliability_tasks`` 同理：cloudpickle 按引用序列化任务函数，worker 子进程
按模块路径导入，任务函数必须定义在规范包路径下（``tests.python._helpers``），
不能定义在 pytest 导入为非规范名的 e2e 测试模块里。
"""

from __future__ import annotations

import hashlib


def big_pattern(size: int) -> bytes:
    """确定性大字节模式：内容可预测，供两端各自计算 sha256 后比对。"""
    return (bytes(range(256)) * (size // 256 + 1))[:size]


def produce_big(size: int) -> bytes:
    """生产一个确定性大结果（>1MB 时触发结果帧落 blob → Ref 路径）。"""
    return big_pattern(size)


def consume_sha256(data: bytes) -> str:
    """消费大参数并返回内容摘要：跨节点字节一致性以 hash 相等判定。"""
    return hashlib.sha256(data).hexdigest()


def quick(x: int) -> int:
    """最小任务：故障注入后验证节点仍然存活收敛。"""
    return x * 2
