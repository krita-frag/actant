"""可复现报告：记录集群时间线并序列化为 JSON。

报告回答三个问题：**在什么环境上**（manifest）、**按什么顺序做了什么**
（timeline/faults）、**观测到了什么**（trace/metrics）。仅记录编排与观测
数据，不序列化 Runtime 对象；目标是把失败现场压缩成可提交、可 diff、
可重放（配合 ``seed``）的 JSON。
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any


class ClusterReport:
    """线程安全的事件时间线 + 可复现清单。

    记录的是编排动作（节点起停、拓扑变更、故障注入、收敛/失联断言）与
    观测摘要（trace/metrics），不含业务负载细节。
    """

    def __init__(self, *, namespace: str, seed: int | None = None) -> None:
        self.namespace = namespace
        self.seed = seed
        self._events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._started_at = time.time()
        self._manifest: dict[str, Any] = {}
        self._faults: list[dict[str, Any]] = []
        self._trace: dict[str, Any] | None = None
        self._metrics: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # 时间线
    # ------------------------------------------------------------------

    def record(self, kind: str, **fields: Any) -> None:
        """追加一条带相对时间戳的事件。"""
        with self._lock:
            self._events.append(
                {
                    "at_ms": round((time.time() - self._started_at) * 1000, 1),
                    "kind": kind,
                    **fields,
                }
            )

    def record_fault(self, fault: dict[str, Any]) -> None:
        """记录一条已应用/已计划的故障。"""
        with self._lock:
            self._faults.append(
                {"at_ms": round((time.time() - self._started_at) * 1000, 1), **fault}
            )

    # ------------------------------------------------------------------
    # 清单与观测摘要
    # ------------------------------------------------------------------

    def set_manifest(self, manifest: dict[str, Any]) -> None:
        """设置/合并环境清单（seed、拓扑、节点模式、配置摘要）。"""
        with self._lock:
            self._manifest.update(manifest)

    @property
    def manifest(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._manifest)

    def attach_trace(self, trace: Any) -> None:
        """附加 trace 摘要（``Trace.summary()`` 或等价 dict）。"""
        data = trace if isinstance(trace, dict) else trace.summary()
        with self._lock:
            self._trace = dict(data)

    def attach_metrics(self, metrics: Any) -> None:
        """附加性能摘要（``PerformanceSampler.summary()`` 或等价 dict）。"""
        data = metrics if isinstance(metrics, dict) else metrics.summary()
        with self._lock:
            self._metrics = dict(data)

    # ------------------------------------------------------------------
    # 序列化
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "namespace": self.namespace,
                "seed": self.seed,
                "duration_ms": round((time.time() - self._started_at) * 1000, 1),
                "manifest": dict(self._manifest),
                "events": [dict(e) for e in self._events],
                "faults": [dict(f) for f in self._faults],
                "trace": dict(self._trace) if self._trace is not None else None,
                "metrics": dict(self._metrics) if self._metrics is not None else None,
            }

    def dump(self, path: str | Path) -> Path:
        """把报告写入 JSON 文件（父目录需存在）。"""
        target = Path(path)
        target.write_text(json.dumps(self.snapshot(), ensure_ascii=False, indent=2))
        return target

    @classmethod
    def load(cls, path: str | Path) -> dict[str, Any]:
        """读取 ``dump()`` 生成的 JSON（返回原始 dict，便于断言/存档）。"""
        data: dict[str, Any] = json.loads(Path(path).read_text())
        return data
