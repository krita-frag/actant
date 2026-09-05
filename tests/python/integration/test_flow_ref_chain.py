"""R6：flow 依赖边携带 Ref——flow 内大值生产→消费链路集成测试。

数据流口径（plans/REF_DESIGN.md 方案①）：

- 生产者 worker 一次 pickle 结果帧 → 父进程把帧字节原样落 blob →
  handle 持 Ref（对象不留在提交方）；
- 消费任务在 flow 体中 eager 提交（早于上游完成，pending → Ref 路径），
  依赖解析保留 Ref、不反序列化；
- 提交方把 blob 原始帧字节内联进消费者 envelope（字节级搬运），消费者
  worker 一次 loads 取值。

断言选型（可确定性验证者）：

1. 消费者 worker 收到正确数据：消费任务是内容 hash，与测试端预期相等。
2. 生产者大结果走 Ref 路径：``big.ref() is not None``（对象缓存态时为
   ``None``）。
3. 提交方解析阶段无整对象反序列化：``Ref.result`` 被 monkeypatch 为抛错
   ——flow 的依赖解析、DAG 回灌（``_export_outcome`` 对 Ref 态原样返回
   引用字节）、终态广播全程若触碰对象级反序列化即失败。
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

import pytest

from actant import Runtime, flow, task
from actant.task._ref import REF_INLINE_THRESHOLD, Ref

# 2MB：超过结果帧阈值即可触发 Ref 路径，控制集成测试开销。
_SIZE = REF_INLINE_THRESHOLD * 2

# 上游任务的短暂延迟，保证 flow 体在结果抵达前提交下游任务（pending 态）。
_PRODUCE_DELAY_S = 0.2


def _pattern(size: int) -> bytes:
    """确定性内容：两端各自计算 sha256 后比对，无需传输期望值。"""
    return (bytes(range(256)) * (size // 256 + 1))[:size]


@task(name="ref_flow_produce")
def _produce(size: int) -> bytes:
    time.sleep(_PRODUCE_DELAY_S)
    return _pattern(size)[:size]


@task(name="ref_flow_consume")
def _consume(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.timeout(120)
def test_flow_large_value_chain_no_deserialization_in_submitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """flow 内 A→B 大值链：worker 收到正确数据，提交方全程零对象级反序列化。"""

    def _boom(self: Ref, timeout: float | None = None) -> Any:
        raise AssertionError(
            "Ref.result() was called during the flow; the large value "
            "reached the submitter as an object (R6 regression)"
        )

    monkeypatch.setattr(Ref, "result", _boom)
    expected = hashlib.sha256(_pattern(_SIZE)).hexdigest()

    @flow(name="wf-ref-chain")
    def pipeline(size: int) -> str:
        big = _produce.submit(size)
        digest = _consume.submit(big)
        result = digest.result()
        # digest.result() 返回时上游必然已终态：此时大结果必须是 Ref 态。
        assert big.ref() is not None, (
            "large upstream result must hold a Ref, not an inline object"
        )
        return result

    with Runtime.with_defaults() as rt:
        rt.serve()
        result = pipeline(_SIZE)

    assert result == expected
