"""值引用（Ref）跨节点传输 e2e（0.3.2 R6 验收）：双节点 100MB 单次序列化。

验收口径（plans/REF_DESIGN.md 方案① / ROADMAP §0.3.2）：

1. ``test_dual_node_100mb_single_serialization``：节点 A 任务返回 ~100MB
   bytes（结果帧超阈值 → 原样落 A 本地 blob，handle 持 Ref），节点 B 提交
   消费任务并携带该 Ref——B 本地未命中经 iroh-blobs 独立 ALPN 跨节点拉取，
   帧字节内联进消费任务载荷，消费者 worker 一次 loads 取值。内容一致性以
   两端各自计算的 sha256 相等判定（= 消费者收到的参数与生产端 blob 持有的
   值逐字节一致，无重序列化损伤）。

   **序列化次数口径（可测断言 + 结构保证）**：全链路 1 次 pickle（生产者
   worker 结果帧）+ 1 次 unpickle（消费者 worker 解 ``_RefArg``）。中间所有
   转发（A 落 blob、B 拉取、envelope、帧）只搬运字节。提交方（含生产节点
   父进程与消费节点父进程）无对象级反序列化直接可测：``Ref.result`` 被
   monkeypatch 为抛错，生产侧以 ``wait()``+``ref()`` 等终态（不触碰对象），
   消费侧以 ``Ref`` 作参数提交（解析路径只搬运 blob 字节）。期望 hash 由
   测试端按确定性模式分块流式计算，不在测试进程物化 100MB 期望值。

2. ``test_ref_small_result_inline_regression``：小结果（<1MB）行为与 0.3.1
   一致——对象缓存态直传，``ref()`` 为 ``None``，无 blob 参与。

3. ``test_ref_survives_blob_missing``：Ref 指向的 blob 缺失（blob hash 被篡
   改为不存在的值，等价于"消费前 blob 被清除"——本节点 blob 存储为 redb
   单文件 FsStore，无法按 blob 删文件；内容寻址下 hash 无对应数据即 blob
   缺失）→ 消费方提交动作同步收到语义化 ``NotFoundError``，不挂起、不崩
   溃，两节点存活。

RSS 为 ru_maxrss 峰值水位观测值（参考 test_chaos_baseline），打印供
docs/SLA_BASELINE.md 记录，不设阈值断言。
"""

from __future__ import annotations

import hashlib
import platform
import resource
import shutil
import tempfile
import time
from collections.abc import Iterator
from typing import Any

import pytest

import actant
from actant import Runtime
from actant.exceptions import NotFoundError
from actant.task import task
from actant.task._ref import Ref
from tests.python._helpers import connect_peers
from tests.python._helpers import ref_tasks as rt

# 100MB 验收值：e2e 分层超时 180s（conftest），实测远低于上限。
SIZE_100MB = 100 * 1024 * 1024
# 小结果回归：远小于 REF_INLINE_THRESHOLD（1MB）。
SIZE_SMALL = 64 * 1024
# blob 缺失用例的中间尺寸：超过阈值走 Ref 路径，同时控制用例开销。
SIZE_MEDIUM = 2 * 1024 * 1024


def _rss_peak_mib() -> tuple[float, float]:
    """进程峰值 RSS（MiB）：返回 (本进程, 已回收子进程)。

    macOS ru_maxrss 单位为字节，Linux 为 KiB。子进程峰值覆盖 worker
    （生产者/消费者的执行端）；父进程峰值覆盖两个节点 Runtime 所在进程。
    """
    scale = 1 if platform.system() == "Darwin" else 1024
    self_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale / (1024 * 1024)
    children_mib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * scale / (
        1024 * 1024
    )
    return self_mib, children_mib


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha_streamed(size: int, chunk: int = 4 * 1024 * 1024) -> str:
    """与 ``ref_tasks.big_pattern(size)`` 内容一致的流式 sha256。

    期望 hash 不在测试进程物化整个 100MB 字节串：按与 big_pattern 相同的
    256 字节周期块分片生成（chunk 为 256 的整数倍保证块对齐），逐片 update。
    """
    assert chunk % 256 == 0
    block = bytes(range(256))
    h = hashlib.sha256()
    remaining = size
    while remaining > 0:
        n = min(chunk, remaining)
        h.update((block * (n // 256 + 1))[:n])
        remaining -= n
    return h.hexdigest()


@pytest.fixture
def two_nodes() -> Iterator[tuple[Runtime, Runtime]]:
    """两个真实 iroh 进程内节点，双向发现收敛后交付，测试后回收。"""
    dir_a = tempfile.mkdtemp(prefix="actant-ref-a-")
    dir_b = tempfile.mkdtemp(prefix="actant-ref-b-")
    rt_a = Runtime.with_defaults(name="ref-a", data_dir=dir_a)
    rt_b = Runtime.with_defaults(name="ref-b", data_dir=dir_b)
    rt_a.start()
    rt_b.start()
    try:
        assert connect_peers(rt_a, rt_b, timeout_s=15.0), "P2P connection failed"
        yield rt_a, rt_b
    finally:
        rt_b.stop()
        rt_a.stop()
        shutil.rmtree(dir_a, ignore_errors=True)
        shutil.rmtree(dir_b, ignore_errors=True)


def _dangling_ref(ref_bytes: bytes) -> bytes:
    """把 BlobRef wire 字节中的 hash 区域（前 32 字节原始 blake3）翻转为
    不存在的内容 hash，构造"blob 缺失"引用（wire 结构保持可解码）。"""
    flip = ref_bytes[10] ^ 0xFF
    return ref_bytes[:10] + bytes([flip]) + ref_bytes[11:]


class TestRefTransfer:
    """Ref 跨节点数据流验收。"""

    def test_dual_node_100mb_single_serialization(
        self, two_nodes: tuple[Runtime, Runtime], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """双节点 100MB 对传：两端 hash 一致，Ref 路径生效，提交方零反序列化。

        生产者在节点 A 本地执行（结果帧落 A 的 blob）；消费任务从节点 B
        提交——B 本地必然未命中（只有 A 的结果回调执行过 value_store），
        跨节点 blob_fetch 是唯一取值途径，覆盖 iroh-blobs 独立 ALPN 传输。
        ``Ref.result`` 被替换为抛错：生产侧只 ``wait()``+``ref()``（等终态
        不取对象），消费侧直接传 ``Ref``（解析只搬运 blob 字节）——两个父
        进程（同为本测试进程）全程若发生对象级反序列化即失败。
        """
        rt_a, rt_b = two_nodes

        def _boom(_self: Ref, timeout: float | None = None) -> Any:
            raise AssertionError(
                "Ref.result() was called in the submitter process: the 100MB "
                "value was deserialized as an object (R6 regression)"
            )

        monkeypatch.setattr(Ref, "result", _boom)
        expected = _sha_streamed(SIZE_100MB)
        rss_before, children_before = _rss_peak_mib()
        started = time.monotonic()

        with actant.use_runtime(rt_a):
            producer = task(rt.produce_big).submit(SIZE_100MB)
            assert producer.wait(timeout=150), "producer did not complete in time"
            assert producer.state == "completed", f"producer state={producer.state}"
            ref = producer.ref()
            assert ref is not None, (
                "100MB result frame must degrade to Ref (blob path), "
                "not inline object cache"
            )

        with actant.use_runtime(rt_b):
            consumer = task(rt.consume_sha256).submit(ref)
            digest = consumer.result(timeout=150)

        elapsed = time.monotonic() - started
        # 内容逐字节一致 = 生产端 blob 持有的值与消费者 worker 取到的参数一致，
        # 中途无重序列化损伤。
        assert digest == expected
        # Ref 来源节点必须是生产节点 A（跨节点寻址依据）。
        assert ref.node == rt_a.listen_addresses()["endpoint_addr"]
        rss_after, children_after = _rss_peak_mib()
        print(
            f"\n[ref-transfer] 100MB dual node: elapsed={elapsed:.1f}s "
            f"parent_rss_peak={rss_after:.0f}MiB (was {rss_before:.0f}) "
            f"worker_rss_peak={children_after:.0f}MiB (was {children_before:.0f})"
        )

    def test_ref_small_result_inline_regression(self, two_nodes) -> None:
        """小结果（<1MB）与 0.3.1 行为一致：对象态直传，ref() 为 None。

        结果与参数两条路径都验证：无 blob 参与（对象缓存态），值语义与
        直传对象完全相同。
        """
        rt_a, rt_b = two_nodes
        expected = rt.big_pattern(SIZE_SMALL)

        with actant.use_runtime(rt_a):
            producer = task(rt.produce_big).submit(SIZE_SMALL)
            value = producer.result(timeout=30)
            assert value == expected
            assert producer.ref() is None, (
                "small result must stay object-cached, not degrade to Ref"
            )

        # 小值直传对象参数（<1MB 不触发参数侧降级），跨节点投递后取回。
        with actant.use_runtime(rt_b):
            consumer = task(rt.consume_sha256).submit(value)
            digest = consumer.result(timeout=30)

        assert digest == _sha(expected)
        assert consumer.ref() is None

    def test_ref_survives_blob_missing(self, two_nodes) -> None:
        """Ref 指向的 blob 缺失：消费方收到语义化错误，节点不挂起不崩溃。

        消费动作 = 携带 Ref 的 submit：解析发生在提交方父进程（方案①），
        blob 缺失表现为 submit 同步抛 ``NotFoundError``（Rust
        ``ActantError::NotFound`` 镜像）——本地未命中 + 远端 provider 无此
        hash，均为确定性路径，无轮询窗口。
        """
        rt_a, rt_b = two_nodes

        with actant.use_runtime(rt_a):
            producer = task(rt.produce_big).submit(SIZE_MEDIUM)
            producer.result(timeout=60)
            ref = producer.ref()
            assert ref is not None
        assert isinstance(ref, Ref)

        with actant.use_runtime(rt_b), pytest.raises(NotFoundError):
            # 悬空 hash 必须包成 Ref 对象随参数提交：解析发生在提交方
            # 父进程（方案①），_materialize_refs 对 Ref 调 value_fetch
            # 才触发取值路径；裸 bytes 参数不会触发解析。
            task(rt.consume_sha256).submit(Ref(_dangling_ref(ref._ref_bytes)))

        # 两节点存活：故障后小任务正常收敛（不因值引用基建故障降级）。
        with actant.use_runtime(rt_a):
            assert task(rt.quick).submit(1).result(timeout=30) == 2
        with actant.use_runtime(rt_b):
            assert task(rt.quick).submit(2).result(timeout=30) == 4
