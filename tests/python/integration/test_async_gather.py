"""异步面集成测试：``aio.gather`` / ``aio.perform_batch`` / ``AsyncResult.__await__``。"""

from __future__ import annotations

import asyncio
import uuid

import pytest

import actant
from actant import aio
from actant.capabilities import StoreReq


@actant.task
def _add(a: int, b: int) -> int:
    return a + b


@actant.task
def _fail() -> None:
    raise ValueError("intentional failure")


@pytest.mark.integration
class TestAsyncResultAwait:
    """``AsyncResult.__await__`` 使任务句柄可直接 ``await``。"""

    def test_await_single_handle(self) -> None:
        with actant.Runtime():
            h = _add.submit(3, 4)
            result = asyncio.run(self._await(h))
            assert result == 7

    async def _await(self, h):
        return await h

    def test_await_in_async_flow(self) -> None:
        """在 async def 中 await 多个 handle（顺序提交+等待）。"""
        with actant.Runtime():

            async def main():
                a = _add.submit(1, 2)
                ra = await a
                b = _add.submit(3, 4)
                rb = await b
                return [ra, rb]

            results = asyncio.run(main())
            assert results == [3, 7]


@pytest.mark.integration
class TestGatherAsync:
    """``aio.gather`` 异步并行等待多个 handle。"""

    def test_gather_basic(self) -> None:
        with actant.Runtime():

            async def main():
                a = _add.submit(1, 2)
                b = _add.submit(3, 4)
                c = _add.submit(5, 6)
                return await aio.gather(a, b, c)

            results = asyncio.run(main())
            assert results == [3, 7, 11]

    def test_gather_return_exceptions(self) -> None:
        with actant.Runtime():

            async def main():
                a = _add.submit(1, 2)
                b = _fail.submit()
                return await aio.gather(a, b, return_exceptions=True)

            results = asyncio.run(main())
            assert results[0] == 3
            assert isinstance(results[1], ValueError)

    def test_gather_empty_raises(self) -> None:
        with actant.Runtime():

            async def main():
                with pytest.raises(ValueError):
                    await aio.gather()

            asyncio.run(main())

    def test_gather_propagates_exception(self) -> None:
        with actant.Runtime():

            async def main():
                a = _add.submit(1, 2)
                b = _fail.submit()
                await aio.gather(a, b)

            with pytest.raises(ValueError, match="intentional failure"):
                asyncio.run(main())

    def test_gather_single_handle(self) -> None:
        with actant.Runtime():

            async def main():
                a = _add.submit(10, 20)
                return await aio.gather(a)

            results = asyncio.run(main())
            assert results == [30]


@pytest.mark.integration
class TestPerformBatchAsync:
    """``aio.perform_batch``：Rust 批量快路径与混合条目回退。"""

    def test_rust_backed_batch_roundtrip(self) -> None:
        """全部条目为 Rust-backed：走 ``perform_batch_async`` 单次边界穿越。"""
        suffix = uuid.uuid4().hex
        key_a = f"pb-{suffix}-a".encode()
        key_b = f"pb-{suffix}-b".encode()
        with actant.Runtime():

            async def main():
                await aio.perform_batch(
                    [
                        ("Store", StoreReq(op="put", key=key_a, value=b"v1")),
                        ("Store", StoreReq(op="put", key=key_b, value=b"v2")),
                    ]
                )
                return await aio.perform_batch(
                    [
                        ("Store", StoreReq(op="get", key=key_a)),
                        ("Store", StoreReq(op="get", key=key_b)),
                    ]
                )

            results = asyncio.run(main())
        assert [bytes(value) for value in results] == [b"v1", b"v2"]

    def test_mixed_batch_falls_back_to_per_item(self) -> None:
        """含 Python handler 的条目不能走 Rust 批量：逐条分发。"""
        rt = actant.Runtime()
        rt.layer("MyPerform", kind="perform").chain(lambda x: f"py:{x}")
        with rt:

            async def main():
                return await aio.perform_batch(
                    [
                        ("Store", StoreReq(op="get", key=b"pb-missing")),
                        ("MyPerform", "a"),
                    ]
                )

            results = asyncio.run(main())
        assert results[0] is None
        assert results[1] == "py:a"
