"""P2-1: async gather + AsyncResult.__await__ 测试。"""

from __future__ import annotations

import asyncio

import pytest

import actant


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
        with actant.Runtime.with_defaults():
            h = _add.submit(3, 4)
            result = asyncio.run(self._await(h))
            assert result == 7

    async def _await(self, h):
        return await h

    def test_await_in_async_flow(self) -> None:
        """在 async def 中 await 多个 handle（顺序提交+等待）。"""
        with actant.Runtime.with_defaults():

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
    """``gather_async`` 异步并行等待多个 handle。"""

    def test_gather_basic(self) -> None:
        with actant.Runtime.with_defaults():

            async def main():
                a = _add.submit(1, 2)
                b = _add.submit(3, 4)
                c = _add.submit(5, 6)
                return await actant.gather_async(a, b, c)

            results = asyncio.run(main())
            assert results == [3, 7, 11]

    def test_gather_return_exceptions(self) -> None:
        with actant.Runtime.with_defaults():

            async def main():
                a = _add.submit(1, 2)
                b = _fail.submit()
                return await actant.gather_async(a, b, return_exceptions=True)

            results = asyncio.run(main())
            assert results[0] == 3
            assert isinstance(results[1], ValueError)

    def test_gather_empty_raises(self) -> None:
        with actant.Runtime.with_defaults():

            async def main():
                with pytest.raises(ValueError):
                    await actant.gather_async()

            asyncio.run(main())

    def test_gather_propagates_exception(self) -> None:
        with actant.Runtime.with_defaults():

            async def main():
                a = _add.submit(1, 2)
                b = _fail.submit()
                await actant.gather_async(a, b)

            with pytest.raises(ValueError, match="intentional failure"):
                asyncio.run(main())

    def test_gather_single_handle(self) -> None:
        with actant.Runtime.with_defaults():

            async def main():
                a = _add.submit(10, 20)
                return await actant.gather_async(a)

            results = asyncio.run(main())
            assert results == [30]
