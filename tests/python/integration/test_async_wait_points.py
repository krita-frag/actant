"""``Runtime.wait_wait_point_async``：await 期间 event loop 保持可用。"""

from __future__ import annotations

import asyncio

from actant import Runtime


def _register(rt: Runtime, workflow_ids: list[str], key: str = "go") -> None:
    for workflow_id in workflow_ids:
        rt.submit_workflow(workflow_id)
        rt.register_wait_point(workflow_id, key, kind="signal", name=key)


def test_awaited_wait_point_resolves_from_signal() -> None:
    with Runtime() as rt:
        workflow_id = "wf-await-signal"
        _register(rt, [workflow_id])

        async def _main() -> bytes | None:
            waiter = asyncio.ensure_future(
                rt.wait_wait_point_async(workflow_id, "go", timeout_ms=10_000)
            )
            # 若绑定阻塞调用线程，这一行在信号到达前不会执行。
            await asyncio.sleep(0.05)
            assert not waiter.done()
            rt.signal_wait_point(workflow_id, "go", b"payload")
            return await waiter

        assert asyncio.run(_main()) == b"payload"


def test_signal_delivered_before_await_is_not_lost() -> None:
    with Runtime() as rt:
        workflow_id = "wf-await-early"
        _register(rt, [workflow_id])
        assert rt.signal_wait_point(workflow_id, "go", b"early") == b"early"

        async def _main() -> bytes | None:
            return await rt.wait_wait_point_async(workflow_id, "go", timeout_ms=5_000)

        assert asyncio.run(_main()) == b"early"


def test_timeout_resolves_to_none() -> None:
    with Runtime() as rt:
        workflow_id = "wf-await-timeout"
        _register(rt, [workflow_id])

        async def _main() -> bytes | None:
            return await rt.wait_wait_point_async(workflow_id, "go", timeout_ms=50)

        assert asyncio.run(_main()) is None


def test_many_waiters_resolve_concurrently() -> None:
    with Runtime() as rt:
        workflow_ids = [f"wf-await-{index}" for index in range(20)]
        _register(rt, workflow_ids)

        async def _main() -> list[bytes | None]:
            waiters = [
                asyncio.ensure_future(rt.wait_wait_point_async(workflow_id, "go"))
                for workflow_id in workflow_ids
            ]
            await asyncio.sleep(0.05)
            assert all(not waiter.done() for waiter in waiters)
            for index, workflow_id in enumerate(workflow_ids):
                rt.signal_wait_point(workflow_id, "go", str(index).encode())
            return list(await asyncio.gather(*waiters))

        assert asyncio.run(_main()) == [str(index).encode() for index in range(20)]
