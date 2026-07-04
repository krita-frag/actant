"""Actor 示例：有状态计数器。

通过 actant.start 启动常驻节点,然后在同一进程内创建 Actor 并调用方法。
"""

import asyncio

import actant
from actant import Actor


class Counter(Actor):
    def __init__(self):
        self.count = 0

    def increment(self, n: int) -> int:
        self.count += n
        return self.count

    def get(self) -> int:
        return self.count


# 启动常驻节点
node = actant.start("actor_demo", signing_key="example-key")


# 创建 Actor 实例
counter = node.create_actor(Counter)


async def main():
    await counter.increment(5)
    await counter.increment(3)
    result = await counter.get()
    print(f"counter = {result}")  # 8


try:
    asyncio.run(main())
finally:
    actant.stop()
