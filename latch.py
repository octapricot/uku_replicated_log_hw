import asyncio

class AsyncCountDownLatch:
    def __init__(self, count):
        self.count = count
        self.condition = asyncio.Condition()

    async def count_down(self):
        async with self.condition:
            self.count -= 1
            if self.count <= 0:
                self.condition.notify_all()

    async def await_latch(self):
        async with self.condition:
            while self.count > 0:
                await self.condition.wait()