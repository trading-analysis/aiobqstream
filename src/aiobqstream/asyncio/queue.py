import asyncio
from collections.abc import Coroutine
from typing import Any


class QueueTask:
    def __init__(self):
        self._queue: asyncio.Queue[
            Coroutine[Any, Any, None] | StopQueue
        ] = asyncio.Queue()
        self._task: asyncio.Task = asyncio.create_task(self._consume())

    async def _consume(self):
        while True:
            item = await self._queue.get()
            if isinstance(item, StopQueue):
                break
            await item

    def produce(self, item: Coroutine[Any, Any, Any]):
        self._queue.put_nowait(item)

    async def _exit(self):
        self._queue.put_nowait(StopQueue())
        await self._task


class StopQueue:
    pass
