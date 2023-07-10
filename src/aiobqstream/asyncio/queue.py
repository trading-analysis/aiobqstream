import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)


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
            try:
                await item
            except Exception as e:
                logger.warning(repr(e))

    def produce(self, item: Coroutine[Any, Any, Any]):
        self._queue.put_nowait(item)

    async def _exit(self):
        self._queue.put_nowait(StopQueue())
        await self._task


class StopQueue:
    pass
