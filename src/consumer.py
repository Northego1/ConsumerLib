import asyncio
from typing import Any, Awaitable, Callable, Self

from aio_pika import Queue, IncomingMessage
from aio_pika.abc import AbstractIncomingMessage


class Singleton(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if not hasattr(cls, "_instance"):
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class Consumer(metaclass=Singleton):
    def __init__(
            self: Self,
    ) -> None:


        self.endpoints = {}
        self.queues = []


    def task(self: Self, routing_key: str) -> Callable[..., Any]:
        def wrapper(func: Callable[..., Any]) -> Any:
            self.endpoints[routing_key] = func
            return func
        return wrapper


    def declare_queue(
            self: Self,
    ): ...


    async def _process_message(
            self: Self,
            func: Callable[..., Awaitable],
            message: AbstractIncomingMessage,
    )-> None:
        result = await func(message)
        if message.reply_to and isinstance(result, int):
            


    async def _consume(self: Self, queue: Queue) -> None:
        async for message in queue:
            route = message.routing_key
            if route in self.endpoints:
                asyncio.create_task(self._process_message(self.endpoints[route], message))  # noqa: RUF006
