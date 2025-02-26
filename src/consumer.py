import asyncio
from typing import Any, Awaitable, Callable, Self

from aio_pika import Message, Queue
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from pamqp.common import Arguments


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

        self._default_channel: AbstractChannel
        self._connection: AbstractRobustConnection

    def task(self: Self, routing_key: str) -> Callable[..., Any]:
        def wrapper(func: Callable[..., Any]) -> Any:
            self.endpoints[routing_key] = func
            return func
        return wrapper


    async def add_queue(  # noqa: PLR0913
            self: Self,
            exchange: AbstractExchange,
            name: str | None = None,
            routing_key: str | None = None,
            *,
            durable: bool = False,
            exclusive: bool = False,
            passive: bool = False,
            auto_delete: bool = False,
            arguments: Arguments = None,
            timeout: float | None = None,  # noqa: ASYNC109
            use_new_channel: bool = False,
    ) -> None:
        if use_new_channel:
            channel = self._default_channel
        else:
            channel = await self._connection.channel()
        try:
            queue: AbstractQueue = await channel.declare_queue(
                name=name,
                durable=durable,
                exclusive=exclusive,
                passive=passive,
                auto_delete=auto_delete,
                arguments=arguments,
                timeout=timeout,
            )
            await queue.bind(exchange=exchange, routing_key=routing_key)
        except Exception as e:
            raise e from e


    async def _process_message(
            self: Self,
            func: Callable[..., Awaitable],
            message: AbstractIncomingMessage,
    )-> None:
        response = await func(message)
        if message.reply_to and isinstance(response, Message):
            response.correlation_id = message.correlation_id

            await message.channel.basic_publish(
                body=response.body,
                properties=response.properties,
                routing_key=message.reply_to,
            )


    async def _consume(self: Self, queue: Queue) -> None:
        async for message in queue:
            route = message.routing_key
            if route in self.endpoints:
                asyncio.create_task(self._process_message(self.endpoints[route], message))  # noqa: RUF006


    async def _setup(self: Self) -> None:
        if not hasattr(self, "_default_channel"):
            self._default_channel = await self._connection.channel()


    async def run(self: Self, connection: AbstractRobustConnection) -> None:
        self._connection = connection
        await self._setup()
        for i in range(len(self.queues)):
            asyncio.create_task(self._consume(self.queues[i])) 