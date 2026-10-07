import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import httpx
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import RabbitBroker, RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Payment
from app.schemas import PaymentCreate
from app.services.gateway import ChargeResult
from app.services.payments import PaymentService


async def eventually(
    predicate: Callable[[], Awaitable[bool]], *, within: float = 10.0, interval: float = 0.05
) -> None:
    """Wait until the async predicate holds."""
    try:
        async with asyncio.timeout(within):
            while not await predicate():  # noqa: ASYNC110 - polls external state (DB, broker)
                await asyncio.sleep(interval)
    except TimeoutError:
        raise AssertionError(f"Condition not met within {within}s") from None


async def create_payment(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    webhook_url: str = "http://merchant.test/webhooks",
    idempotency_key: str | None = None,
) -> Payment:
    data = PaymentCreate.model_validate(
        {
            "amount": "100.00",
            "currency": "RUB",
            "description": "Test payment",
            "metadata": {"order_id": 42},
            "webhook_url": webhook_url,
        }
    )
    async with session_factory() as session:
        result = await PaymentService(session).create_payment(
            data, idempotency_key or str(uuid.uuid4())
        )
    return result.payment


async def get_payment(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


async def take_messages(broker: RabbitBroker, queue: RabbitQueue) -> list[AbstractIncomingMessage]:
    """Remove and return all messages currently in the queue."""
    declared = await broker.declare_queue(queue)
    messages: list[AbstractIncomingMessage] = []
    while (message := await declared.get(no_ack=True, fail=False)) is not None:
        messages.append(message)
    return messages


async def queue_depth(broker: RabbitBroker, queue: RabbitQueue) -> int:
    """Number of messages ready in the queue, without consuming them."""
    declared = await broker.declare_queue(queue)
    return (await declared.declare()).message_count or 0


async def purge(broker: RabbitBroker, queues: Iterable[RabbitQueue]) -> None:
    for queue in queues:
        await (await broker.declare_queue(queue)).purge()


@dataclass
class SpyGateway:
    """Payment gateway double: records charges and returns a fixed result."""

    succeeded: bool = True
    charged: list[uuid.UUID] = field(default_factory=list)

    async def charge(self, payment: Payment) -> ChargeResult:
        self.charged.append(payment.id)
        return ChargeResult(succeeded=self.succeeded)


@dataclass
class WebhookEndpoint:
    """Merchant endpoint double for httpx.MockTransport.

    Answers with `statuses` one by one (the last one repeats) and records every request.
    """

    statuses: list[int] = field(default_factory=lambda: [200])
    requests: list[httpx.Request] = field(default_factory=list)
    received_at: list[float] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.received_at.append(time.monotonic())
        status = self.statuses[min(len(self.requests), len(self.statuses)) - 1]
        return httpx.Response(status)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

    @property
    def payloads(self) -> list[dict[str, Any]]:
        return [json.loads(request.content) for request in self.requests]
