import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from itertools import pairwise
from typing import Any

import pytest
from aio_pika.abc import AbstractIncomingMessage
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer.app import create_app
from app.enums import PaymentStatus
from app.messaging.events import PaymentCreatedEvent
from app.messaging.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_ROUTING_KEY,
)
from app.outbox.relay import OutboxRelay
from app.replay_dlq import replay_dead_letters
from app.services.webhooks import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign
from tests.conftest import WEBHOOK_SECRET
from tests.helpers import (
    SpyGateway,
    WebhookEndpoint,
    create_payment,
    eventually,
    get_payment,
    queue_depth,
    take_messages,
)

RETRY_BASE_DELAY = 0.2  # see the `settings` fixture


@pytest.fixture
def gateway() -> SpyGateway:
    return SpyGateway()


@pytest.fixture
def webhook() -> WebhookEndpoint:
    return WebhookEndpoint()


@pytest.fixture
async def consumer(
    broker_settings: Settings,
    broker: RabbitBroker,
    session_factory: async_sessionmaker[AsyncSession],
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> AsyncIterator[FastStream]:
    app = create_app(broker_settings, gateway=gateway, webhook_transport=webhook.transport)
    await app.start()
    yield app
    await app.stop()


async def publish_event(broker: RabbitBroker, body: Any, **kwargs: Any) -> None:
    await broker.publish(
        body,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        message_id=str(uuid.uuid4()),
        persist=True,
        **kwargs,
    )


async def publish_created(broker: RabbitBroker, payment_id: uuid.UUID) -> None:
    await publish_event(broker, PaymentCreatedEvent(payment_id=payment_id).model_dump(mode="json"))


async def wait_for_dead_letter(broker: RabbitBroker) -> AbstractIncomingMessage:
    dead: list[AbstractIncomingMessage] = []

    async def arrived() -> bool:
        dead.extend(await take_messages(broker, PAYMENTS_DLQ))
        return bool(dead)

    await eventually(arrived)
    [message] = dead
    return message


def webhook_delivered(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Any:
    async def check() -> bool:
        return (await get_payment(session_factory, payment_id)).webhook_delivered_at is not None

    return check


@pytest.mark.usefixtures("consumer")
async def test_payment_is_charged_and_merchant_notified(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> None:
    payment = await create_payment(session_factory)

    await publish_created(broker, payment.id)
    await eventually(webhook_delivered(session_factory, payment.id))

    stored = await get_payment(session_factory, payment.id)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert gateway.charged == [payment.id]

    [request] = webhook.requests
    [notification] = webhook.payloads
    assert notification["event"] == "payment.succeeded"
    assert notification["payment"]["payment_id"] == str(payment.id)
    assert notification["payment"]["status"] == "succeeded"
    assert notification["payment"]["processed_at"] is not None
    timestamp = request.headers[TIMESTAMP_HEADER]
    assert request.headers[SIGNATURE_HEADER] == (
        f"sha256={sign(WEBHOOK_SECRET.encode(), timestamp, request.content)}"
    )
    assert await take_messages(broker, PAYMENTS_DLQ) == []


@pytest.mark.usefixtures("consumer")
async def test_declined_payment_is_marked_failed(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> None:
    gateway.succeeded = False
    payment = await create_payment(session_factory)

    await publish_created(broker, payment.id)
    await eventually(webhook_delivered(session_factory, payment.id))

    assert (await get_payment(session_factory, payment.id)).status == PaymentStatus.FAILED
    assert webhook.payloads[0]["event"] == "payment.failed"


@pytest.mark.usefixtures("consumer")
async def test_outbox_event_reaches_the_consumer(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    webhook: WebhookEndpoint,
) -> None:
    payment = await create_payment(session_factory)

    relay = OutboxRelay(session_factory, broker, batch_size=10, poll_interval=0.05)
    assert await relay.relay_batch() == 1
    await eventually(webhook_delivered(session_factory, payment.id))

    assert webhook.payloads[0]["payment"]["payment_id"] == str(payment.id)


@pytest.mark.usefixtures("consumer")
async def test_failed_webhook_is_retried_with_exponential_backoff(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> None:
    webhook.statuses = [503, 503, 200]
    payment = await create_payment(session_factory)

    await publish_created(broker, payment.id)
    await eventually(webhook_delivered(session_factory, payment.id))

    assert len(webhook.requests) == 3
    first_pause, second_pause = (b - a for a, b in pairwise(webhook.received_at))
    assert first_pause >= RETRY_BASE_DELAY * 0.9
    assert second_pause >= RETRY_BASE_DELAY * 2 * 0.9
    # Retries only repeat the notification: the payment was charged once.
    assert gateway.charged == [payment.id]
    assert len({p["payment"]["processed_at"] for p in webhook.payloads}) == 1
    assert await take_messages(broker, PAYMENTS_DLQ) == []


@pytest.mark.usefixtures("consumer")
async def test_message_is_dead_lettered_after_the_last_attempt(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> None:
    webhook.statuses = [500]
    payment = await create_payment(session_factory)

    await publish_created(broker, payment.id)
    dead = await wait_for_dead_letter(broker)

    assert json.loads(dead.body) == {"payment_id": str(payment.id)}
    assert dead.headers["x-retry-count"] == 2
    assert "HTTP 500" in str(dead.headers["x-last-error"])
    assert len(webhook.requests) == 3
    stored = await get_payment(session_factory, payment.id)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None
    assert gateway.charged == [payment.id]
    assert await take_messages(broker, PAYMENTS_NEW_QUEUE) == []


@pytest.mark.usefixtures("consumer")
async def test_permanent_webhook_error_is_dead_lettered_without_retries(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    webhook: WebhookEndpoint,
) -> None:
    webhook.statuses = [400]
    payment = await create_payment(session_factory)

    await publish_created(broker, payment.id)
    dead = await wait_for_dead_letter(broker)

    assert len(webhook.requests) == 1
    assert "x-retry-count" not in dead.headers


@pytest.mark.usefixtures("consumer")
async def test_event_of_unknown_payment_is_dead_lettered(
    broker: RabbitBroker, gateway: SpyGateway, webhook: WebhookEndpoint
) -> None:
    await publish_created(broker, uuid.uuid4())
    await wait_for_dead_letter(broker)

    assert gateway.charged == []
    assert webhook.requests == []


@pytest.mark.parametrize(
    ("body", "content_type"),
    [({"unexpected": "shape"}, None), (b"{not json", "application/json")],
)
@pytest.mark.usefixtures("consumer")
async def test_malformed_message_is_dead_lettered(
    broker: RabbitBroker, gateway: SpyGateway, body: Any, content_type: str | None
) -> None:
    await publish_event(broker, body, content_type=content_type)
    await wait_for_dead_letter(broker)

    assert gateway.charged == []


@pytest.mark.usefixtures("consumer")
async def test_redelivered_event_neither_charges_nor_notifies_again(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="app")
    payment = await create_payment(session_factory)
    await publish_created(broker, payment.id)
    await eventually(webhook_delivered(session_factory, payment.id))

    # E.g. the relay crashed after publishing but before marking the event as published.
    await publish_created(broker, payment.id)

    async def duplicate_handled() -> bool:
        return "was already delivered" in caplog.text

    await eventually(duplicate_handled)
    assert gateway.charged == [payment.id]
    assert len(webhook.requests) == 1


@pytest.mark.usefixtures("consumer")
async def test_concurrent_duplicates_charge_the_payment_once(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> None:
    original_charge = gateway.charge

    async def slow_charge(payment: Any) -> Any:
        await asyncio.sleep(0.3)  # keep the row locked while the duplicate arrives
        return await original_charge(payment)

    gateway.charge = slow_charge  # type: ignore[method-assign]
    payment = await create_payment(session_factory)

    await asyncio.gather(publish_created(broker, payment.id), publish_created(broker, payment.id))
    await eventually(webhook_delivered(session_factory, payment.id))
    await asyncio.sleep(0.5)  # let the duplicate finish

    assert gateway.charged == [payment.id]
    assert await take_messages(broker, PAYMENTS_DLQ) == []


@pytest.mark.usefixtures("consumer")
async def test_dead_letter_is_processed_after_replay(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    gateway: SpyGateway,
    webhook: WebhookEndpoint,
) -> None:
    webhook.statuses = [500, 500, 500, 200]  # down for all attempts, then fixed
    payment = await create_payment(session_factory)
    await publish_created(broker, payment.id)

    async def dead_lettered() -> bool:
        return await queue_depth(broker, PAYMENTS_DLQ) == 1

    await eventually(dead_lettered)

    assert await replay_dead_letters(broker) == 1
    await eventually(webhook_delivered(session_factory, payment.id))

    assert len(webhook.requests) == 4
    assert gateway.charged == [payment.id]
    assert await queue_depth(broker, PAYMENTS_DLQ) == 0
