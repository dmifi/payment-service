import asyncio
import json
import uuid
from typing import Any, cast

import pytest
from aio_pika import DeliveryMode
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.models import OutboxEvent
from app.messaging.topology import PAYMENTS_NEW_QUEUE
from app.outbox.app import create_app
from app.outbox.relay import OutboxPublishError, OutboxRelay
from tests.helpers import create_payment, eventually, take_messages


def make_relay(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker, batch_size: int = 100
) -> OutboxRelay:
    return OutboxRelay(session_factory, broker, batch_size=batch_size, poll_interval=0.05)


async def outbox_events(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


async def test_relay_publishes_each_pending_event_once(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    payments = [await create_payment(session_factory) for _ in range(3)]
    relay = make_relay(session_factory, broker)

    assert await relay.relay_batch() == 3
    assert await relay.relay_batch() == 0

    events = {event.aggregate_id: event for event in await outbox_events(session_factory)}
    assert all(event.published_at is not None for event in events.values())

    messages = await take_messages(broker, PAYMENTS_NEW_QUEUE)
    assert sorted(json.loads(m.body)["payment_id"] for m in messages) == sorted(
        str(p.id) for p in payments
    )
    for message in messages:
        payment_id = uuid.UUID(json.loads(message.body)["payment_id"])
        assert message.message_id == str(events[payment_id].id)
        assert message.correlation_id == str(payment_id)
        assert message.type == "payment.created"
        assert message.content_type == "application/json"
        assert message.delivery_mode == DeliveryMode.PERSISTENT


async def test_events_stay_in_outbox_while_broker_is_unavailable(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class UnavailableBroker:
        async def publish(self, *_args: Any, **_kwargs: Any) -> None:
            raise ConnectionError("broker is down")

    await create_payment(session_factory)
    relay = make_relay(session_factory, cast(RabbitBroker, UnavailableBroker()))

    with pytest.raises(OutboxPublishError):
        await relay.relay_batch()

    [event] = await outbox_events(session_factory)
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert event.last_error is not None
    assert "broker is down" in event.last_error


async def test_unroutable_event_is_not_considered_published(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    # Publisher confirms alone would acknowledge a message no queue receives;
    # mandatory publishing turns it into an error, so the event is kept.
    async with session_factory() as session, session.begin():
        session.add(
            OutboxEvent(
                aggregate_id=uuid.uuid4(),
                event_type="test.event",
                routing_key="no.queue.bound",
                payload={},
            )
        )

    with pytest.raises(OutboxPublishError):
        await make_relay(session_factory, broker).relay_batch()

    [event] = await outbox_events(session_factory)
    assert event.published_at is None


async def test_parallel_relays_publish_every_event_exactly_once(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    payments = [await create_payment(session_factory) for _ in range(30)]
    relays = [make_relay(session_factory, broker, batch_size=4) for _ in range(3)]

    async def drain(relay: OutboxRelay) -> None:
        while await relay.relay_batch():
            pass

    await asyncio.gather(*map(drain, relays))

    published = [
        json.loads(m.body)["payment_id"] for m in await take_messages(broker, PAYMENTS_NEW_QUEUE)
    ]
    assert sorted(published) == sorted(str(p.id) for p in payments)
    assert all(event.published_at is not None for event in await outbox_events(session_factory))


async def test_relay_service_publishes_new_events_in_background(
    broker_settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
) -> None:
    app = create_app(broker_settings)
    await app.start()
    try:
        payment = await create_payment(session_factory)

        async def published() -> bool:
            return all(e.published_at for e in await outbox_events(session_factory))

        await eventually(published)
    finally:
        await app.stop()

    [message] = await take_messages(broker, PAYMENTS_NEW_QUEUE)
    assert json.loads(message.body) == {"payment_id": str(payment.id)}
