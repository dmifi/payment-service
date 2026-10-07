from faststream import FastStream

from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.messaging.broker import create_broker
from app.messaging.retry import RetryPolicy
from app.messaging.topology import declare_topology
from app.outbox.relay import OutboxRelay


def create_app(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    engine = create_engine(settings)
    broker = create_broker(settings)
    relay = OutboxRelay(
        create_session_factory(engine),
        broker,
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
    )

    async def declare_queues() -> None:
        # The queues must exist before the first publish, even if no consumer is running yet.
        await broker.connect()
        await declare_topology(broker, RetryPolicy.from_settings(settings))

    async def start_relay() -> None:
        await relay.start()

    async def stop_relay() -> None:
        await relay.stop()

    async def close_resources() -> None:
        await engine.dispose()

    return FastStream(
        broker,
        on_startup=[declare_queues],
        after_startup=[start_relay],
        on_shutdown=[stop_relay],
        after_shutdown=[close_resources],
    )
