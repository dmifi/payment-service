import asyncio
import contextlib
import logging
import uuid

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent
from app.messaging.topology import PAYMENTS_EXCHANGE
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

PUBLISH_TIMEOUT = 10.0  # seconds to wait for the broker's publisher confirm
MAX_BACKOFF = 10.0  # seconds between iterations while publishing keeps failing


class OutboxPublishError(Exception):
    pass


class OutboxRelay:
    """Publishes events from the outbox table to RabbitMQ with at-least-once guarantee.

    Each iteration locks a batch of unpublished events (`FOR UPDATE SKIP LOCKED`, so relay
    instances can be scaled out), publishes them waiting for publisher confirms and marks
    them published in the same transaction. A crash between the publish and the commit
    publishes the event again later, which is why the consumer is idempotent.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        *,
        batch_size: int,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="outbox-relay")

    async def stop(self) -> None:
        """Finish the current batch and stop."""
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def relay_batch(self) -> int:
        """Publish one batch of pending events and return the number of published ones."""
        published: list[uuid.UUID] = []
        error: Exception | None = None

        async with self._session_factory() as session, session.begin():
            outbox = OutboxRepository(session)
            for event in await outbox.lock_unpublished(self._batch_size):
                try:
                    await self._publish(event)
                except Exception as exc:
                    # The broker is most likely unavailable: the rest of the batch waits too.
                    await outbox.record_failure(event.id, repr(exc))
                    error = exc
                    break
                published.append(event.id)
            await outbox.mark_published(published)

        if published:
            logger.info("Published %d outbox event(s)", len(published))
        if error is not None:
            raise OutboxPublishError(f"Failed to publish an outbox event: {error!r}") from error
        return len(published)

    async def _publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=event.routing_key,
            message_id=str(event.id),
            correlation_id=str(event.aggregate_id),
            message_type=event.event_type,
            timestamp=event.created_at,
            persist=True,
            mandatory=True,
            timeout=PUBLISH_TIMEOUT,
        )

    async def _run(self) -> None:
        logger.info("Outbox relay started")
        failures = 0
        while not self._stopping.is_set():
            try:
                published = await self.relay_batch()
            except Exception:
                failures += 1
                logger.exception("Outbox relay iteration failed (%d in a row)", failures)
                published = 0
            else:
                failures = 0

            # A full batch means there is a backlog: continue without pausing.
            if published < self._batch_size:
                await self._sleep(self._next_delay(failures))
        logger.info("Outbox relay stopped")

    def _next_delay(self, failures: int) -> float:
        if failures == 0:
            return self._poll_interval
        return min(self._poll_interval * 2.0**failures, MAX_BACKOFF)

    async def _sleep(self, seconds: float) -> None:
        """Sleep interrupted by `stop()`."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
