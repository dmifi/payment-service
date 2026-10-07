import uuid
from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        """Oldest unpublished events, row-locked until the end of the transaction.

        `SKIP LOCKED` lets several relay instances work in parallel without
        publishing the same event twice.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    async def mark_published(self, event_ids: Sequence[uuid.UUID]) -> None:
        if not event_ids:
            return
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(event_ids))
            .values(published_at=func.now())
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)

    async def record_failure(self, event_id: uuid.UUID, error: str) -> None:
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(publish_attempts=OutboxEvent.publish_attempts + 1, last_error=error)
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)
