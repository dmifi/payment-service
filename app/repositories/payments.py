import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.enums import Currency


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_if_absent(
        self,
        *,
        idempotency_key: str,
        request_fingerprint: str,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
    ) -> Payment | None:
        """Insert a payment unless one with the same idempotency key exists (then return None).

        `ON CONFLICT DO NOTHING` makes concurrent requests with the same key safe without
        exception-driven control flow: a competing transaction waits on the unique index
        until the first one commits and then inserts nothing.
        """
        stmt = (
            insert(Payment)
            .values(
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                amount=amount,
                currency=currency,
                description=description,
                metadata_=metadata,
                webhook_url=webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(stmt)

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        stmt = (
            select(Payment)
            .where(Payment.id == payment_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return await self._session.scalar(stmt)

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=func.now())
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)
