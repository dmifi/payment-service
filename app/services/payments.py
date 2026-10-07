import hashlib
import json
import uuid
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Payment
from app.exceptions import IdempotencyKeyReusedError, PaymentNotFoundError
from app.messaging.events import PAYMENT_CREATED, PaymentCreatedEvent
from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentCreate


@dataclass(frozen=True, slots=True)
class PaymentCreation:
    payment: Payment
    created: bool
    """False when the request repeats an earlier one with the same Idempotency-Key."""


class PaymentService:
    """Use cases of the HTTP API."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(self, data: PaymentCreate, idempotency_key: str) -> PaymentCreation:
        fingerprint = request_fingerprint(data)
        async with self._session.begin():
            payment = await self._payments.create_if_absent(
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata=data.metadata,
                webhook_url=str(data.webhook_url),
            )
            if payment is not None:
                # Same transaction as the payment: the event exists if and only if the payment does.
                self._outbox.add(payment_created_event(payment))
                return PaymentCreation(payment=payment, created=True)

            existing = await self._payments.get_by_idempotency_key(idempotency_key)

        if existing is None:  # pragma: no cover - payments are never deleted
            raise RuntimeError(f"Payment with idempotency key {idempotency_key!r} vanished")
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyKeyReusedError(idempotency_key)
        return PaymentCreation(payment=existing, created=False)

    async def get_payment(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment


def request_fingerprint(data: PaymentCreate) -> str:
    """Stable hash of the validated (normalized) request body."""
    canonical = json.dumps(
        data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def payment_created_event(payment: Payment) -> OutboxEvent:
    return OutboxEvent(
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
    )
