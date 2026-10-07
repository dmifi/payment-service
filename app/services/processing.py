import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Payment
from app.enums import PaymentStatus
from app.exceptions import PaymentNotFoundError
from app.repositories.payments import PaymentRepository
from app.services.gateway import PaymentGateway
from app.services.webhooks import WebhookNotifier

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Consumer business logic: charge a payment exactly once, then notify the merchant.

    Safe to run any number of times for the same payment (messages are delivered at least
    once, and failed attempts are retried): every step checks what is already done.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        notifier: WebhookNotifier,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._charge(payment_id)

        if payment.webhook_delivered_at is not None:
            logger.info("Webhook for payment %s was already delivered", payment_id)
            return

        await self._notifier.notify(payment)
        async with self._session_factory() as session, session.begin():
            await PaymentRepository(session).mark_webhook_delivered(payment_id)
        logger.info("Webhook for payment %s delivered to %s", payment_id, payment.webhook_url)

    async def _charge(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session, session.begin():
            # The row lock serializes concurrent deliveries of the same event: a duplicate
            # waits here and then finds the final status instead of charging a second time.
            payment = await PaymentRepository(session).get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)

            if payment.status != PaymentStatus.PENDING:
                logger.info(
                    "Payment %s is already %s, not charging again", payment_id, payment.status
                )
                return payment

            result = await self._gateway.charge(payment)
            payment.status = PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED
            payment.processed_at = datetime.now(UTC)

        logger.info("Payment %s %s", payment_id, payment.status)
        return payment
