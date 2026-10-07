import logging
from collections.abc import Mapping
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitMessage

from app.exceptions import PaymentNotFoundError, WebhookDeliveryError
from app.messaging.events import PaymentCreatedEvent
from app.messaging.retry import RetryPolicy
from app.messaging.topology import RETRY_EXCHANGE, retry_queue
from app.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"
LAST_ERROR_HEADER = "x-last-error"
_BROKER_HEADER_PREFIXES = ("x-death", "x-first-death-", "x-last-death-")


def is_retryable(exc: Exception) -> bool:
    """Whether another attempt can succeed; unknown errors are treated as transient."""
    if isinstance(exc, WebhookDeliveryError):
        return exc.retryable
    return not isinstance(exc, PaymentNotFoundError)


def without_broker_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    """Message headers without those RabbitMQ maintains itself when dead-lettering."""
    return {
        name: value
        for name, value in headers.items()
        if not name.startswith(_BROKER_HEADER_PREFIXES)
    }


def retry_count(message: RabbitMessage) -> int:
    try:
        return max(int(message.headers.get(RETRY_COUNT_HEADER, 0)), 0)
    except (TypeError, ValueError):
        return 0


class PaymentCreatedHandler:
    """Handles `payments.new`: processes the payment and decides the fate of the message.

    * success -> ack;
    * transient error with attempts left -> republish into the delay queue of this attempt
      (exponential backoff) and ack the original;
    * permanent error or failed last attempt -> reject: RabbitMQ dead-letters the message
      into `payments.new.dlq`.
    """

    def __init__(
        self, processor: PaymentProcessor, broker: RabbitBroker, retry_policy: RetryPolicy
    ) -> None:
        self._processor = processor
        self._broker = broker
        self._retry_policy = retry_policy

    async def handle(self, event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        attempt = retry_count(message) + 1
        try:
            await self._processor.process(event.payment_id)
        except Exception as exc:
            await self._on_failure(event, message, attempt, exc)

    async def _on_failure(
        self, event: PaymentCreatedEvent, message: RabbitMessage, attempt: int, exc: Exception
    ) -> None:
        retryable = is_retryable(exc)
        if retryable and self._retry_policy.can_retry(attempt):
            delay = self._retry_policy.delay_after(attempt)
            logger.warning(
                "Payment %s: attempt %d/%d failed (%s), retrying in %.1fs",
                event.payment_id,
                attempt,
                self._retry_policy.max_attempts,
                exc,
                delay,
            )
            # Publish before ack: a crash in between duplicates the message but never loses it.
            # If publishing fails, the error propagates and FastStream rejects the message,
            # so it is parked in the DLQ rather than dropped.
            await self._schedule_retry(message, attempt=attempt, delay=delay, error=exc)
            await message.ack()
            return

        reason = f"all {attempt} attempts failed" if retryable else "permanent error"
        logger.error(
            "Payment %s: %s, moving message to DLQ", event.payment_id, reason, exc_info=exc
        )
        await message.reject(requeue=False)

    async def _schedule_retry(
        self, message: RabbitMessage, *, attempt: int, delay: float, error: Exception
    ) -> None:
        headers = without_broker_headers(message.headers)
        headers[RETRY_COUNT_HEADER] = attempt
        headers[LAST_ERROR_HEADER] = f"{type(error).__name__}: {error}"[:1000]

        await self._broker.publish(
            message.body,
            exchange=RETRY_EXCHANGE,
            routing_key=retry_queue(delay).routing(),
            headers=headers,
            content_type=message.content_type,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            message_type=message.raw_message.type,
            persist=True,
        )
