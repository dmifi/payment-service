import hashlib
import hmac
import time
from http import HTTPStatus

import httpx

from app.db.models import Payment
from app.exceptions import WebhookDeliveryError
from app.schemas import PaymentWebhook

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"

# Status codes that mean "try again later"; any other 4xx/3xx is a permanent rejection.
_RETRYABLE_STATUS_CODES = frozenset({408, 425, 429})


def is_retryable_status(status_code: int) -> bool:
    return status_code >= HTTPStatus.INTERNAL_SERVER_ERROR or status_code in _RETRYABLE_STATUS_CODES


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    """HMAC-SHA256 over `<timestamp>.<body>`; the timestamp lets receivers reject replays."""
    return hmac.new(secret, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()


class WebhookNotifier:
    """Notifies the merchant about the result of a payment.

    A single attempt per call: retries are scheduled by the consumer through RabbitMQ,
    so a slow or failing endpoint never blocks the consumer while it waits for a retry.
    """

    def __init__(self, client: httpx.AsyncClient, *, secret: str | None = None) -> None:
        self._client = client
        self._secret = secret.encode() if secret else None

    async def notify(self, payment: Payment) -> None:
        body = PaymentWebhook.from_model(payment).model_dump_json().encode()
        headers = {"Content-Type": "application/json", **self._signature_headers(body)}
        try:
            response = await self._client.post(payment.webhook_url, content=body, headers=headers)
        except httpx.TransportError as exc:  # connection errors and timeouts
            raise WebhookDeliveryError(f"Webhook request failed: {exc!r}", retryable=True) from exc

        if not response.is_success:
            raise WebhookDeliveryError(
                f"Webhook endpoint responded with HTTP {response.status_code}",
                retryable=is_retryable_status(response.status_code),
            )

    def _signature_headers(self, body: bytes) -> dict[str, str]:
        if self._secret is None:
            return {}
        timestamp = str(int(time.time()))
        return {
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: f"sha256={sign(self._secret, timestamp, body)}",
        }
