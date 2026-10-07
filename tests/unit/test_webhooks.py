import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.db.models import Payment
from app.enums import Currency, PaymentStatus
from app.exceptions import WebhookDeliveryError
from app.schemas import PaymentWebhook
from app.services.webhooks import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookNotifier,
    is_retryable_status,
    sign,
)

SECRET = "s3cret"


def make_payment(status: PaymentStatus = PaymentStatus.SUCCEEDED) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        idempotency_key="key-1",
        request_fingerprint="0" * 64,
        amount=Decimal("10.50"),
        currency=Currency.USD,
        description="Order",
        metadata_={"order_id": 7},
        status=status,
        webhook_url="https://merchant.example/webhooks",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        processed_at=datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC),
    )


def notifier_with(handler: httpx.MockTransport, secret: str | None = SECRET) -> WebhookNotifier:
    return WebhookNotifier(httpx.AsyncClient(transport=handler), secret=secret)


async def test_sends_signed_notification() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    payment = make_payment()
    await notifier_with(httpx.MockTransport(handler)).notify(payment)

    [request] = requests
    assert request.method == "POST"
    assert str(request.url) == payment.webhook_url
    assert request.headers["Content-Type"] == "application/json"
    body = json.loads(request.content)
    assert body["event"] == "payment.succeeded"
    assert body["payment"]["payment_id"] == str(payment.id)
    assert body["payment"]["amount"] == "10.50"
    assert body["payment"]["metadata"] == {"order_id": 7}

    timestamp = request.headers[TIMESTAMP_HEADER]
    expected = sign(SECRET.encode(), timestamp, request.content)
    assert request.headers[SIGNATURE_HEADER] == f"sha256={expected}"


async def test_no_signature_without_secret() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    await notifier_with(httpx.MockTransport(handler), secret=None).notify(make_payment())

    assert SIGNATURE_HEADER not in requests[0].headers


async def test_failed_payment_is_reported_as_such() -> None:
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(200)

    await notifier_with(httpx.MockTransport(handler)).notify(make_payment(PaymentStatus.FAILED))

    assert json.loads(bodies[0])["event"] == "payment.failed"


@pytest.mark.parametrize(
    ("status_code", "retryable"),
    [
        (500, True),
        (502, True),
        (503, True),
        (429, True),
        (408, True),
        (400, False),
        (401, False),
        (404, False),
        (410, False),
        (301, False),
    ],
)
async def test_error_responses_are_classified(status_code: int, retryable: bool) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status_code))

    with pytest.raises(WebhookDeliveryError) as error:
        await notifier_with(transport).notify(make_payment())

    assert error.value.retryable is retryable
    assert is_retryable_status(status_code) is retryable


@pytest.mark.parametrize(
    "exception",
    [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), httpx.RemoteProtocolError("bad")],
)
async def test_network_errors_are_retryable(exception: httpx.TransportError) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exception

    with pytest.raises(WebhookDeliveryError) as error:
        await notifier_with(httpx.MockTransport(handler)).notify(make_payment())

    assert error.value.retryable


def test_pending_payment_has_no_webhook() -> None:
    with pytest.raises(ValueError, match="not processed"):
        PaymentWebhook.from_model(make_payment(PaymentStatus.PENDING))
