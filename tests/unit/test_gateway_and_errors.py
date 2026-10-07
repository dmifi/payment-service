import random
import uuid
from types import SimpleNamespace
from typing import cast

import pytest
from faststream.rabbit import RabbitMessage

from app.consumer.handler import is_retryable, retry_count
from app.db.models import Payment
from app.exceptions import PaymentNotFoundError, WebhookDeliveryError
from app.services.gateway import EmulatedPaymentGateway


@pytest.mark.parametrize(("success_rate", "expected"), [(1.0, True), (0.0, False)])
async def test_gateway_honours_success_rate(success_rate: float, expected: bool) -> None:
    gateway = EmulatedPaymentGateway(min_delay=0, max_delay=0, success_rate=success_rate)
    payment = Payment(id=uuid.uuid4())

    results = [await gateway.charge(payment) for _ in range(20)]

    assert all(result.succeeded is expected for result in results)


async def test_gateway_success_rate_is_statistically_correct() -> None:
    gateway = EmulatedPaymentGateway(
        min_delay=0, max_delay=0, success_rate=0.9, rng=random.Random(42)
    )
    payment = Payment(id=uuid.uuid4())

    results = [await gateway.charge(payment) for _ in range(2000)]
    rate = sum(result.succeeded for result in results) / len(results)

    assert 0.87 < rate < 0.93


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (WebhookDeliveryError("HTTP 503", retryable=True), True),
        (WebhookDeliveryError("HTTP 400", retryable=False), False),
        (PaymentNotFoundError(uuid.uuid4()), False),
        (ConnectionError("database is down"), True),
        (RuntimeError("unexpected"), True),
    ],
)
def test_error_classification(error: Exception, retryable: bool) -> None:
    assert is_retryable(error) is retryable


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 0),
        ({"x-retry-count": 2}, 2),
        ({"x-retry-count": "1"}, 1),
        ({"x-retry-count": "junk"}, 0),
        ({"x-retry-count": -5}, 0),
    ],
)
def test_retry_count_header_parsing(headers: dict[str, object], expected: int) -> None:
    message = cast(RabbitMessage, SimpleNamespace(headers=headers))

    assert retry_count(message) == expected
