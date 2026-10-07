"""End-to-end tests against the running docker compose stack.

docker compose up -d --build --wait
uv run pytest -m e2e
"""

import os
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

API_URL = os.environ.get("E2E_API_URL", "http://localhost:8000")
API_KEY = os.environ.get("E2E_API_KEY", "dev-api-key")
RECEIVER_URL = os.environ.get("E2E_RECEIVER_URL", "http://localhost:9000")
# The webhook receiver as the consumer container sees it.
RECEIVER_INTERNAL_URL = os.environ.get("E2E_RECEIVER_INTERNAL_URL", "http://webhook-receiver:9000")
RABBITMQ_API_URL = os.environ.get("E2E_RABBITMQ_API_URL", "http://localhost:15672/api")
RABBITMQ_CREDENTIALS = ("payments", "payments")


def wait_for[T](probe: Callable[[], T | None], *, within: float = 40.0) -> T:
    """Poll until `probe` returns a truthy value and return it."""
    deadline = time.monotonic() + within
    while not (result := probe()):
        if time.monotonic() > deadline:
            raise AssertionError(f"Condition not met within {within}s")
        time.sleep(0.5)
    return result


@pytest.fixture(scope="module")
def api() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=API_URL, headers={"X-API-Key": API_KEY}, timeout=10) as client:
        yield client


@pytest.fixture(scope="module")
def receiver() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=RECEIVER_URL, timeout=10) as client:
        yield client


@pytest.fixture(scope="module")
def rabbitmq() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=RABBITMQ_API_URL, auth=RABBITMQ_CREDENTIALS, timeout=10) as client:
        yield client


def create_payment(api: httpx.Client, webhook_query: str = "") -> str:
    response = api.post(
        "/api/v1/payments",
        json={
            "amount": "250.00",
            "currency": "EUR",
            "description": "e2e",
            "metadata": {"test": "e2e"},
            "webhook_url": f"{RECEIVER_INTERNAL_URL}/webhooks{webhook_query}",
        },
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 202, response.text
    return str(response.json()["payment_id"])


def deliveries(receiver: httpx.Client, payment_id: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = receiver.get(
        "/webhooks", params={"payment_id": payment_id}
    ).json()
    return result


def dead_letters(rabbitmq: httpx.Client) -> list[dict[str, Any]]:
    """Peek at the DLQ without consuming its messages."""
    response = rabbitmq.post(
        "/queues/%2F/payments.new.dlq/get",
        json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
    )
    response.raise_for_status()
    messages: list[dict[str, Any]] = response.json()
    return messages


def test_payment_is_processed_and_merchant_notified(
    api: httpx.Client, receiver: httpx.Client
) -> None:
    payment_id = create_payment(api)

    def processed() -> dict[str, Any] | None:
        payment: dict[str, Any] = api.get(f"/api/v1/payments/{payment_id}").json()
        return payment if payment["status"] != "pending" else None

    payment = wait_for(processed)
    assert payment["status"] in {"succeeded", "failed"}
    assert payment["processed_at"] is not None

    [delivery] = wait_for(lambda: deliveries(receiver, payment_id))
    assert delivery["payload"]["event"] == f"payment.{payment['status']}"
    assert delivery["payload"]["payment"]["payment_id"] == payment_id
    assert delivery["signature"] == "valid"


def test_repeated_request_returns_the_same_payment(api: httpx.Client) -> None:
    body = {
        "amount": "10.00",
        "currency": "USD",
        "description": "e2e idempotency",
        "webhook_url": f"{RECEIVER_INTERNAL_URL}/webhooks",
    }
    headers = {"Idempotency-Key": str(uuid.uuid4())}

    first = api.post("/api/v1/payments", json=body, headers=headers)
    second = api.post("/api/v1/payments", json=body, headers=headers)

    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert second.headers["Idempotent-Replayed"] == "true"


def test_flaky_webhook_is_delivered_on_a_retry(api: httpx.Client, receiver: httpx.Client) -> None:
    payment_id = create_payment(api, "?fail=2")

    def delivered() -> list[dict[str, Any]] | None:
        attempts = deliveries(receiver, payment_id)
        return attempts if attempts and attempts[-1]["response_status"] == 200 else None

    attempts = wait_for(delivered)
    assert [attempt["response_status"] for attempt in attempts] == [500, 500, 200]


def test_undeliverable_webhook_ends_up_in_dead_letter_queue(
    api: httpx.Client, receiver: httpx.Client, rabbitmq: httpx.Client
) -> None:
    payment_id = create_payment(api, "?status=500")

    def dead_letter() -> dict[str, Any] | None:
        return next(
            (m for m in dead_letters(rabbitmq) if payment_id in m["payload"]),
            None,
        )

    message = wait_for(dead_letter)
    assert message["properties"]["headers"]["x-retry-count"] == 2
    assert len(deliveries(receiver, payment_id)) == 3
