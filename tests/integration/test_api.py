import asyncio
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.base import Base
from app.db.models import OutboxEvent, Payment
from app.messaging.events import PAYMENT_CREATED
from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.repositories.outbox import OutboxRepository

PAYMENTS_URL = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #1042",
    "metadata": {"order_id": 1042, "customer": {"id": "c-17"}},
    "webhook_url": "https://merchant.example/webhooks",
}


def with_key(key: str | None = None) -> dict[str, str]:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


async def count(session_factory: async_sessionmaker[AsyncSession], model: type[Base]) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model)) or 0


async def test_create_payment_accepts_it_and_records_outbox_event(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await api_client.post(PAYMENTS_URL, json=BODY, headers=with_key("order-1042"))

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    payment_id = uuid.UUID(data["payment_id"])
    assert response.headers["Location"] == f"http://test{PAYMENTS_URL}/{payment_id}"
    assert "Idempotent-Replayed" not in response.headers

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        events = (await session.scalars(select(OutboxEvent))).all()
    assert payment is not None
    assert payment.idempotency_key == "order-1042"
    assert payment.metadata_ == BODY["metadata"]
    assert payment.processed_at is None
    [event] = events
    assert event.aggregate_id == payment_id
    assert event.event_type == PAYMENT_CREATED
    assert event.routing_key == PAYMENTS_NEW_ROUTING_KEY
    assert event.payload == {"payment_id": str(payment_id)}
    assert event.published_at is None


async def test_payment_is_not_stored_without_its_outbox_event(
    api_client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_add(self: OutboxRepository, event: OutboxEvent) -> None:
        raise RuntimeError("outbox write failed")

    monkeypatch.setattr(OutboxRepository, "add", broken_add)

    with pytest.raises(RuntimeError, match="outbox write failed"):
        await api_client.post(PAYMENTS_URL, json=BODY, headers=with_key())

    assert await count(session_factory, Payment) == 0


async def test_repeated_request_returns_the_same_payment(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = with_key()

    first = await api_client.post(PAYMENTS_URL, json=BODY, headers=headers)
    second = await api_client.post(PAYMENTS_URL, json=BODY, headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_equivalent_body_counts_as_the_same_request(api_client: httpx.AsyncClient) -> None:
    headers = with_key()
    reordered_metadata = dict(reversed(list(BODY["metadata"].items())))

    first = await api_client.post(PAYMENTS_URL, json={**BODY, "amount": 1500}, headers=headers)
    second = await api_client.post(
        PAYMENTS_URL,
        json={**BODY, "amount": "1500.0", "metadata": reordered_metadata},
        headers=headers,
    )

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_reusing_key_for_another_payment_is_rejected(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = with_key()
    await api_client.post(PAYMENTS_URL, json=BODY, headers=headers)

    response = await api_client.post(PAYMENTS_URL, json={**BODY, "amount": "1.00"}, headers=headers)

    assert response.status_code == 422
    assert response.json() == {
        "detail": "Idempotency-Key has already been used with a different request payload"
    }
    assert await count(session_factory, Payment) == 1


async def test_concurrent_requests_with_one_key_create_a_single_payment(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = with_key()

    responses = await asyncio.gather(
        *(api_client.post(PAYMENTS_URL, json=BODY, headers=headers) for _ in range(10))
    )

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert sum("Idempotent-Replayed" not in response.headers for response in responses) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_different_keys_create_different_payments(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await api_client.post(PAYMENTS_URL, json=BODY, headers=with_key())
    second = await api_client.post(PAYMENTS_URL, json=BODY, headers=with_key())

    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await count(session_factory, OutboxEvent) == 2


async def test_idempotency_key_header_is_required(api_client: httpx.AsyncClient) -> None:
    response = await api_client.post(PAYMENTS_URL, json=BODY)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


@pytest.mark.parametrize("key", ["", "k" * 256])
async def test_idempotency_key_length_is_limited(api_client: httpx.AsyncClient, key: str) -> None:
    response = await api_client.post(PAYMENTS_URL, json=BODY, headers={"Idempotency-Key": key})

    assert response.status_code == 422


async def test_invalid_body_is_rejected_and_nothing_is_stored(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await api_client.post(
        PAYMENTS_URL, json={**BODY, "amount": "-5", "currency": "XXX"}, headers=with_key()
    )

    assert response.status_code == 422
    assert {error["loc"][-1] for error in response.json()["detail"]} == {"amount", "currency"}
    assert await count(session_factory, Payment) == 0


async def test_get_payment_returns_its_details(api_client: httpx.AsyncClient) -> None:
    created = (await api_client.post(PAYMENTS_URL, json=BODY, headers=with_key("k-1"))).json()

    response = await api_client.get(f"{PAYMENTS_URL}/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": BODY["metadata"],
        "status": "pending",
        "idempotency_key": "k-1",
        "webhook_url": "https://merchant.example/webhooks",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_unknown_payment_is_not_found(api_client: httpx.AsyncClient) -> None:
    payment_id = uuid.uuid4()

    response = await api_client.get(f"{PAYMENTS_URL}/{payment_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": f"Payment {payment_id} not found"}


async def test_malformed_payment_id_is_rejected(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get(f"{PAYMENTS_URL}/not-a-uuid")

    assert response.status_code == 422


@pytest.mark.parametrize("api_key", [None, "", "wrong-key"])
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", PAYMENTS_URL), ("GET", f"{PAYMENTS_URL}/{uuid.uuid4()}")],
)
async def test_endpoints_require_valid_api_key(
    api_client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    method: str,
    path: str,
    api_key: str | None,
) -> None:
    del api_client.headers["X-API-Key"]
    headers = with_key() if api_key is None else {**with_key(), "X-API-Key": api_key}

    response = await api_client.request(method, path, json=BODY, headers=headers)

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "APIKey"
    assert await count(session_factory, Payment) == 0


async def test_health_probes_are_public(api_client: httpx.AsyncClient) -> None:
    del api_client.headers["X-API-Key"]

    assert (await api_client.get("/health/live")).json() == {"status": "ok"}
    assert (await api_client.get("/health/ready")).json() == {"status": "ok"}
