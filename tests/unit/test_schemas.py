from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.schemas import PaymentCreate
from app.services.payments import request_fingerprint

VALID_BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1, "tags": ["a", "b"]},
    "webhook_url": "https://merchant.example/webhooks",
}


def body(**overrides: Any) -> dict[str, Any]:
    return {**VALID_BODY, **overrides}


@pytest.mark.parametrize("amount", [100, 100.0, "100", "100.0", "100.00", "1E+2"])
def test_amount_is_normalized_to_two_decimal_places(amount: Any) -> None:
    payment = PaymentCreate.model_validate(body(amount=amount))

    assert payment.amount == Decimal("100.00")
    assert str(payment.amount) == "100.00"


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": 0},
        {"amount": "-1.00"},
        {"amount": "10.001"},  # fractions of a cent
        {"amount": "NaN"},
        {"amount": "10000000000000000.00"},  # 17 integer digits do not fit NUMERIC(18, 2)
        {"currency": "GBP"},
        {"currency": "rub"},
        {"description": ""},
        {"description": "   "},
        {"description": "x" * 1025},
        {"webhook_url": "ftp://merchant.example/hook"},
        {"webhook_url": "not a url"},
        {"metadata": ["not", "an", "object"]},
        {"unexpected": "field"},
    ],
)
def test_invalid_request_is_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(**overrides))


def test_metadata_is_optional() -> None:
    data = {key: value for key, value in VALID_BODY.items() if key != "metadata"}

    assert PaymentCreate.model_validate(data).metadata == {}


def test_fingerprint_ignores_insignificant_differences() -> None:
    first = PaymentCreate.model_validate(body(amount=1500, metadata={"b": 2, "a": 1}))
    second = PaymentCreate.model_validate(body(amount="1500.00", metadata={"a": 1, "b": 2}))

    assert request_fingerprint(first) == request_fingerprint(second)


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "1500.01"},
        {"currency": "USD"},
        {"description": "Order #2"},
        {"metadata": {"order_id": 2}},
        {"webhook_url": "https://merchant.example/other"},
    ],
)
def test_fingerprint_changes_with_any_field(overrides: dict[str, Any]) -> None:
    original = PaymentCreate.model_validate(VALID_BODY)
    changed = PaymentCreate.model_validate(body(**overrides))

    assert request_fingerprint(original) != request_fingerprint(changed)
