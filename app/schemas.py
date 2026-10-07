"""Public contract: HTTP API request/response bodies and webhook notifications."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, HttpUrl, StringConstraints

from app.db.models import Payment
from app.enums import Currency, PaymentStatus

_CENT = Decimal("0.01")

Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=18, decimal_places=2),
    # 100, 100.0 and "100.00" are the same amount (and the same idempotent request).
    AfterValidator(lambda value: value.quantize(_CENT)),
]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.00",
                    "currency": "RUB",
                    "description": "Order #1042",
                    "metadata": {"order_id": 1042, "customer_id": "c-17"},
                    "webhook_url": "http://webhook-receiver:9000/webhooks",
                }
            ]
        },
    )

    amount: Amount
    currency: Currency
    description: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1024)
    ]
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary JSON object stored with the payment"
    )
    webhook_url: HttpUrl = Field(description="Receives a POST with the result of processing")


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> Self:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentDetails(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class PaymentWebhook(BaseModel):
    """Body of the POST request sent to `webhook_url` once the payment is processed."""

    event: Literal["payment.succeeded", "payment.failed"]
    payment: PaymentDetails

    @classmethod
    def from_model(cls, payment: Payment) -> Self:
        if payment.status == PaymentStatus.SUCCEEDED:
            event: Literal["payment.succeeded", "payment.failed"] = "payment.succeeded"
        elif payment.status == PaymentStatus.FAILED:
            event: Literal["payment.succeeded", "payment.failed"] = "payment.failed"
        else:
            raise ValueError(f"Payment {payment.id} is not processed yet")
        return cls(event=event, payment=PaymentDetails.from_model(payment))


class ErrorResponse(BaseModel):
    detail: str
