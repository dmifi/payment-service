import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import CheckConstraint, Enum, Index, Numeric, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.enums import Currency, PaymentStatus


def _str_enum(enum_cls: type[StrEnum], *, name: str, length: int) -> Enum:
    """VARCHAR + CHECK constraint storing enum values: simpler to evolve than a native PG enum."""
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=length,
        values_callable=lambda members: [member.value for member in members],
        validate_strings=True,
    )


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    # SHA-256 of the canonical request body: tells a retry of the same request
    # from a reuse of the key for another payment.
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_str_enum(Currency, name="currency", length=3))
    description: Mapped[str] = mapped_column(String(1024))
    # `metadata` is reserved by SQLAlchemy's declarative API, hence the attribute name.
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[PaymentStatus] = mapped_column(
        _str_enum(PaymentStatus, name="payment_status", length=16),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    webhook_url: Mapped[str] = mapped_column(Text)
    # Set once the merchant acknowledged the notification: redeliveries must not repeat it.
    webhook_delivered_at: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    processed_at: Mapped[datetime | None]


class OutboxEvent(Base):
    """Transactional outbox: events are written in the same transaction as the state change
    and published to RabbitMQ asynchronously by the outbox relay."""

    __tablename__ = "outbox"
    __table_args__ = (
        # The relay only ever scans unpublished events, so the partial index stays tiny.
        Index("ix_outbox_unpublished", "created_at", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    aggregate_id: Mapped[uuid.UUID]
    event_type: Mapped[str] = mapped_column(String(128))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    published_at: Mapped[datetime | None]
    publish_attempts: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
