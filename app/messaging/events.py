from uuid import UUID

from pydantic import BaseModel

PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    """Body of `payments.new` messages.

    Only a reference: the consumer reads the payment from the database, which stays the
    single source of truth (and tells it whether the payment was already processed).
    """

    payment_id: UUID
