from uuid import UUID


class PaymentServiceError(Exception):
    """Base class for domain errors."""


class PaymentNotFoundError(PaymentServiceError):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id = payment_id


class IdempotencyKeyReusedError(PaymentServiceError):
    """The Idempotency-Key was already used for a request with a different payload."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__("Idempotency-Key has already been used with a different request payload")
        self.idempotency_key = idempotency_key


class WebhookDeliveryError(PaymentServiceError):
    """The merchant's webhook endpoint did not accept the notification."""

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable
