import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Protocol

from app.db.models import Payment

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ChargeResult:
    succeeded: bool


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> ChargeResult:
        """Charge the payment. A decline is a regular result, not an exception.

        A real implementation passes `payment.id` to the acquirer as its idempotency key:
        if the consumer crashes after the charge but before the commit, the charge is
        repeated on redelivery and must not take the money twice.
        """
        ...


class EmulatedPaymentGateway:
    """Stand-in for an external acquirer: answers in `min_delay..max_delay` seconds
    and approves `success_rate` of the charges."""

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()  # noqa: S311 - an emulation, not cryptography

    async def charge(self, payment: Payment) -> ChargeResult:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        succeeded = self._rng.random() < self._success_rate
        logger.info(
            "Gateway %s payment %s (%s %s)",
            "approved" if succeeded else "declined",
            payment.id,
            payment.amount,
            payment.currency,
        )
        return ChargeResult(succeeded=succeeded)
