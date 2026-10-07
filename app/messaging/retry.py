from dataclasses import dataclass
from typing import Self

from app.config import Settings


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff between processing attempts of a message.

    Attempt 1 is the original delivery. After a failed attempt N (N < max_attempts) the
    message is redelivered in `base_delay * multiplier ** (N - 1)` seconds; after the last
    attempt it goes to the dead letter queue. The defaults give 3 attempts with 2s and 4s pauses.
    """

    max_attempts: int = 3
    base_delay: float = 2.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(max_attempts=settings.retry_max_attempts, base_delay=settings.retry_base_delay)

    def can_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts

    def delay_after(self, attempt: int) -> float:
        """Pause in seconds between the failed `attempt` and the next one."""
        if not 1 <= attempt < self.max_attempts:
            raise ValueError(f"attempt {attempt} is not followed by a retry")
        return float(self.base_delay * self.multiplier ** (attempt - 1))

    @property
    def delays(self) -> tuple[float, ...]:
        return tuple(self.delay_after(attempt) for attempt in range(1, self.max_attempts))
