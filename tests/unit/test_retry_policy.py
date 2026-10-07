import pytest

from app.messaging.retry import RetryPolicy
from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_ROUTING_KEY, retry_queue


def test_default_policy_gives_three_attempts_with_exponential_pauses() -> None:
    policy = RetryPolicy()

    assert policy.max_attempts == 3
    assert policy.delays == (2.0, 4.0)
    assert [policy.can_retry(attempt) for attempt in (1, 2, 3)] == [True, True, False]


def test_delays_grow_exponentially() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay=0.5)

    assert policy.delays == (0.5, 1.0, 2.0, 4.0)
    assert policy.delay_after(3) == 2.0


@pytest.mark.parametrize("attempt", [0, 3, 4])
def test_no_delay_after_the_last_attempt(attempt: int) -> None:
    with pytest.raises(ValueError, match="not followed by a retry"):
        RetryPolicy(max_attempts=3).delay_after(attempt)


def test_single_attempt_policy_never_retries() -> None:
    policy = RetryPolicy(max_attempts=1)

    assert policy.delays == ()
    assert not policy.can_retry(1)


@pytest.mark.parametrize(
    "kwargs",
    [{"max_attempts": 0}, {"base_delay": 0}, {"multiplier": 0.5}],
)
def test_invalid_policy_is_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must be"):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


def test_retry_queue_holds_messages_for_the_delay_and_returns_them_to_main_queue() -> None:
    queue = retry_queue(2.0)

    assert queue.name == "payments.new.retry.2000ms"
    assert queue.routing() == queue.name
    assert queue.durable
    assert queue.arguments is not None
    assert queue.arguments["x-message-ttl"] == 2000
    assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE.name
    assert queue.arguments["x-dead-letter-routing-key"] == PAYMENTS_NEW_ROUTING_KEY
