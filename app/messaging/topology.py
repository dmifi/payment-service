"""RabbitMQ topology of the service.

    outbox relay ─▶ [payments] ──payments.new──▶ (payments.new) ─▶ consumer
                         ▲                            │
                         │ TTL expired                │ rejected: permanent error or
                         │ (dead-lettered back)       │ the last attempt failed
                         │                            ▼
    (payments.new.retry.<delay>ms) ◀── [payments.retry]     [payments.dlx] ──▶ (payments.new.dlq)
                         ▲
                         └── consumer republishes a message that failed with a transient error

    [exchange]  (queue)

A retry is delayed by a per-delay queue without consumers: its `x-message-ttl` holds the
message for the backoff interval, then RabbitMQ dead-letters it back to `payments.new`.
All messages of one delay queue share the TTL, so there is no head-of-line blocking.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.messaging.retry import RetryPolicy

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_DLQ_ROUTING_KEY = "payments.new.dlq"

PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_DLQ_ROUTING_KEY,
    },
)

PAYMENTS_DLQ = RabbitQueue(
    "payments.new.dlq",
    durable=True,
    routing_key=PAYMENTS_DLQ_ROUTING_KEY,
)


def retry_queue(delay: float) -> RabbitQueue:
    """Delay queue holding messages for `delay` seconds before returning them to payments.new.

    The delay is part of the name: changing the backoff settings creates new queues instead
    of failing on redeclaration of an existing queue with different arguments.
    """
    delay_ms = round(delay * 1000)
    name = f"payments.new.retry.{delay_ms}ms"
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_policy: RetryPolicy) -> None:
    """Idempotently declare all exchanges, queues and bindings.

    Called on startup of both the relay and the consumer: the relay must never publish into
    a missing queue, and the consumer must not reject a message before the DLQ exists.
    """
    bindings = [
        (PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE),
        (DEAD_LETTER_EXCHANGE, PAYMENTS_DLQ),
        *((RETRY_EXCHANGE, retry_queue(delay)) for delay in retry_policy.delays),
    ]
    for exchange, queue in bindings:
        declared_exchange = await broker.declare_exchange(exchange)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing())
