"""Move dead-lettered messages back to `payments.new` once the cause is fixed.

    python -m app.replay_dlq [--limit N]
    docker compose exec consumer python -m app.replay_dlq

Replaying is safe because processing is idempotent: an already charged payment is not
charged again, only its webhook is retried. Every message gets a fresh set of attempts.
"""

import argparse
import asyncio
import logging

from faststream.rabbit import RabbitBroker

from app.config import get_settings
from app.consumer.handler import RETRY_COUNT_HEADER, without_broker_headers
from app.logging_config import setup_logging
from app.messaging.broker import create_broker
from app.messaging.retry import RetryPolicy
from app.messaging.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_ROUTING_KEY,
    declare_topology,
)

logger = logging.getLogger(__name__)


async def replay_dead_letters(broker: RabbitBroker, limit: int | None = None) -> int:
    """Republish up to `limit` messages from the DLQ and return how many were moved."""
    dlq = await broker.declare_queue(PAYMENTS_DLQ)
    # Only what is there now: messages failing again must not be replayed in a loop.
    pending = (await dlq.declare()).message_count or 0
    to_move = pending if limit is None else min(limit, pending)

    moved = 0
    while moved < to_move and (message := await dlq.get(no_ack=False, fail=False)) is not None:
        headers = without_broker_headers(message.headers)
        headers.pop(RETRY_COUNT_HEADER, None)
        await broker.publish(
            message.body,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            headers=headers,
            content_type=message.content_type,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            message_type=message.type,
            persist=True,
        )
        await message.ack()  # after the publish: a failure in between duplicates, never loses
        moved += 1
    return moved


async def main(limit: int | None) -> None:
    settings = get_settings()
    broker = create_broker(settings)
    async with broker:
        await declare_topology(broker, RetryPolicy.from_settings(settings))
        moved = await replay_dead_letters(broker, limit)
    logger.info(
        "Moved %d message(s) from %s back to %s", moved, PAYMENTS_DLQ.name, PAYMENTS_NEW_ROUTING_KEY
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--limit", type=int, default=None, help="move at most N messages")
    setup_logging(get_settings().log_level)
    asyncio.run(main(parser.parse_args().limit))
