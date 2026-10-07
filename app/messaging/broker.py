from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings

APP_ID = "payment-service"


def create_broker(settings: Settings, *, prefetch_count: int | None = None) -> RabbitBroker:
    return RabbitBroker(
        str(settings.rabbitmq_url),
        app_id=APP_ID,
        default_channel=Channel(
            # Bounds the number of messages processed concurrently by one consumer.
            prefetch_count=prefetch_count,
            # Publishing awaits the broker's confirmation that the message is persisted ...
            publisher_confirms=True,
            # ... and fails if no queue is bound for it instead of silently dropping it.
            on_return_raises=True,
        ),
        # On shutdown wait for in-flight messages: a payment takes up to 5s plus the webhook.
        graceful_timeout=settings.gateway_max_delay + settings.webhook_timeout + 5,
    )
