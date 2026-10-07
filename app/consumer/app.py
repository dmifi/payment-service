import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.config import Settings, get_settings
from app.consumer.handler import PaymentCreatedHandler
from app.db.session import create_engine, create_session_factory
from app.messaging.broker import APP_ID, create_broker
from app.messaging.events import PaymentCreatedEvent
from app.messaging.retry import RetryPolicy
from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, declare_topology
from app.services.gateway import EmulatedPaymentGateway, PaymentGateway
from app.services.processing import PaymentProcessor
from app.services.webhooks import WebhookNotifier


def create_app(
    settings: Settings | None = None,
    *,
    gateway: PaymentGateway | None = None,
    webhook_transport: httpx.AsyncBaseTransport | None = None,
) -> FastStream:
    """Composition root of the consumer; `gateway` and `webhook_transport` are test seams."""
    settings = settings or get_settings()
    retry_policy = RetryPolicy.from_settings(settings)
    engine = create_engine(settings)
    broker = create_broker(settings, prefetch_count=settings.consumer_prefetch_count)
    http_client = httpx.AsyncClient(
        timeout=settings.webhook_timeout,
        headers={"User-Agent": APP_ID},
        transport=webhook_transport,
    )
    webhook_secret = settings.webhook_secret.get_secret_value() if settings.webhook_secret else None

    handler = PaymentCreatedHandler(
        processor=PaymentProcessor(
            session_factory=create_session_factory(engine),
            gateway=gateway
            or EmulatedPaymentGateway(
                min_delay=settings.gateway_min_delay,
                max_delay=settings.gateway_max_delay,
                success_rate=settings.gateway_success_rate,
            ),
            notifier=WebhookNotifier(http_client, secret=webhook_secret),
        ),
        broker=broker,
        retry_policy=retry_policy,
    )

    @broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE, title="payments.new")
    async def on_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        """Charge the payment, store the result and notify the merchant via webhook."""
        await handler.handle(event, message)

    async def declare_queues() -> None:
        # Runs before the subscriber starts: a message must not be rejected into a missing DLX.
        await broker.connect()
        await declare_topology(broker, retry_policy)

    async def close_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(broker, on_startup=[declare_queues], after_shutdown=[close_resources])
