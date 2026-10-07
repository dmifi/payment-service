"""Shared fixtures.

Integration tests run against real PostgreSQL and RabbitMQ: containers are started lazily
(only when a test needs them) via testcontainers. Set TEST_DATABASE_URL / TEST_RABBITMQ_URL
to use already running instances instead.
"""

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from app.api.app import create_app
from app.config import Settings
from app.messaging.broker import create_broker
from app.messaging.retry import RetryPolicy
from app.messaging.topology import PAYMENTS_DLQ, PAYMENTS_NEW_QUEUE, declare_topology, retry_queue
from tests.helpers import purge

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POSTGRES_IMAGE = "postgres:18-alpine"
RABBITMQ_IMAGE = "rabbitmq:4-alpine"

API_KEY = "test-api-key"
WEBHOOK_SECRET = "test-webhook-secret"


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    if url := os.environ.get("TEST_DATABASE_URL"):
        yield url
        return

    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    if url := os.environ.get("TEST_RABBITMQ_URL"):
        yield url
        return

    with RabbitMqContainer(RABBITMQ_IMAGE) as rabbitmq:
        host = rabbitmq.get_container_host_ip()
        port = rabbitmq.get_exposed_port(rabbitmq.port)
        yield f"amqp://{rabbitmq.username}:{rabbitmq.password}@{host}:{port}/"


def alembic_config(database_url: str) -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    return config


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    # Sync fixture on purpose: Alembic's env.py runs its own event loop.
    command.upgrade(alembic_config(database_url), "head")
    return database_url


@pytest.fixture(scope="session")
async def engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(migrated_database)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory over an empty database."""
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def settings(migrated_database: str) -> Settings:
    return Settings(
        _env_file=None,
        api_key=API_KEY,
        database_url=migrated_database,
        gateway_min_delay=0,
        gateway_max_delay=0,
        gateway_success_rate=1.0,
        retry_base_delay=0.2,
        webhook_secret=WEBHOOK_SECRET,
        outbox_poll_interval=0.05,
    )


@pytest.fixture
def broker_settings(settings: Settings, rabbitmq_url: str) -> Settings:
    return settings.model_copy(update={"rabbitmq_url": rabbitmq_url})


@pytest.fixture
async def broker(broker_settings: Settings) -> AsyncIterator[RabbitBroker]:
    """A connected broker with the service topology declared and all its queues empty."""
    broker = create_broker(broker_settings)
    await broker.connect()
    retry_policy = RetryPolicy.from_settings(broker_settings)
    await declare_topology(broker, retry_policy)
    await purge(broker, [PAYMENTS_NEW_QUEUE, PAYMENTS_DLQ, *map(retry_queue, retry_policy.delays)])
    yield broker
    await broker.stop()


@pytest.fixture
async def api_client(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with (
        LifespanManager(app) as manager,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=manager.app),
            base_url="http://test",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client
