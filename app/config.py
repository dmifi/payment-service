from functools import lru_cache
from typing import Literal, Self

from pydantic import AmqpDsn, Field, PostgresDsn, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration of all service processes, read from environment variables (or `.env`)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # HTTP API. The key is mandatory for the API process and unused by the workers.
    api_key: SecretStr | None = None

    # PostgreSQL
    database_url: PostgresDsn = PostgresDsn(
        "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    )
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)

    # RabbitMQ
    rabbitmq_url: AmqpDsn = AmqpDsn("amqp://guest:guest@localhost:5672/")

    # Outbox relay
    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_poll_interval: float = Field(default=0.5, gt=0, description="Seconds")

    # Consumer
    consumer_prefetch_count: int = Field(
        default=10, ge=1, description="Messages processed concurrently by one consumer"
    )
    retry_max_attempts: int = Field(
        default=3, ge=1, description="Processing attempts per message before it goes to the DLQ"
    )
    retry_base_delay: float = Field(
        default=2.0, gt=0, description="Seconds before the first retry, doubled for every next one"
    )

    # Payment gateway emulator
    gateway_min_delay: float = Field(default=2.0, ge=0, description="Seconds")
    gateway_max_delay: float = Field(default=5.0, ge=0, description="Seconds")
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    # Webhooks
    webhook_timeout: float = Field(default=10.0, gt=0, description="Seconds")
    webhook_secret: SecretStr | None = Field(
        default=None, description="Enables HMAC-SHA256 signing of webhook requests"
    )

    @model_validator(mode="after")
    def _check_gateway_delays(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("GATEWAY_MIN_DELAY must not be greater than GATEWAY_MAX_DELAY")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
