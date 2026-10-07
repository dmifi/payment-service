from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.routes import health, payments
from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.logging_config import setup_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    if settings.api_key is None:
        raise RuntimeError("API_KEY environment variable is required to run the HTTP API")
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        app.state.session_factory = create_session_factory(engine)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Payment Service",
        version="0.1.0",
        summary="Asynchronous payment processing",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.include_router(health.router)
    app.include_router(payments.router, prefix="/api/v1")
    register_exception_handlers(app)
    return app
