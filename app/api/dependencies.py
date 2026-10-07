import secrets
from collections.abc import AsyncIterator
from typing import Annotated, cast

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.services.payments import PaymentService

api_key_header = APIKeyHeader(
    name="X-API-Key", auto_error=False, description="Static API key of the service"
)


def get_app_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


async def require_api_key(
    settings: Annotated[Settings, Depends(get_app_settings)],
    api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    expected = settings.api_key
    # Constant-time comparison: the response time must not leak how much of the key matched.
    if (
        expected is None
        or api_key is None
        or not secrets.compare_digest(api_key.encode(), expected.get_secret_value().encode())
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "APIKey"},
        )


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)
    async with session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_payment_service(session: SessionDep) -> PaymentService:
    return PaymentService(session)


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
