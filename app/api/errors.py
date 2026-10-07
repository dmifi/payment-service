from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.exceptions import IdempotencyKeyReusedError, PaymentNotFoundError


async def _payment_not_found(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})


async def _idempotency_key_reused(_: Request, exc: Exception) -> JSONResponse:
    # 422 as recommended by the IETF draft "The Idempotency-Key HTTP Header Field".
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"detail": str(exc)}
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(PaymentNotFoundError, _payment_not_found)
    app.add_exception_handler(IdempotencyKeyReusedError, _idempotency_key_reused)
