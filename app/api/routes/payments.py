from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response, status

from app.api.dependencies import PaymentServiceDep, require_api_key
from app.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentDetails

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse}},
)

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Unique key of the payment: repeating a request with it returns "
        "the same payment instead of creating a new one",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a payment",
    description="Accepts the payment for asynchronous processing. The result is sent "
    "to `webhook_url` and is available via `GET /api/v1/payments/{payment_id}`.\n\n"
    "A repeated request with the same `Idempotency-Key` and body returns the existing "
    "payment (with the `Idempotent-Replayed: true` header); a different body is rejected.",
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Invalid body, or Idempotency-Key reused with a different body"
        },
    },
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    service: PaymentServiceDep,
    request: Request,
    response: Response,
) -> PaymentAccepted:
    result = await service.create_payment(body, idempotency_key)
    response.headers["Location"] = str(request.url_for("get_payment", payment_id=result.payment.id))
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted.from_model(result.payment)


@router.get(
    "/{payment_id}",
    summary="Get a payment",
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentDetails:
    return PaymentDetails.from_model(await service.get_payment(payment_id))
