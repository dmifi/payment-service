from fastapi import APIRouter, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.api.dependencies import SessionDep

# Infrastructure probes, deliberately outside the API key protection.
router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", summary="Liveness probe")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get(
    "/ready",
    summary="Readiness probe: the database is reachable",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Database is unavailable"}},
)
async def readiness(session: SessionDep) -> dict[str, str]:
    try:
        await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError) as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Database is unavailable") from exc
    return {"status": "ok"}
