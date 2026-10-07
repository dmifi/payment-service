"""Demo webhook receiver for the docker-compose environment (not part of the service).

    POST /webhooks              accept the notification (HTTP 200)
    POST /webhooks?fail=N       answer HTTP 500 to the first N deliveries of each payment,
                                then accept: shows retries with exponential backoff
    POST /webhooks?status=CODE  always answer CODE, e.g. 500 (retries, then DLQ)
                                or 400 (permanent error, straight to DLQ)
    GET  /webhooks              notifications received so far (optionally ?payment_id=...)

If WEBHOOK_SECRET is set, the HMAC signature of every request is verified and logged.
"""

import hashlib
import hmac
import json
import logging
import os
from collections import Counter
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import FastAPI, Query, Request, Response

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s")
logger = logging.getLogger("webhook-receiver")

app = FastAPI(title="Demo webhook receiver")

_SECRET = os.environ.get("WEBHOOK_SECRET", "").encode()
_deliveries: list[dict[str, Any]] = []
_attempts: Counter[str] = Counter()


def _verify_signature(request: Request, body: bytes) -> str:
    if not _SECRET:
        return "not configured"
    timestamp = request.headers.get("X-Webhook-Timestamp", "")
    received = request.headers.get("X-Webhook-Signature", "").removeprefix("sha256=")
    expected = hmac.new(_SECRET, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return "valid" if hmac.compare_digest(received, expected) else "INVALID"


@app.post("/webhooks")
async def receive_webhook(
    request: Request,
    fail: Annotated[int, Query(ge=0)] = 0,
    status: Annotated[int, Query(ge=200, le=599)] = 200,
) -> Response:
    body = await request.body()
    payload = json.loads(body)
    payment_id = str(payload.get("payment", {}).get("payment_id"))
    _attempts[payment_id] += 1
    attempt = _attempts[payment_id]
    response_status = 500 if attempt <= fail else status

    _deliveries.append(
        {
            "received_at": datetime.now(UTC).isoformat(),
            "attempt": attempt,
            "response_status": response_status,
            "signature": _verify_signature(request, body),
            "payload": payload,
        }
    )
    logger.info(
        "%s for payment %s: attempt %d, signature %s -> HTTP %d",
        payload.get("event"),
        payment_id,
        attempt,
        _deliveries[-1]["signature"],
        response_status,
    )
    return Response(status_code=response_status)


@app.get("/webhooks")
async def list_webhooks(payment_id: str | None = None) -> list[dict[str, Any]]:
    if payment_id is None:
        return _deliveries
    return [
        d for d in _deliveries if d["payload"].get("payment", {}).get("payment_id") == payment_id
    ]
