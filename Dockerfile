# syntax=docker/dockerfile:1

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

# Dependencies only: this layer is rebuilt when the lock file changes, not on every code change.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project


FROM python:3.14-slim

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --system app && useradd --system --gid app --no-create-home app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY alembic.ini ./
COPY migrations ./migrations
COPY app ./app
COPY tools ./tools

USER app

EXPOSE 8000

# The same image runs every process; docker-compose.yml overrides the command per service.
CMD ["uvicorn", "app.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
