.PHONY: up down logs test e2e lint format

up:  ## build and start the whole stack
	docker compose up -d --build --wait

down:  ## stop the stack and drop its data
	docker compose down -v

logs:
	docker compose logs -f api outbox-relay consumer webhook-receiver

test:  ## unit + integration tests (PostgreSQL and RabbitMQ via testcontainers)
	uv run pytest

e2e:  ## end-to-end tests against the running stack (`make up` first)
	uv run pytest -m e2e

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

format:
	uv run ruff format .
	uv run ruff check --fix .
