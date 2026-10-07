# Payment Service

Микросервис асинхронного процессинга платежей. Принимает платёж по HTTP, обрабатывает его через эмулятор платёжного шлюза и сообщает результат клиенту через webhook.

**Стек:** Python 3.14 · FastAPI · Pydantic v2 · SQLAlchemy 2 (async) · PostgreSQL 18 · RabbitMQ 4 (FastStream) · Alembic · Docker Compose.

- [Быстрый старт](#быстрый-старт)
- [Примеры запросов](#примеры-запросов)
- [Демонстрационные сценарии](#демонстрационные-сценарии)
- [Архитектура](#архитектура)
- [Как выполнены требования](#как-выполнены-требования)
- [Ключевые решения](#ключевые-решения)
- [Конфигурация](#конфигурация)
- [Тесты и качество кода](#тесты-и-качество-кода)
- [Структура проекта](#структура-проекта)
- [Что стоит добавить для продакшена](#что-стоит-добавить-для-продакшена)

## Быстрый старт

Нужен только Docker (Compose v2.24+).

```bash
docker compose up -d --build --wait
```

| Что | Адрес | Доступ |
|---|---|---|
| API, Swagger UI | http://localhost:8000/docs | заголовок `X-API-Key: dev-api-key` |
| RabbitMQ Management | http://localhost:15672 | `payments` / `payments` |
| Демо-приёмник webhook-ов | http://localhost:9000/webhooks | список полученных уведомлений |

Остановить и удалить данные: `docker compose down -v`. Ключ, порты и параметры обработки можно переопределить в `.env` (см. [`.env.example`](.env.example)).

Сервисы в `docker-compose.yml`:

| Сервис | Назначение |
|---|---|
| `postgres`, `rabbitmq` | инфраструктура (с healthcheck-ами) |
| `migrations` | разово применяет миграции Alembic; остальные сервисы ждут его успешного завершения |
| `api` | HTTP API (FastAPI) |
| `outbox-relay` | публикует события из таблицы `outbox` в RabbitMQ |
| `consumer` | обрабатывает `payments.new`: шлюз → статус в БД → webhook, ретраи и DLQ |
| `webhook-receiver` | демо-«мерчант» для приёма webhook-ов ([`tools/webhook_receiver.py`](tools/webhook_receiver.py)), не часть сервиса |

Все процессы запускаются из одного образа с разными командами.

## Примеры запросов

**Создание платежа** — `202 Accepted`:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-1042" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1042",
        "metadata": {"order_id": 1042},
        "webhook_url": "http://webhook-receiver:9000/webhooks"
      }'
```

```http
HTTP/1.1 202 Accepted
location: http://localhost:8000/api/v1/payments/db5aa1cf-c9f9-4c23-9b0d-bea7355c4a11

{"payment_id": "db5aa1cf-c9f9-4c23-9b0d-bea7355c4a11", "status": "pending", "created_at": "2026-10-06T11:44:55.874845Z"}
```

**Получение платежа** (через 2–5 секунд статус станет `succeeded` или `failed`):

```bash
curl http://localhost:8000/api/v1/payments/db5aa1cf-c9f9-4c23-9b0d-bea7355c4a11 -H "X-API-Key: dev-api-key"
```

```json
{
  "payment_id": "db5aa1cf-c9f9-4c23-9b0d-bea7355c4a11",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1042",
  "metadata": {"order_id": 1042},
  "status": "succeeded",
  "idempotency_key": "order-1042",
  "webhook_url": "http://webhook-receiver:9000/webhooks",
  "created_at": "2026-10-06T11:44:55.874845Z",
  "processed_at": "2026-10-06T11:44:59.375773Z"
}
```

Сумма передаётся и возвращается строкой (`"1500.00"`), чтобы не терять точность на float. На вход принимается и число; `100`, `100.0` и `"100.00"` — одна и та же сумма.

**Повтор запроса с тем же `Idempotency-Key`** и тем же телом возвращает уже созданный платёж с заголовком `Idempotent-Replayed: true`; новый платёж не создаётся.

**Тот же ключ, но другое тело** — `422`:

```json
{"detail": "Idempotency-Key has already been used with a different request payload"}
```

**Ответы с ошибками:** `401` — нет ключа или ключ неверный; `404` — платёж не найден; `422` — ошибка валидации, отсутствует `Idempotency-Key` или ключ повторно использован с другим телом.

**Webhook**, который получает `webhook_url` после обработки:

```http
POST /webhooks
Content-Type: application/json
X-Webhook-Timestamp: 1791287099
X-Webhook-Signature: sha256=5f0c…

{
  "event": "payment.succeeded",
  "payment": { …то же представление, что и в GET /api/v1/payments/{id}… }
}
```

`event` — `payment.succeeded` или `payment.failed`. Подпись — HMAC-SHA256 от `"<timestamp>.<body>"` с секретом `WEBHOOK_SECRET`; проверка — `_verify_signature` в [`tools/webhook_receiver.py`](tools/webhook_receiver.py).

## Демонстрационные сценарии

Демо-приёмник умеет имитировать сбои через query-параметры `webhook_url`. Логи удобно смотреть через `docker compose logs -f consumer webhook-receiver`.

| `webhook_url` | Что произойдёт |
|---|---|
| `http://webhook-receiver:9000/webhooks` | доставка с первой попытки |
| `http://webhook-receiver:9000/webhooks?fail=2` | две попытки получают `500`, третья (через 2 с и ещё 4 с) проходит |
| `http://webhook-receiver:9000/webhooks?status=500` | три неудачные попытки, затем сообщение уходит в `payments.new.dlq` |
| `http://webhook-receiver:9000/webhooks?status=400` | постоянная ошибка: сразу в DLQ, без ретраев |

Фрагмент лога consumer-а для `?fail=2`:

```text
WARNING [app.consumer.handler] Payment c133…: attempt 1/3 failed (Webhook endpoint responded with HTTP 500), retrying in 2.0s
INFO    [app.services.processing] Payment c133… is already succeeded, not charging again
WARNING [app.consumer.handler] Payment c133…: attempt 2/3 failed (Webhook endpoint responded with HTTP 500), retrying in 4.0s
INFO    [app.services.processing] Webhook for payment c133… delivered to http://webhook-receiver:9000/webhooks?fail=2
```

**Отказ брокера (Outbox в действии).** API продолжает принимать платежи без RabbitMQ, события копятся в `outbox`, после восстановления брокера они публикуются и обрабатываются:

```bash
docker compose stop rabbitmq
curl -X POST http://localhost:8000/api/v1/payments ...    # 202, событие ждёт в outbox
docker compose start rabbitmq                             # через несколько секунд платёж обработан
```

**Возврат сообщений из DLQ** после устранения причины. Повтор безопасен: уже списанный платёж повторно не списывается, повторяется только webhook.

```bash
docker compose exec consumer python -m app.replay_dlq [--limit N]
```

## Архитектура

```mermaid
flowchart LR
    client([Клиент]) -- "POST /api/v1/payments" --> api[API]
    api -- "одна транзакция:<br/>payments + outbox" --> db[(PostgreSQL)]
    relay[Outbox relay] -- "SELECT … FOR UPDATE<br/>SKIP LOCKED" --> db
    relay -- "publish, confirm" --> ex{{"payments"}}
    ex -- "payments.new" --> q[["payments.new"]]
    q --> consumer[Consumer]
    consumer -- "статус платежа" --> db
    consumer -- "POST webhook" --> merchant([webhook_url])
    consumer -. "временная ошибка,<br/>попытки остались" .-> rex{{"payments.retry"}}
    rex --> rq[["payments.new.retry.*<br/>TTL = задержка"]]
    rq -. "TTL истёк<br/>(dead-letter)" .-> ex
    q -. "reject" .-> dlx{{"payments.dlx"}}
    dlx --> dlq[["payments.new.dlq"]]
```

Жизненный цикл платежа:

1. **API** валидирует запрос и в **одной транзакции** вставляет платёж (`INSERT … ON CONFLICT (idempotency_key) DO NOTHING`) и событие `payment.created` в таблицу `outbox`. Ответ — `202`.
2. **Outbox relay** забирает пачку неопубликованных событий (`FOR UPDATE SKIP LOCKED`), публикует их в exchange `payments` с ключом маршрутизации `payments.new` и помечает `published_at` в той же транзакции.
3. **Consumer** получает сообщение, блокирует строку платежа (`SELECT … FOR UPDATE`) и, если платёж ещё `pending`, вызывает эмулятор шлюза (2–5 с, 90% успеха). Затем сохраняет `succeeded`/`failed` и `processed_at`.
4. Consumer отправляет подписанный webhook, после успешной доставки ставит `webhook_delivered_at` и подтверждает сообщение (ack).
5. При ошибке consumer либо планирует повтор через очередь задержки, либо отклоняет сообщение — RabbitMQ перекладывает его в DLQ.

Топология RabbitMQ (объявляется кодом, идемпотентно, при старте relay и consumer — [`app/messaging/topology.py`](app/messaging/topology.py)):

| Объект | Тип | Назначение |
|---|---|---|
| `payments` | direct exchange | основной обменник событий |
| `payments.new` | очередь | `x-dead-letter-exchange=payments.dlx`: отклонённые сообщения уходят в DLQ |
| `payments.retry` | direct exchange | приём сообщений на повтор |
| `payments.new.retry.2000ms`, `…4000ms` | очереди без потребителей | `x-message-ttl` = задержка, по истечении сообщение возвращается в `payments` → `payments.new` |
| `payments.dlx` | direct exchange | dead letter exchange |
| `payments.new.dlq` | очередь | окончательно необработанные сообщения |

## Как выполнены требования

| Требование | Реализация |
|---|---|
| Сущность Payment, все поля | [`app/db/models.py`](app/db/models.py): `NUMERIC(18,2)`, валюта и статус — `VARCHAR` + `CHECK`, `metadata` — `JSONB`, уникальный `idempotency_key`, `created_at`/`processed_at` (`timestamptz`) |
| Модели и миграции: `payments`, `outbox` | [`migrations/versions/0001_initial.py`](migrations/versions/0001_initial.py); тест сверяет миграции с моделями (`alembic check`) и проверяет downgrade |
| `POST /api/v1/payments` (обязательный `Idempotency-Key`, `202`) | [`app/api/routes/payments.py`](app/api/routes/payments.py), [`app/services/payments.py`](app/services/payments.py) |
| `GET /api/v1/payments/{payment_id}` | там же |
| Событие в `payments.new` при создании | запись в `outbox` в транзакции платежа + [`app/outbox/relay.py`](app/outbox/relay.py) |
| Один consumer: шлюз 2–5 с, 90/10, статус, webhook, повторы | [`app/consumer/handler.py`](app/consumer/handler.py), [`app/services/processing.py`](app/services/processing.py), [`app/services/gateway.py`](app/services/gateway.py), [`app/services/webhooks.py`](app/services/webhooks.py) |
| Outbox pattern | см. [Outbox](#outbox) |
| Idempotency key | см. [Идемпотентность](#идемпотентность) |
| Retry: 3 попытки с экспоненциальной задержкой | см. [Retry и DLQ](#retry-и-dlq) |
| DLQ после 3 попыток | `payments.dlx` → `payments.new.dlq`, команда возврата `app.replay_dlq` |
| `X-API-Key` для всех эндпоинтов | [`app/api/dependencies.py`](app/api/dependencies.py), сравнение за постоянное время; открыты только health-пробы `/health/live`, `/health/ready` и документация |
| Docker: postgres, rabbitmq, api, consumer | [`docker-compose.yml`](docker-compose.yml), [`Dockerfile`](Dockerfile) (multi-stage, uv, непривилегированный пользователь) |

## Ключевые решения

### Outbox

- Платёж и событие пишутся в одной транзакции: событие существует тогда и только тогда, когда платёж закоммичен. API не зависит от доступности RabbitMQ.
- **Relay — отдельный процесс.** API остаётся stateless, а relay масштабируется независимо: `SELECT … FOR UPDATE SKIP LOCKED` позволяет нескольким экземплярам работать параллельно без двойной публикации (есть тест).
- Публикация с **publisher confirms** и `mandatory` + `on_return_raises`. Без последнего RabbitMQ подтверждает и сообщение, которое не попало ни в одну очередь, и relay посчитал бы его доставленным (есть тест).
- `published_at` ставится в той же транзакции, что и выборка. Если relay упадёт между публикацией и коммитом, событие будет опубликовано повторно — это *at-least-once*, поэтому consumer идемпотентен.
- При недоступности брокера relay повторяет попытки с экспоненциальной паузой (до 10 с) и фиксирует `publish_attempts` и `last_error`; соединение с RabbitMQ восстанавливается автоматически.

### Идемпотентность

**API:**
- Уникальный индекс по `idempotency_key` и `INSERT … ON CONFLICT DO NOTHING`. Конкурентный запрос с тем же ключом ждёт на уникальном индексе коммита первого и затем находит готовый платёж. Без гонок и без управления потоком через исключения (тест: 10 параллельных запросов → 1 платёж и 1 событие).
- Хранится `request_fingerprint` — SHA-256 нормализованного тела. Тот же ключ и то же тело — вернуть существующий платёж (`Idempotent-Replayed: true`); тот же ключ и другое тело — `422`, как рекомендует черновик IETF [*The Idempotency-Key HTTP Header Field*](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/). Нормализация (сумма, порядок ключей в `metadata`, пробелы в `description`) не даёт отклонить честный повтор.
- Повтор возвращает **текущий** статус платежа: клиенту полезнее знать, что платёж уже `succeeded`.

**Consumer** (сообщения доставляются минимум один раз, плюс ретраи):
- Строка платежа блокируется `SELECT … FOR UPDATE` на время обращения к шлюзу. Дубликат, пришедший параллельно, ждёт блокировку и видит финальный статус — **шлюз вызывается один раз** (тест с конкурентными дубликатами).
- `webhook_delivered_at` защищает от повторной отправки уже доставленного webhook-а при повторной доставке сообщения.
- Остаточное окно: если процесс упадёт после ответа шлюза, но до коммита, списание повторится. Поэтому реальная интеграция должна передавать `payment.id` шлюзу как его idempotency key — это зафиксировано в контракте `PaymentGateway`.

### Retry и DLQ

- **3 попытки** обработки сообщения: исходная и два повтора с паузами **2 с и 4 с** (`base_delay · 2^(n-1)`). Настраивается через `RETRY_MAX_ATTEMPTS` и `RETRY_BASE_DELAY`.
- Задержка реализована **очередями с TTL + dead-lettering**, а не `sleep` в обработчике. Ожидающие повтора сообщения не занимают слоты prefetch, переживают рестарт consumer-а и видны в RabbitMQ. У каждой очереди задержки свой фиксированный TTL, поэтому нет head-of-line blocking. Имя очереди содержит задержку: изменение настроек создаёт новую очередь, а не падает с `PRECONDITION_FAILED` на переобъявлении.
- **Классификация ошибок.** Временные (сеть, таймаут, `5xx`, `408`/`425`/`429`, ошибки БД, неизвестные) повторяются. Постоянные (остальные `4xx`, неизвестный платёж, невалидное сообщение) сразу уходят в DLQ — повтор не поможет.
- Сообщение в DLQ несёт `x-retry-count`, `x-last-error` и стандартный `x-death`, этого достаточно для разбора. Вернуть сообщения в работу можно командой `python -m app.replay_dlq` — она переносит только текущее содержимое DLQ, не зацикливаясь.
- **Отказ шлюза (10%) — не ошибка обработки**, а бизнес-результат: статус `failed` и webhook `payment.failed`.
- Порядок «сначала опубликовать повтор, потом ack» исключает потерю сообщения: при сбое между шагами возможен только дубль, а его гасит идемпотентность.

### Webhook-и

- Подпись HMAC-SHA256 (`X-Webhook-Signature`) с меткой времени (`X-Webhook-Timestamp`) против подделки и повторного воспроизведения.
- Доставка *at-least-once*: получатель должен дедуплицировать по `payment.payment_id`.
- Редиректы не выполняются, таймаут 10 с.

### Прочее

- **Graceful shutdown:** на SIGTERM consumer перестаёт брать новые сообщения и дорабатывает текущие (`graceful_timeout` 20 с, `stop_grace_period: 30s`); relay завершает текущую пачку.
- **Concurrency:** consumer обрабатывает до `CONSUMER_PREFETCH_COUNT` (10) сообщений одновременно; пул соединений БД рассчитан с запасом.
- **Composition root.** Фабрики `create_app()` для каждого процесса собирают зависимости явно, без глобального состояния; для тестов есть точки подмены шлюза и HTTP-транспорта.

## Конфигурация

Переменные окружения (полный список — [`app/config.py`](app/config.py)):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | — (в compose `dev-api-key`) | ключ для `X-API-Key`, обязателен для API |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ |
| `WEBHOOK_SECRET` | — (в compose `dev-webhook-secret`) | секрет подписи webhook-ов; без него запросы не подписываются |
| `WEBHOOK_TIMEOUT` | `10` | таймаут запроса webhook-а, с |
| `RETRY_MAX_ATTEMPTS` | `3` | попыток обработки сообщения до DLQ |
| `RETRY_BASE_DELAY` | `2` | пауза перед первым повтором, с; удваивается |
| `CONSUMER_PREFETCH_COUNT` | `10` | одновременно обрабатываемых сообщений |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2` / `5` | время ответа эмулятора шлюза, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `OUTBOX_BATCH_SIZE` / `OUTBOX_POLL_INTERVAL` | `100` / `0.5` | размер пачки и период опроса outbox |
| `LOG_LEVEL` | `INFO` | уровень логирования |

## Тесты и качество кода

```bash
uv sync
uv run pytest          # unit + integration: 106 тестов, ~20 с
uv run pytest -m e2e   # e2e против запущенного docker compose
make lint              # ruff + mypy --strict
```

Интеграционные тесты работают с **настоящими** PostgreSQL и RabbitMQ, которые поднимает testcontainers (нужен Docker). Можно указать уже запущенные через `TEST_DATABASE_URL` / `TEST_RABBITMQ_URL`. Что проверяется:

- **API:** валидация, `401`, `404`, атомарность платежа и события outbox, повтор запроса, нормализация тела, `422` при повторном использовании ключа, 10 конкурентных запросов с одним ключом.
- **Outbox relay:** публикация ровно один раз с нужными свойствами (`message_id`, persistent, `correlation_id`); недоступный брокер; немаршрутизируемое сообщение не считается опубликованным; три параллельных relay публикуют каждое событие ровно один раз; фоновый режим.
- **Consumer:** успешная и отклонённая оплата, подпись webhook-а, экспоненциальные паузы между повторами без повторного списания, DLQ после 3 попыток и сразу при постоянной ошибке, неизвестный платёж, невалидные сообщения, повторная и конкурентная доставка дубликатов, возврат из DLQ.
- **Миграции:** upgrade, downgrade, соответствие моделям.
- **E2E** (`tests/e2e`): полный путь через docker compose — от HTTP-запроса до webhook-а и DLQ.

CI (GitHub Actions, [`.github/workflows/ci.yml`](.github/workflows/ci.yml)): линтеры и mypy, тесты, сборка и e2e-проверка docker compose.

## Структура проекта

```text
app/
├── api/              # FastAPI: фабрика приложения, роуты, зависимости (auth, сессия), обработчики ошибок
├── consumer/         # FastStream-приложение consumer-а и обработчик payments.new (ретраи, DLQ)
├── outbox/           # outbox relay и его FastStream-приложение
├── messaging/        # топология RabbitMQ, политика ретраев, схемы сообщений, фабрика брокера
├── services/         # бизнес-логика: создание платежа, обработка, эмулятор шлюза, webhook-и
├── repositories/     # доступ к данным (SQLAlchemy)
├── db/               # модели, базовый класс, движок и сессии
├── schemas.py        # публичный контракт: тела запросов и ответов API, webhook
├── config.py         # настройки (pydantic-settings)
└── replay_dlq.py     # команда возврата сообщений из DLQ
migrations/           # Alembic (async)
tests/                # unit, integration (testcontainers), e2e (docker compose)
tools/                # демо-приёмник webhook-ов
```

## Что стоит добавить для продакшена

Сознательно не входит в объём тестового задания:

- **Очистка `outbox`:** удаление или архивация опубликованных событий по расписанию, либо партиционирование по `created_at`.
- **Наблюдаемость:** метрики (глубина DLQ, задержка outbox, доля отказов) и трейсинг — у FastStream есть middleware для Prometheus и OpenTelemetry. Алерты на рост DLQ и `publish_attempts`.
- **Защита от SSRF:** `webhook_url` задаёт клиент, поэтому нужно запрещать адреса внутренних сетей и проверять разрешённый IP при отправке.
- **Многоарендность:** область действия `Idempotency-Key` в пределах клиента и срок хранения ключей; ключи API на клиента вместо одного статического.
- **Надёжность RabbitMQ:** кластер и quorum-очереди; `x-delivery-limit` как страховка от сообщений, роняющих процесс.
- **Реальный шлюз:** передавать `payment.id` как idempotency key шлюза, сверять «зависшие» `pending`-платежи (reconciliation).
- **Безопасность:** секреты из хранилища (Vault и т. п.), отдельный пользователь БД для миграций.
