# Payments Service

Микросервис асинхронной обработки платежей: принимает запрос на оплату, обрабатывает его
через эмуляцию платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker.

## Архитектура

```
Client ──POST /payments──► API ──(одна транзакция)──► payments + outbox   (PostgreSQL)
                                                          │
                              outbox relay (фон. задача в API, FOR UPDATE SKIP LOCKED)
                                                          ▼
                                                   RabbitMQ: payments.new
                                                          │
                                                       Consumer
                              эмуляция шлюза (2–5 с, 90% ok) → UPDATE status → webhook
                                    │ ошибка
                                    ▼
          payments.new.retry.1 (TTL 5с) ─► payments.new   (попытка 2)
          payments.new.retry.2 (TTL 10с) ─► payments.new  (попытка 3)
                                    │ 3-я неудача
                                    ▼
                              payments.dlq
```

**Гарантии и решения**

- **Outbox.** Платёж и событие пишутся в одной транзакции; relay публикует события с publisher confirms
  и только после подтверждения помечает `published_at`. Доставка — at-least-once.
- **Idempotency-Key.** Уникальный индекс в БД. Повтор с тем же ключом и тем же телом возвращает
  исходный платёж (202); с другим телом — `409`. Гонки параллельных запросов обрабатываются через `IntegrityError`.
- **Идемпотентный consumer.** Каждый шаг (смена статуса, отправка webhook) пропускается, если уже выполнен
  (`status != pending`, `webhook_delivered_at`). Повторная попытка из-за недоступного webhook не пересчитывает статус.
- **Retry.** 3 попытки обработки, задержка экспоненциальная: `RETRY_BASE_DELAY_SECONDS * 2^(n-1)` → 5 с, 10 с.
  Реализовано отдельными retry-очередями с TTL (без блокировки очереди и без `sleep` в consumer-е).
- **DLQ.** После 3-й неудачной попытки сообщение уходит в `payments.dlq` с заголовками `x-attempt`, `x-error`.
  Также `payments.new` настроена с dead-letter на `payments.dlq` — reject (например, невалидное тело) не теряет сообщение.
- **Webhook** считается доставленным при ответе 2xx (таймаут 10 с). Получатель должен быть готов к дублям
  (дедупликация по `payment_id`).

## Запуск

```bash
cp .env.example .env        # при желании поменяйте API_KEY
docker compose up --build
```

- API: http://localhost:8000 (Swagger: http://localhost:8000/docs)
- RabbitMQ UI: http://localhost:15672 (guest / guest)

Миграции применяются автоматически при старте контейнера `api`; `consumer` стартует после того, как `api` стал healthy.

## Примеры

Все эндпоинты требуют заголовок `X-API-Key` (значение из `.env`, по умолчанию `change-me-secret-key`).

**Приёмник webhook на хосте** (для консьюмера в Docker используйте `host.docker.internal`):

```bash
python scripts/webhook_receiver.py              # отвечает 200, печатает тело
python scripts/webhook_receiver.py --status 500 --port 9001   # для проверки retry/DLQ
```

**Создать платёж**

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me-secret-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": "42"},
        "webhook_url": "http://host.docker.internal:9000/hook"
      }'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"672c9ea4-...","status":"pending","created_at":"2026-10-15T20:06:57.821370Z"}
```

**Получить платёж**

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me-secret-key"
```

**Тело webhook**

```json
{
  "payment_id": "672c9ea4-...",
  "status": "succeeded",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": "42"},
  "created_at": "...",
  "processed_at": "..."
}
```

**Проверка retry и DLQ.** Создайте платёж с `webhook_url` на приёмник, отвечающий 500
(`--status 500`). В логах consumer-а будут попытки 1/3 и 2/3 (через 5 и 10 с), затем
сообщение окажется в `payments.dlq` (видно в RabbitMQ UI → Queues).

## Коды ответов

| Код | Когда |
|-----|-------|
| 202 | платёж принят (в т.ч. повтор с тем же Idempotency-Key) |
| 401 | нет/неверный `X-API-Key` |
| 404 | платёж не найден |
| 409 | `Idempotency-Key` уже использован с другим телом |
| 422 | ошибка валидации (нет заголовка `Idempotency-Key`, неверная валюта и т.д.) |

`GET /health` — служебный эндпоинт для healthcheck, без аутентификации.

## Конфигурация (env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | |
| `API_KEY` | `change-me-secret-key` | значение `X-API-Key` |
| `MAX_ATTEMPTS` | `3` | попыток обработки до DLQ |
| `RETRY_BASE_DELAY_SECONDS` | `5` | база экспоненциальной задержки |
| `WEBHOOK_TIMEOUT_SECONDS` | `10` | |
| `PROCESSING_MIN_SECONDS` / `PROCESSING_MAX_SECONDS` | `2` / `5` | длительность эмуляции |
| `SUCCESS_RATE` | `0.9` | доля успешных платежей |

> Если меняете `MAX_ATTEMPTS` или `RETRY_BASE_DELAY_SECONDS` на уже существующем брокере,
> удалите retry-очереди (или `docker compose down -v`): RabbitMQ не позволяет переобъявить очередь с другими аргументами.

## Локальная разработка без Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
uvicorn app.main:app --reload            # API + outbox relay
faststream run app.consumer:app          # consumer
```

## Структура

```
app/
  main.py       FastAPI, lifespan (брокер + outbox relay)
  api.py        POST/GET /api/v1/payments
  security.py   проверка X-API-Key
  models.py     Payment, OutboxEvent
  schemas.py    Pydantic v2 схемы
  outbox.py     relay: outbox → RabbitMQ
  messaging.py  топология очередей (payments.new, retry.N, payments.dlq)
  consumer.py   FastStream consumer: обработка, webhook, retry/DLQ
  webhook.py    отправка webhook (httpx)
alembic/        миграции
scripts/webhook_receiver.py   демо-приёмник webhook
```
