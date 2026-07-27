# Notification Service (Go) — сервис массовой рассылки уведомлений

Сервис на **Go** для массовой отправки SMS и Email-уведомлений с гарантированной доставкой, приоритезацией трафика и защитой от дублирования сообщений. Полный цикл обработки — от приёма запроса через API до фиксации статуса доставки в базе данных.

## Возможности

### Функциональные

- **Массовая рассылка** — единый API-endpoint для отправки одного сообщения множеству получателей через заданный канал (`sms` / `email`).
- **Приоритезация трафика** — Приоритеты: `transactional` (9), `default` (5), `marketing` (2).
- **Детализация статусов** — API для получения истории всех уведомлений конкретного получателя с фильтрацией по каналу и статусу и постраничной пагинацией.

### Нефункциональные

- **At-least-once delivery** — сообщения персистентно хранятся в очереди RabbitMQ. При временных сбоях шлюза выполняется автоматический повтор (retry с настраиваемым `MAX_RETRIES`).
- **Exactly-once на уровне бизнес-логики** — idempotency-ключи (Redis) + уникальные в PostgreSQL + проверка терминального статуса в worker-обработчике исключают повторную отправку.
- **Дедубликация запросов** — повторный вызов API с тем же `idempotency_key` возвращает `409 Conflict`, не создавая дубликатов.
- **Cloud Native** — весь стек упакован в Docker. Проект поднимается одной командой `docker-compose up`. Один бинарник обслуживает и API, и worker.
- **Graceful shutdown** — API-сервер и worker корректно завершают работу при `SIGTERM`/`SIGINT`.
- **Structured logging** — `log/slog` с request-id в каждом запросе.

## Жизненный цикл уведомления

```
queued → sent → delivered
              → rejected
```

| Статус       | Описание |
|-------------|----------|
| `queued`     | Сообщение принято, записано в БД, ожидает отправки |
| `sent`       | Передано шлюзу (провайдеру) |
| `delivered`  | Подтверждено провайдером — доставлено получателю |
| `rejected`   | Постоянная ошибка доставки (несуществующий номер/email и т.д.) |

## API

Базовый путь: `/api/v1`

### POST `/notifications/bulk`

Запуск массовой рассылки.

**Тело запроса (JSON):**

```json
{
  "channel": "sms",
  "message": "Текст уведомления",
  "recipients": ["user-001", "user-002", "user-003"],
  "priority": "transactional",
  "idempotency_key": "550e8400-e29b-41d4-a716-446655440000"
}
```

| Поле              | Тип      | Обязательное | Описание |
|-------------------|----------|:---:|----------|
| `channel`         | string   | да  | Канал отправки: `sms` или `email` |
| `message`         | string   | да  | Текст уведомления (макс. 2000 символов) |
| `recipients`      | string[] | да  | Массив идентификаторов получателей (1–1000) |
| `priority`        | string   | да  | Приоритет: `transactional`, `marketing`, `default` |
| `idempotency_key` | string   | да  | Уникальный ключ идемпотентности (UUID v4) |

**Ответ (202 Accepted):**

```json
{
  "status": "accepted",
  "batch_id": "e4da3b7f-bbce-4d1a-b0fc-eacf8b5a1c91",
  "accepted_count": 3
}
```

**Ответ при дубликате (409 Conflict):**

```json
{
  "status": "duplicate",
  "message": "This request has already been processed.",
  "idempotency_key": "550e8400-e29b-41d4-a716-446655440000"
}
```

### GET `/subscribers/{subscriber_id}/notifications`

Получение истории уведомлений получателя с пагинацией.

**Query-параметры (опционально):**

| Параметр   | Тип    | Описание |
|------------|--------|----------|
| `channel`  | string | Фильтр по каналу (`sms` / `email`) |
| `status`   | string | Фильтр по статусу (`queued`, `sent`, `delivered`, `rejected`) |
| `page`     | int    | Номер страницы (по умолчанию: 1) |
| `per_page` | int    | Записей на странице (1–100, по умолчанию: 20) |

**Ответ (200 OK):**

```json
{
  "data": [
    {
      "id": 42,
      "subscriber_id": "user-001",
      "channel": "sms",
      "message": "Текст уведомления",
      "status": "delivered",
      "priority": 9,
      "idempotency_key": "550e8400-e29b-41d4-a716-446655440000",
      "batch_id": "e4da3b7f-bbce-4d1a-b0fc-eacf8b5a1c91",
      "attempts": 1,
      "created_at": "2026-05-22T12:00:00Z",
      "updated_at": "2026-05-22T12:00:02Z"
    }
  ],
  "meta": {
    "current_page": 1,
    "last_page": 1,
    "per_page": 20,
    "total": 1
  }
}
```

### GET `/health`

Health-check эндпоинт. Возвращает `200 OK` при доступности PostgreSQL.

## Архитектура

```
Client Request
     │
     ▼
┌──────────┐     ┌───────────────┐     ┌──────────┐
│  chi     │────▶│ notificationd │────▶│ RabbitMQ │
│  router  │     │   (serve)     │     │  (5673)  │
└──────────┘     └───────┬───────┘     └────┬─────┘
                         │                   │
             ┌───────────┼───────────────────┤
             ▼           ▼                   ▼
        ┌─────────┐ ┌─────────┐    ┌──────────────────┐
        │PostgreSQL│ │  Redis  │    │ notificationd    │
        │ (5433)   │ │ (6380)  │    │    (work)        │
        └─────────┘ └─────────┘    └────┬─────────────┘
                                        │
                                        ▼
                                 ┌──────────────┐
                                 │ Mock SMS/    │
                                 │ Email Gateway│
                                 └──────────────┘
```

- **chi router** — лёгкий HTTP-роутер на порту 8080 (middleware: request-id, structured logging, panic recovery, таймауты)
- **API-сервер** (`notificationd serve`) — принимает API-запросы, проверяет идемпотентность, пишет в БД, публикует задания в RabbitMQ
- **Worker** (`notificationd work`) — отдельный процесс того же бинарника, потребляет задания из RabbitMQ, вызывает мок-шлюзы, обновляет статусы с retry-логикой
- **PostgreSQL 17** — основное хранилище: уведомления, миграции
- **Redis 7** — идемпотентность (`SETNX`-блокировки)
- **RabbitMQ 4** — брокер сообщений с поддержкой приоритетов (`x-max-priority=10`)

## Стек технологий

| Компонент          | Технология |
|--------------------|------------|
| Язык               | Go 1.25   |
| HTTP-роутер        | chi v5    |
| Валидация          | go-playground/validator |
| База данных        | PostgreSQL 17 (pgx v5) |
| Брокер очередей    | RabbitMQ 4 (amqp091-go) |
| Кэш / идемпотентность | Redis 7 (go-redis v9) |
| Миграции           | golang-migrate v4 |
| Конфигурация       | caarlos0/env v11 |
| Логирование        | log/slog (structured) |
| Контейнеризация    | Docker + multi-stage build |

## Быстрый старт

### 1. Клонирование

```bash
git clone <repo-url> smlogistics
cd smlogistics
```

### 2. Конфигурация

```bash
cp .env.example .env
```

Файл `.env.example` уже содержит корректные настройки для работы в Docker-окружении. При необходимости можно изменить параметры:


### 3. Запуск

```bash
docker-compose up -d
```

Будут подняты все сервисы:

| Сервис          | Контейнер            | Порт (хост) | Порт (контейнер) |
|-----------------|----------------------|:-----------:|:----------------:|
| API-сервер      | `sml_noti_api`       | 8080        | 8080             |
| Worker          | `sml_noti_worker`    | —           | —                |
| PostgreSQL      | `sml_noti_db`        | 5433        | 5432             |
| RabbitMQ        | `sml_noti_rabbitmq`  | 5673, 15673 | 5672, 15672      |
| Redis           | `sml_noti_redis`     | 6380        | 6379             |

Обратите внимание: порты на хосте смещены на +1 относительно стандартных (8080, 5433, 5673, 15673, 6380), чтобы не конфликтовать с возможными запущенными.

После запуска:
- API доступен на `http://localhost:8080/api/v1/`
- Health-check: `http://localhost:8080/health`
- RabbitMQ Management UI: `http://localhost:15673` (логин/пароль: `notify_user` / `secret`)

### 4. Миграции

Миграции применяются автоматически контейнером `sml_noti_migrate` при запуске `docker-compose up`.

### 5. Примеры запросов

**Отправка массовой рассылки:**

```bash
curl -X POST http://localhost:8080/api/v1/notifications/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "channel": "sms",
    "message": "Ваш заказ готов!",
    "recipients": ["user-001", "user-002"],
    "priority": "transactional",
    "idempotency_key": "550e8400-e29b-41d4-a716-446655440000"
  }'
```

**Получение статусов:**

```bash
curl "http://localhost:8080/api/v1/subscribers/user-001/notifications?status=delivered&per_page=10"
```

**Health-check:**

```bash
curl http://localhost:8080/health
```

## Структура проекта

```
smlogistics-go/
├── cmd/
│   └── notificationd/
│       └── main.go                 # Точка входа: serve / work
├── internal/
│   ├── api/
│   │   └── router.go               # chi-роутер, middleware, хендлеры
│   ├── config/
│   │   └── config.go               # Парсинг env-переменных, DSN/URL-билдеры
│   ├── domain/
│   │   └── notification.go         # Доменная модель: Channel, Status, Priority, Notification
│   ├── gateway/
│   │   ├── gateway.go              # Интерфейс Gateway + GatewayResponse
│   │   └── mockgateway.go          # MockSMS/MockEmail с настраиваемым failure rate
│   ├── log/
│   │   └── logger.go               # slog с JSON/text-форматом
│   ├── service/
│   │   ├── dispatch.go             # DispatchService: пакетная диспетчеризация в RabbitMQ
│   │   └── idempotency.go          # IdempotencyService: Redis SETNX-блокировки
│   ├── store/
│   │   └── pg.go                   # PGStore: PostgreSQL-репозиторий (pgx)
│   └── worker/
│       └── consumer.go             # Consumer: потребление из RabbitMQ, вызов шлюзов, retry
├── migrations/
│   ├── 000001_init.up.sql          # Создание таблицы notifications
│   └── 000001_init.down.sql        # Откат миграции
├── docker/
│   ├── Dockerfile                   # Multi-stage сборка (golang → alpine)
│   └── rabbitmq/
│       ├── definitions.json         # Предварительное объявление очереди с приоритетами
│       └── rabbitmq.conf            # Конфигурация RabbitMQ
├── docker-compose.yml               # Оркестрация: api, worker, migrate, db, rabbitmq, redis
├── entrypoint.sh                    # Точка входа контейнера: migrate / serve / work
├── go.mod
├── go.sum
├── .env.example                     # Шаблон конфигурации
└── .dockerignore
```
