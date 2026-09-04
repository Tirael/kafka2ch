# План: sandbox-приложение Kafka → ClickHouse (Protobuf + Schema Registry)

## Цель

Демонстрационный стенд в docker compose:

1. Приложение на C# (.NET 8) публикует Protobuf-сообщения в Kafka (KRaft) через Confluent Schema Registry.
2. ClickHouse 26.x средствами Kafka table engine читает топик и складывает события в «сырую» таблицу.
3. Инкрементальная materialized view агрегирует данные в отдельную таблицу.
4. Background worker в том же sandbox-приложении периодически читает агрегаты из ClickHouse и логирует их.

## Архитектура

```
┌─────────────────────────── sandbox app (.NET 8) ────────────────────────────┐
│  ProducerWorker (BackgroundService)          AggregatesReaderWorker         │
│  Confluent.Kafka + ProtobufSerializer        ClickHouse.Client (HTTP :8123) │
└───────────────┬──────────────────────────────────────────▲──────────────────┘
                │ protobuf (Confluent wire format)          │ SELECT агрегатов
                ▼                                           │
        Kafka (KRaft) ── topic `orders` ◄── Schema Registry (регистрация схемы)
                │
                ▼  ClickHouse 26.x
  Kafka engine table (ProtobufSingle, kafka_schema_registry_skip_bytes = 6)
                │  MV (триггер на insert)
                ▼
        orders (MergeTree, сырые события)
                │  MV (инкрементальная агрегация)
                ▼
        orders_agg_1m (SummingMergeTree, поминутные агрегаты)
```

## Ключевое техническое решение: Protobuf + Schema Registry в ClickHouse

Producer с `Confluent.SchemaRegistry.Serdes.Protobuf` пишет сообщения в Confluent wire format:
`magic byte (1) + schema id (4) + message-indexes (varint, для первого message в .proto — 1 байт 0x00) + protobuf payload`.

- Нативного формата `ProtobufConfluent` в ClickHouse 26.x **нет** (PR [#94750](https://github.com/ClickHouse/ClickHouse/pull/94750) ещё открыт).
- Используем настройку Kafka engine `kafka_schema_registry_skip_bytes = 6` (доступна с версий после ноября 2025, PR [#89621](https://github.com/ClickHouse/ClickHouse/pull/89621)): ClickHouse пропускает 6-байтовый конверт и парсит payload как `ProtobufSingle` по локальной `.proto`-схеме из `/var/lib/clickhouse/format_schemas`.
- **Ограничение**: заголовок фиксированной длины 6 байт валиден только когда сериализуется *первый* (лучше — единственный) `message` в `.proto`-файле. Держим в файле ровно один message-тип. Категория: derived.
- План Б (если skip_bytes окажется недоступен в выбранном образе): producer регистрирует схему в Schema Registry «для порядка», но сериализует чистый protobuf без конверта (`Google.Protobuf` напрямую); ClickHouse читает `ProtobufSingle` без skip_bytes.

## Архитектурные решения ClickHouse (с провенансом)

| Решение | Обоснование | Категория |
|---|---|---|
| Kafka engine + MV → MergeTree | Декаплинг, replay, бурстовая нагрузка — документированный паттерн потоковой ингестии | official |
| Сырая таблица `orders` на MergeTree, `ORDER BY (event_time, order_id)`, без PARTITION BY (объёмы sandbox малы) | Ключ сортировки под запросы по времени; партиционирование на малых объёмах вредно | official |
| Инкрементальная MV → `SummingMergeTree` для повторяющейся поминутной агрегации | Документированный best fit для rollup-ов над append-only потоком | official |
| Worker читает агрегат через `GROUP BY` поверх SummingMergeTree (а не `FINAL`) | Слияние строк в SummingMergeTree отложенное; GROUP BY с sum() всегда даёт корректный результат | official |
| `kafka_schema_registry_skip_bytes = 6` для Confluent-Protobuf | Единственный способ съесть Confluent-конверт без ClickPipes | derived |

## Модель данных

Демо-домен — заказы. `contracts/order_event.proto`:

```protobuf
syntax = "proto3";
package sandbox.orders.v1;
option csharp_namespace = "Sandbox.Contracts";

message OrderEvent {
  string order_id = 1;          // UUID
  string category = 2;          // electronics / books / food ...
  double amount = 3;
  uint32 quantity = 4;
  int64 event_time_unix_ms = 5;
}
```

Один и тот же файл используется дважды: codegen в C# (Grpc.Tools) и как format schema в ClickHouse (монтируется в контейнер).

## DDL ClickHouse (docker-entrypoint-initdb.d)

```sql
CREATE TABLE orders_queue
(
    order_id            String,
    category            LowCardinality(String),
    amount              Float64,
    quantity            UInt32,
    event_time_unix_ms  Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'order_event.proto:sandbox.orders.v1.OrderEvent',
    kafka_schema_registry_skip_bytes = 6,
    kafka_num_consumers = 1;

CREATE TABLE orders
(
    order_id   String,
    category   LowCardinality(String),
    amount     Float64,
    quantity   UInt32,
    event_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, order_id);

CREATE MATERIALIZED VIEW orders_mv TO orders AS
SELECT
    order_id,
    category,
    amount,
    quantity,
    fromUnixTimestamp64Milli(event_time_unix_ms) AS event_time
FROM orders_queue;

CREATE TABLE orders_agg_1m
(
    minute        DateTime,
    category      LowCardinality(String),
    orders_count  UInt64,
    total_amount  Float64,
    total_qty     UInt64
)
ENGINE = SummingMergeTree
ORDER BY (minute, category);

CREATE MATERIALIZED VIEW orders_agg_mv TO orders_agg_1m AS
SELECT
    toStartOfMinute(event_time) AS minute,
    category,
    count()                     AS orders_count,
    sum(amount)                 AS total_amount,
    sum(quantity)               AS total_qty
FROM orders
GROUP BY minute, category;
```

Чтение из worker (корректно при недомерженных партах):

```sql
SELECT minute, category,
       sum(orders_count) AS orders_count,
       sum(total_amount) AS total_amount,
       sum(total_qty)    AS total_qty
FROM orders_agg_1m
WHERE minute >= now() - INTERVAL 10 MINUTE
GROUP BY minute, category
ORDER BY minute DESC, category;
```

## Sandbox-приложение (.NET 8)

Один worker-хост (`Microsoft.NET.Sdk.Worker`), два hosted service:

- **ProducerWorker**: каждые ~500 мс генерирует `OrderEvent` (случайная категория/сумма) и публикует в топик `orders` через `IProducer<string, OrderEvent>` с `ProtobufSerializer<OrderEvent>` (`AutoRegisterSchemas = true` — схема сама попадает в Schema Registry при первом сообщении).
- **AggregatesReaderWorker**: каждые ~5 с выполняет запрос к `orders_agg_1m` через `ClickHouse.Client` (HTTP :8123) и пишет свод в лог.

Структура решения:

```
src/
  Sandbox.Contracts/        # .proto + Grpc.Tools codegen
  Sandbox.App/
    Workers/ProducerWorker.cs
    Workers/AggregatesReaderWorker.cs
    Program.cs              # Host, DI, конфигурация из env
    appsettings.json
docker/
  clickhouse/init/01_schema.sql
  clickhouse/format_schemas/order_event.proto   # symlink/copy contracts
docker-compose.yml
Dockerfile                  # multi-stage build Sandbox.App
README.md
```

NuGet-зависимости (все MIT/Apache-2.0, активно поддерживаются):

- `Confluent.Kafka` 2.x, `Confluent.SchemaRegistry.Serdes.Protobuf` 2.x
- `Google.Protobuf` 3.x + `Grpc.Tools` (codegen, PrivateAssets)
- `ClickHouse.Client` (ADO.NET, HTTP-протокол)

Устойчивость к порядку старта: retry с backoff при недоступности Kafka/Schema Registry/ClickHouse на старте (контейнеры поднимаются параллельно; healthcheck-и снимают большую часть проблем, ретраи — остальное).

## docker-compose.yml (состав)

| Сервис | Образ | Назначение |
|---|---|---|
| `kafka` | `apache/kafka:3.9.x` (или `confluentinc/cp-kafka:7.8.x`), single-node KRaft (combined broker+controller) | брокер; listener'ы: `kafka:9092` внутри сети, `localhost:29092` наружу для отладки |
| `schema-registry` | `confluentinc/cp-schema-registry:7.8.x` | :8081; `depends_on: kafka (healthy)` |
| `clickhouse` | `clickhouse/clickhouse-server:26.x` (запинить актуальный патч) | монтируются `docker/clickhouse/init` → `/docker-entrypoint-initdb.d`, `format_schemas` → `/var/lib/clickhouse/format_schemas`; healthcheck `SELECT 1` |
| `sandbox-app` | build из `Dockerfile` | `depends_on` на healthy kafka/schema-registry/clickhouse; конфигурация через env |
| `kafka-ui` (опционально) | `provectuslabs/kafka-ui` | визуальная проверка топика и схемы в SR |

Плюс: сервис-«одноразовый» `kafka-init` (или auto.create.topics) для создания топика `orders` с 1 партицией.

## Этапы реализации

1. **Каркас и контракты**: solution, `Sandbox.Contracts` с `.proto` и codegen, копирование `.proto` в `docker/clickhouse/format_schemas` на build.
2. **Инфраструктура**: docker-compose с kafka (KRaft), schema-registry, clickhouse + init-SQL и format schema; healthcheck-и. Smoke: `docker compose up`, вручную произвести сообщение `kcat`-ом невозможно (protobuf) — проверяем просто готовность сервисов.
3. **Producer**: `ProducerWorker` + Dockerfile; проверка — схема появилась в SR (`GET :8081/subjects`), сообщения в топике (kafka-ui).
4. **Пайплайн ClickHouse**: убедиться, что `orders` наполняется и `orders_agg_1m` растёт; при ошибках парсинга смотреть `system.kafka_consumers` / лог clickhouse. Здесь же валидируется решение `skip_bytes = 6`; при провале — переключение на план Б.
5. **Reader**: `AggregatesReaderWorker`, вывод агрегатов в лог.
6. **Полировка**: README с инструкцией запуска и проверочными запросами, `.env` с версиями образов, e2e-прогон с нуля (`docker compose down -v && up`).

## Риски

- **`skip_bytes` и message-indexes**: длина конверта 6 байт гарантирована только для первого message в схеме — держим один message на файл; при эволюции схемы с несколькими типами конверт «поплывёт». Митигируется планом Б.
- **Версия ClickHouse**: настройка появилась в конце 2025 — тег образа 26.x обязателен, старые кэши образов не подойдут.
- **Отставание видимости агрегатов**: Kafka engine флашит блоками (по `kafka_max_block_size`/таймауту ~стрим-флаш); в демо задержка в секунды — норма, отражаем в README.
- **Порядок старта контейнеров**: закрывается healthcheck-ами + ретраями в приложении.

## Критерии готовности демо

- `docker compose up -d --build` с чистого состояния поднимает весь стенд без ручных действий.
- В логах `sandbox-app` видно и публикацию событий, и периодический вывод поминутных агрегатов по категориям.
- `SELECT count() FROM orders` растёт; суммы в `orders_agg_1m` (через GROUP BY) сходятся с `orders`.
- Схема `orders-value` зарегистрирована в Schema Registry.
