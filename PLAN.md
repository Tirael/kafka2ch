# План: sandbox-приложение Kafka → ClickHouse (Protobuf + Schema Registry)

## Цель

Демонстрационный стенд в docker compose:

1. Приложение на C# (.NET 8) публикует Protobuf-сообщения в Kafka (KRaft) через Confluent Schema Registry.
2. ClickHouse 26.6+ средствами Kafka table engine читает топик и складывает события в «сырую» таблицу.
3. Инкрементальная materialized view агрегирует данные в отдельную таблицу.
4. Background worker в том же sandbox-приложении периодически читает агрегаты из ClickHouse и логирует их.

## Архитектура

```
┌────────────────── sandbox app (.NET 8, vertical slices) ────────────────────┐
│  Features/PublishOrders                      Features/ReadAggregates        │
│  Confluent.Kafka + ProtobufSerializer        ClickHouse.Client (HTTP :8123) │
└───────────────┬──────────────────────────────────────────▲──────────────────┘
                │ protobuf (Confluent wire format)          │ SELECT агрегатов
                ▼                                           │
        Kafka (KRaft) ── topic `orders` ◄── Schema Registry (регистрация схемы)
                │
                ▼  ClickHouse 26.6+
  Kafka engine table (ProtobufSingle, kafka_schema_registry_skip_bytes = 6)
                │  MV (триггер при poll/insert батча из Kafka engine)
                ▼
        orders (MergeTree, сырые события)
                │  MV (инкрементальная агрегация)
                ▼
        orders_agg_1m (SummingMergeTree, поминутные агрегаты)
```

## Ключевое техническое решение: Protobuf + Schema Registry в ClickHouse

Producer с `Confluent.SchemaRegistry.Serdes.Protobuf` пишет сообщения в Confluent wire format:
`magic byte (1) + schema id (4) + message-indexes (varint, для первого message в .proto — 1 байт 0x00) + protobuf payload`.

- Нативного формата `ProtobufConfluent` в ClickHouse **нет** (PR [#94750](https://github.com/ClickHouse/ClickHouse/pull/94750) закрыт без merge).
- Используем настройку Kafka engine `kafka_schema_registry_skip_bytes = 6` (доступна с **25.11+**, PR [#89621](https://github.com/ClickHouse/ClickHouse/pull/89621)): ClickHouse пропускает 6-байтовый конверт и парсит payload как `ProtobufSingle` по локальной `.proto`-схеме из `/var/lib/clickhouse/format_schemas`.
- **Важно:** официальные примеры ClickHouse для Confluent Schema Registry (JSON/Avro) используют `skip_bytes = 5` (magic + schema id). Для **Protobuf** нужно **6** — добавляется 1 байт message-index (`0x00` для первого top-level message). Не путать при отладке.
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

Демо-домен — заказы. И ключ, и значение Kafka-сообщения — protobuf. Контракт разбит на три файла: основной `order_event.proto` (value) импортирует кастомные общие типы из `common/money.proto`; ключ — отдельный `order_key.proto`.

`protos/common/money.proto`:

```protobuf
syntax = "proto3";
package sandbox.common.v1;
option csharp_namespace = "Sandbox.Contracts.Common";

message Money {
  string currency = 1;          // ISO 4217: RUB, USD ...
  double amount = 2;            // для демо OK; в production — Decimal или Int64 (копейки)
}

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_CREATED = 1;
  ORDER_STATUS_PAID = 2;
}
```

`protos/order_key.proto` (ключ сообщения; отдельный файл, чтобы в каждом сериализуемом файле message остался первым и единственным):

```protobuf
syntax = "proto3";
package sandbox.orders.v1;
option csharp_namespace = "Sandbox.Contracts";

message OrderKey {
  string order_id = 1;          // UUID — определяет партицию
}
```

`protos/order_event.proto` (значение сообщения):

```protobuf
syntax = "proto3";
package sandbox.orders.v1;

import "common/money.proto";

option csharp_namespace = "Sandbox.Contracts";

message OrderEvent {
  string order_id = 1;                    // UUID
  string category = 2;                    // electronics / books / food ...
  sandbox.common.v1.Money price = 3;
  uint32 quantity = 4;
  int64 event_time_unix_ms = 5;
  sandbox.common.v1.OrderStatus status = 6;
}
```

Одно и то же дерево `protos/` используется дважды: codegen в C# и как format schemas в ClickHouse. Импорт должен корректно разрешаться в трёх местах:

1. **C# codegen (Grpc.Tools)**: в `Sandbox.Contracts.csproj` задаётся `ProtoRoot="protos"`, компилируются оба файла — относительный путь в `import "common/money.proto"` разрешается от ProtoRoot. Синхронизация в ClickHouse — MSBuild target `SyncProtoSchemas` (AfterTargets="Build"), копирует `@(Protobuf)` в `docker/clickhouse/format_schemas/` с сохранением структуры каталогов (`SkipUnchangedFiles="true"`), чтобы исключить drift между codegen и format schemas.
2. **Schema Registry**: `ProtobufSerializer` при `AutoRegisterSchemas = true` рекурсивно регистрирует импортируемые схемы как schema references — `common/money.proto` становится отдельным subject, на который ссылается `orders-value`; схема ключа регистрируется как subject `orders-key`. Ставим `SkipKnownTypes = true`, чтобы не регистрировать well-known types Google. Проверка: `GET :8081/subjects` показывает три subject (`orders-key`, `orders-value`, схема-reference), `GET :8081/subjects/orders-value/versions/latest` содержит блок `references`. **Имя reference-subject** Confluent назначает по умолчанию (обычно имя файла, напр. `common/money.proto` или нормализованный вариант) — на этапе 3 зафиксировать фактическое имя и записать в README.
3. **ClickHouse**: импорты в format schema разрешаются относительно каталога `/var/lib/clickhouse/format_schemas`, поэтому монтируем всё дерево `protos/` с сохранением относительных путей (`format_schemas/order_event.proto`, `format_schemas/common/money.proto`). В `kafka_schema` указывается только главный файл — импортированный подтянется сам.

На конверт Confluent (`skip_bytes = 6`) импорты не влияют: message-indexes считаются по top-level message-ам главного файла, а `OrderEvent` в нём остаётся первым и единственным (типы из импортов в индексацию не входят).

Protobuf-ключ на ClickHouse не влияет: Kafka engine парсит только value сообщения, ключ доступен лишь как сырые байты через виртуальную колонку `_key` (в нашем пайплайне не используется — `order_id` есть в самом value). Ключ нужен для детерминированного партиционирования и как демонстрация типизированного контракта key+value.

## DDL ClickHouse (docker-entrypoint-initdb.d)

```sql
CREATE TABLE orders_queue
(
    order_id            String,
    category            LowCardinality(String),
    -- вложенный message Money из common/money.proto маппится на колонки с точкой
    `price.currency`    String,
    `price.amount`      Float64,
    quantity            UInt32,
    event_time_unix_ms  Int64,
    -- proto enum маппится по именам значений; fallback при ошибке парсинга — Int32 + toString() в MV
    status              Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2)
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
    currency   LowCardinality(String),
    amount     Float64,
    quantity   UInt32,
    status     LowCardinality(String),
    event_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, order_id);

CREATE MATERIALIZED VIEW orders_mv TO orders AS
SELECT
    order_id,
    category,
    `price.currency`            AS currency,
    `price.amount`              AS amount,
    quantity,
    toString(status)            AS status,
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

## Sandbox-приложение (.NET 8, vertical slice architecture)

Один worker-хост (`Microsoft.NET.Sdk.Worker`), код организован по вертикальным срезам: каждая фича — самодостаточный каталог со своим worker-ом, моделями, опциями конфигурации и регистрацией DI. Технических слоёв (Services/Repositories/Infrastructure-проектов) нет.

Срезы:

- **`Features/PublishOrders`**: каждые ~500 мс генерирует `OrderEvent` (случайная категория/сумма) и публикует в топик `orders` через `IProducer<OrderKey, OrderEvent>` — и ключ, и значение сериализуются `ProtobufSerializer<T>` (`AutoRegisterSchemas = true` — обе схемы и references сами попадают в Schema Registry при первом сообщении). Внутри среза: `PublishOrdersWorker` (BackgroundService), `OrderEventFactory` (генерация демо-данных), `PublishOrdersOptions`, `PublishOrdersSlice.AddPublishOrders(...)` — extension-метод регистрации.
- **`Features/ReadAggregates`**: каждые ~5 с выполняет запрос к `orders_agg_1m` через `ClickHouse.Client` (HTTP :8123) и пишет свод в лог. Внутри среза: `ReadAggregatesWorker`, `OrderAggregateRow` (модель строки результата), `ReadAggregatesOptions`, `ReadAggregatesSlice.AddReadAggregates(...)`.

Правила организации:

- Срезы не ссылаются друг на друга; общаются только через внешние системы (Kafka, ClickHouse) — что соответствует реальному потоку данных демо.
- **`Common/`** — минимальное разделяемое ядро: фабрики подключений (`KafkaClientFactory` c конфигом брокера/SR, `ClickHouseConnectionFactory`), retry-хелпер для старта. Только то, что нужно более чем одному срезу; бизнес-логики в Common нет.
- `Program.cs` — composition root: `builder.Services.AddCommon(...).AddPublishOrders(...).AddReadAggregates(...)`; конфигурация каждого среза — отдельная секция `appsettings.json`/env (`PublishOrders__IntervalMs`, `ReadAggregates__WindowMinutes`, ...).
- Contracts (protobuf codegen) — отдельный проект, разделяемый срезами как контракт внешней системы.
- MediatR/CQRS-обвязка не используется: в срезе один сценарий, посредник не добавил бы ничего, кроме церемонии.

Структура решения:

```
src/
  Sandbox.Contracts/        # Grpc.Tools codegen, ProtoRoot=protos, SyncProtoSchemas target
    protos/
      order_event.proto     # value: главный файл, import "common/money.proto"
      order_key.proto       # key: protobuf-ключ сообщения
      common/money.proto    # кастомные общие типы (Money, OrderStatus)
  Sandbox.App/
    Features/
      PublishOrders/
        PublishOrdersWorker.cs
        OrderEventFactory.cs
        PublishOrdersOptions.cs
        PublishOrdersSlice.cs        # DI-регистрация среза
      ReadAggregates/
        ReadAggregatesWorker.cs
        OrderAggregateRow.cs
        ReadAggregatesOptions.cs
        ReadAggregatesSlice.cs
    Common/
      KafkaClientFactory.cs
      ClickHouseConnectionFactory.cs
      StartupRetry.cs
      CommonSlice.cs
    Program.cs              # composition root
    appsettings.json
docker/
  clickhouse/init/01_schema.sql
  clickhouse/format_schemas/           # копия всего дерева protos/
    order_event.proto                  # (относительные пути сохраняются,
    common/money.proto                 #  иначе import не разрешится)
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
| `kafka` | **`confluentinc/cp-kafka:7.8.x`** (основной; совместим с SR из того же стека) | single-node KRaft; listener'ы: `kafka:9092` внутри сети, `localhost:29092` наружу |
| `schema-registry` | `confluentinc/cp-schema-registry:7.8.x` | :8081; `depends_on: kafka (condition: service_healthy)` |
| `clickhouse` | `clickhouse/clickhouse-server:26.x` (минимум **26.6+**, pin на конкретный патч в `.env`) | init → `/docker-entrypoint-initdb.d`, format_schemas → `/var/lib/clickhouse/format_schemas` |
| `sandbox-app` | build из `Dockerfile` | `depends_on` на healthy kafka / schema-registry / clickhouse; конфигурация через env |
| `kafka-init` | `confluentinc/cp-kafka:7.8.x` (one-shot) | создаёт топик `orders --partitions 1 --replication-factor 1`; `depends_on: kafka (healthy)` |
| `kafka-ui` (опционально) | `provectuslabs/kafka-ui` | визуальная проверка топика и схемы в SR |

### Kafka listeners (критично для Docker)

Самая частая причина падения стенда — некорректные **advertised listeners**: ClickHouse и .NET producer внутри compose-сети должны резолвить `kafka:9092`, хост — `localhost:29092`. Без dual-listener конфигурации клиенты получают `localhost:9092` из metadata и падают с connection refused.

```yaml
# confluentinc/cp-kafka:7.8.x
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Producer и ClickHouse (`kafka_broker_list = 'kafka:9092'`) используют internal listener `PLAINTEXT`. Отладка с хоста — `--bootstrap-server localhost:29092`.

### Healthcheck-и

| Сервис | Проверка |
|---|---|
| `kafka` | `kafka-broker-api-versions --bootstrap-server localhost:9092` |
| `schema-registry` | `curl -f http://localhost:8081/subjects` |
| `clickhouse` | `clickhouse-client --query "SELECT 1"` |

`sandbox-app` стартует только после `condition: service_healthy` у kafka, schema-registry и clickhouse.

### kafka-init (one-shot)

```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.8.x
  depends_on:
    kafka:
      condition: service_healthy
  entrypoint: ["/bin/sh", "-c"]
  command:
    - |
      kafka-topics --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic orders --partitions 1 --replication-factor 1
  restart: "no"
```

### ClickHouse init

Скрипты в `/docker-entrypoint-initdb.d` выполняются **только при пустом volume**. Для пересоздания схемы: `docker compose down -v && docker compose up`.

## Этапы реализации

1. **Каркас и контракты**: solution, `Sandbox.Contracts` с деревом `protos/` (главный файл + импортируемый `common/money.proto`) и codegen через ProtoRoot; MSBuild target `SyncProtoSchemas` копирует `protos/` → `docker/clickhouse/format_schemas/` при сборке.
2. **Инфраструктура**: docker-compose (`cp-kafka:7.8.x` + SR + clickhouse >= 26.6) с dual-listener Kafka, healthcheck-ами и `kafka-init`; init-SQL и format schemas. Smoke: `docker compose up`, все healthcheck-и green; вручную произвести сообщение `kcat`-ом невозможно (protobuf) — проверяем готовность сервисов.
3. **Срез PublishOrders**: каркас `Sandbox.App` (composition root, `Common/` с фабриками подключений), срез `PublishOrders` + Dockerfile; проверка — в SR зарегистрированы subjects `orders-key`, `orders-value` и reference-схема (зафиксировать фактическое имя subject), у `orders-value` заполнен блок `references` (`GET :8081/subjects`, `GET :8081/subjects/orders-value/versions/latest`), сообщения в топике (kafka-ui).
4. **Пайплайн ClickHouse**: убедиться, что `orders` наполняется и `orders_agg_1m` растёт; при ошибках парсинга смотреть `system.kafka_consumers` / лог clickhouse. **Smoke-check envelope** (до отладки DDL):
   ```bash
   # первые 6 байт value: 00 <schema_id_be_4_bytes> 00
   docker exec kafka kafka-console-consumer \
     --bootstrap-server kafka:9092 --topic orders \
     --from-beginning --max-messages 1 --property print.key=false | xxd
   ```
   Здесь же валидируется `skip_bytes = 6` и маппинг nested/enum; при провале enum — fallback `status Int32` + `toString()` в MV; при провале envelope — переключение на план Б.
5. **Срез ReadAggregates**: worker чтения агрегатов, вывод свода в лог.
6. **Полировка**: README с инструкцией запуска, проверочными запросами и зафиксированным именем reference-subject в SR; `.env` с версиями образов (CH >= 26.6, cp-kafka 7.8.x); e2e-прогон с нуля (`docker compose down -v && up`).

## Риски

- **`skip_bytes` и message-indexes**: длина конверта 6 байт гарантирована только для первого top-level message в главном файле — держим в нём один message (типы из импортов на индексацию не влияют); при эволюции схемы с несколькими типами конверт «поплывёт». Митигируется планом Б.
- **Расхождение путей импорта**: `import "common/money.proto"` должен разрешаться одинаково от ProtoRoot в codegen и от корня `format_schemas` в ClickHouse — дерево `protos/` копируется в `format_schemas` как есть, без переименований; иначе ClickHouse упадёт с ошибкой резолва схемы при создании Kafka-таблицы.
- **Маппинг вложенных типов в ClickHouse**: вложенный message читается в колонки с точкой в имени (`price.currency`), proto enum — в Enum8 по именам значений; проверяется на этапе 4 вместе со `skip_bytes`. Fallback: `status Int32` + `toString()` в MV.
- **`Money.amount` как double/Float64**: для демо допустимо; в production — потеря точности, нужен Decimal или Int64 (копейки).
- **Версия ClickHouse**: `kafka_schema_registry_skip_bytes` появился в **25.11**; pin на конкретный патч в `.env` (рекомендуется 26.x); старые кэши образов (< 26.6) не подойдут.
- **Kafka advertised listeners в Docker**: без dual-listener конфигурации клиенты внутри compose-сети получают `localhost` из metadata и падают — см. раздел «Kafka listeners».
- **ClickHouse init одноразовый**: `/docker-entrypoint-initdb.d` только при пустом volume; для пересоздания схемы нужен `docker compose down -v`.
- **Drift protos**: без MSBuild target `SyncProtoSchemas` C# codegen и ClickHouse format schemas разойдутся.
- **Отставание видимости агрегатов**: Kafka engine флашит блоками (по `kafka_max_block_size`/таймауту ~стрим-флаш); в демо задержка в секунды — норма, отражаем в README.
- **Порядок старта контейнеров**: закрывается healthcheck-ами + `kafka-init` + ретраями в приложении.

## Критерии готовности демо

- `docker compose up -d --build` с чистого состояния поднимает весь стенд без ручных действий.
- В логах `sandbox-app` видно и публикацию событий, и периодический вывод поминутных агрегатов по категориям.
- `SELECT count() FROM orders` растёт; суммы в `orders_agg_1m` (через GROUP BY) сходятся с `orders`.
- В Schema Registry зарегистрированы схемы `orders-key`, `orders-value` и reference-схема импорта (фактическое имя subject задокументировано в README); связь value-схемы с импортом видна в `references`.
