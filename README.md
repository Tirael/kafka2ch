# kafka2ch

Демонстрационный стенд: **Kafka (Protobuf + Schema Registry) → ClickHouse → агрегаты**.

Sandbox-приложение на .NET 8 публикует события заказов и отгрузок в Kafka; ClickHouse читает топики через Kafka table engine, складывает сырые строки в MergeTree и агрегирует их materialized view. Background worker периодически читает агрегаты и пишет их в лог.

```
PublishOrders / PublishShipments  →  Kafka + Schema Registry
                                         ↓
                              ClickHouse (Kafka engine + MV)
                                         ↓
                              ReadAggregates (лог агрегатов)
```

Подробности архитектуры и решений — в [PLAN.md](PLAN.md).

## Требования

- [Docker](https://docs.docker.com/get-docker/) и Docker Compose v2
- (опционально, для локальной разработки) [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)

Образы и версии задаются в `.env` (см. ниже). ClickHouse должен быть **≥ 26.6** (`kafka_schema_registry_skip_bytes`).

## Настройка

1. Скопируйте пример окружения:

```bash
cp .env.example .env
```

2. При необходимости отредактируйте `.env`:

| Переменная | Назначение | Значение по умолчанию |
|---|---|---|
| `KAFKA_IMAGE` | брокер Kafka (KRaft) | `confluentinc/cp-kafka:7.8.0` |
| `SCHEMA_REGISTRY_IMAGE` | Confluent Schema Registry | `confluentinc/cp-schema-registry:7.8.0` |
| `CLICKHOUSE_IMAGE` | ClickHouse Server | `clickhouse/clickhouse-server:26.6.3.62` |
| `CLICKHOUSE_PASSWORD` | пароль пользователя `default` | `sandbox` |
| `KAFKA_UI_IMAGE` | Kafka UI | `provectuslabs/kafka-ui:v0.7.2` |

Пароль ClickHouse должен совпадать с `ClickHouse__Password` / секцией `ClickHouse` в приложении (в compose уже прокидывается из `.env`).

### Порты на хосте

| Порт | Сервис |
|---|---|
| `29092` | Kafka (доступ с хоста) |
| `8081` | Schema Registry |
| `8123` / `9000` | ClickHouse HTTP / native |
| `8080` | Kafka UI |

Внутри Docker-сети брокер доступен как `kafka:9092`, с хоста — `localhost:29092`.

### Конфигурация приложения

Основные секции — в `src/Sandbox.App/appsettings.json` (перекрываются переменными окружения):

| Секция / env | Описание |
|---|---|
| `Kafka__BootstrapServers` | брокеры Kafka |
| `Kafka__SchemaRegistryUrl` | URL Schema Registry |
| `PublishOrders__Topic` / `IntervalMs` | топик и интервал публикации заказов |
| `PublishShipments__Topic` / `IntervalMs` | топик и интервал публикации отгрузок |
| `ClickHouse__Host` / `Port` / `Password` / … | подключение к ClickHouse |
| `ReadAggregates__IntervalMs` / `WindowMinutes` | период и окно чтения агрегатов |

В docker-compose эти значения уже заданы для работы внутри сети (`kafka:9092`, `clickhouse:8123` и т.д.).

## Быстрый запуск (весь стенд в Docker)

```bash
cp .env.example .env
docker compose up -d --build
```

Compose поднимает: Kafka, Schema Registry, ClickHouse, one-shot `kafka-init` (топики `orders` и `shipments`), `sandbox-app`, Kafka UI.

Проверка статуса:

```bash
docker compose ps
docker compose logs -f sandbox-app
```

В логах `sandbox-app` должны появляться сообщения о публикации и периодический вывод агрегатов.

Остановка:

```bash
docker compose down
```

Полный сброс данных (включая volume ClickHouse — init SQL выполнится заново):

```bash
docker compose down -v
docker compose up -d --build
```

> Init-скрипты ClickHouse (`docker/clickhouse/init/`) выполняются **только при пустом volume**. После изменения конечной схемы на уже существующем volume используйте миграции (ниже), а не `down -v`, если данные нужно сохранить. Полный сброс: `docker compose down -v`.

## Миграции схемы

Эволюция proto / pipeline без потери данных в MergeTree. История — в таблице `schema_migrations`: стартовые init-скрипты пишутся с `kind = 'init'`, миграции — с `kind = 'migration'` (`SELECT * FROM schema_migrations ORDER BY kind, version`).

1. Измените `protos/` или `clickhouse.codegen.json`
2. `dotnet build src/Sandbox.Contracts` — упадёт при drift `schema.snapshot.json`
3. Сгенерируйте миграцию:

```bash
dotnet build tools/ClickHouseSchemaGen.Cli
dotnet exec tools/ClickHouseSchemaGen.Cli/bin/Debug/net8.0/ClickHouseSchemaGen.Cli.dll \
  migrate --config src/Sandbox.Contracts/clickhouse.codegen.json --name <change_name>
```

4. Закоммитьте SQL в `docker/clickhouse/migrations/`, обновлённый `schema.snapshot.json` и `00_schema_migrations.sql`
5. Apply: сервис `clickhouse-migrate` при `docker compose up`, либо вручную:

```bash
dotnet exec tools/ClickHouseSchemaGen.Migrator/bin/Debug/net8.0/ClickHouseSchemaGen.Migrator.dll \
  --migrations docker/clickhouse/migrations
```

В сырых таблицах `orders` / `shipments` сохраняются `kafka_key` (сырые байты protobuf-ключа) и `kafka_headers` (`Map(String, String)`). Подробности — [clickhouse.codegen.md](src/Sandbox.Contracts/clickhouse.codegen.md) и [MIGRATIONS_PLAN.md](MIGRATIONS_PLAN.md).

## Кластерный ClickHouse

Тот же стенд на кластере **2 шарда × 2 реплики + ClickHouse Keeper** (нужен Docker Compose ≥ 2.24.4):

```bash
docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d --build
./scripts/verify-cluster.sh
```

Overlay [`docker-compose.cluster.yml`](docker-compose.cluster.yml) превращает сервис `clickhouse` в шард 1 / реплику 1 (порты `8123`/`9000` те же, `sandbox-app` и запросы ниже работают без изменений) и добавляет `clickhouse-02..04`, `clickhouse-keeper`, one-shot `clickhouse-cluster-init` и топики на 4 партиции.

| Нода | Шард | Реплика |
|---|---|---|
| `clickhouse` | 01 | `clickhouse` |
| `clickhouse-02` | 01 | `clickhouse-02` |
| `clickhouse-03` | 02 | `clickhouse-03` |
| `clickhouse-04` | 02 | `clickhouse-04` |

Топология схемы (генерируется из [`clickhouse.codegen.cluster.json`](src/Sandbox.Contracts/clickhouse.codegen.cluster.json) в `docker/clickhouse-cluster/init/`):

- `orders_queue` / `shipments_queue` (classic `ENGINE = Kafka`) и их MV — на каждой ноде, с общим `kafka_group_name`: партиции топика распределяются между всеми 4 нодами;
- MV пишут в `orders_local` / `shipments_local` (`ReplicatedMergeTree`, путь `/clickhouse/tables/{shard}/{database}/{table}`) — данные шардируются по партициям Kafka и реплицируются внутри шарда;
- агрегаты `orders_agg_1m_local` / `shipments_agg_1m_local` — `ReplicatedSummingMergeTree`, их MV читают из `*_local`;
- `orders`, `shipments`, `orders_agg_1m`, `shipments_agg_1m` — `Distributed`-таблицы для чтения (и внешних вставок);
- `schema_migrations` — `ReplicatedMergeTree` без `{shard}` в пути: одинаковая история на всех нодах.

Init-скрипты применяются один раз сервисом `clickhouse-cluster-init` (`ON CLUSTER` с одной ноды), а не через `docker-entrypoint-initdb.d` каждой ноды. Миграции кластера лежат в `docker/clickhouse-cluster/migrations/` и применяются `clickhouse-migrate` с `ClickHouse__Cluster=kafka2ch`. Настройки `cluster` и ограничения — в [clickhouse.codegen.md](src/Sandbox.Contracts/clickhouse.codegen.md#cluster-кластерный-clickhouse).

Состояние кластера:

```bash
docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT hostName(), table, assignments.partition_id
FROM clusterAllReplicas('kafka2ch', system.kafka_consumers)"
```

Остановка: `docker compose -f docker-compose.yml -f docker-compose.cluster.yml down -v`.

## Локальный запуск приложения

Инфраструктура в Docker, приложение на хосте:

```bash
cp .env.example .env
docker compose up -d kafka schema-registry clickhouse kafka-init kafka-ui
```

Дождитесь healthy-статуса сервисов, затем:

```bash
dotnet run --project src/Sandbox.App
```

Переопределите endpoints под хост (значения по умолчанию рассчитаны на Docker-сеть):

```bash
export Kafka__BootstrapServers=localhost:29092
export Kafka__SchemaRegistryUrl=http://localhost:8081
export ClickHouse__Host=localhost
export ClickHouse__Password=sandbox

dotnet run --project src/Sandbox.App
```

Или передайте те же ключи через `appsettings.Development.json` / User Secrets.

## Проверка пайплайна

Скрипт smoke-проверки (стенд должен быть запущен):

```bash
./scripts/verify-pipeline.sh
```

Вручную:

```bash
# Schema Registry subjects
curl -s http://localhost:8081/subjects

# Число сырых заказов
docker exec clickhouse clickhouse-client --password sandbox \
  --query "SELECT count() FROM orders"

# Поминутные агрегаты (SummingMergeTree — всегда через GROUP BY / sum)
docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT minute, category,
       sum(orders_count) AS orders_count,
       sum(total_amount) AS total_amount,
       sum(total_qty)    AS total_qty
FROM orders_agg_1m
WHERE minute >= now() - INTERVAL 10 MINUTE
GROUP BY minute, category
ORDER BY minute DESC, category
LIMIT 20
"

# Состояние Kafka-консьюмеров ClickHouse
docker exec clickhouse clickhouse-client --password sandbox \
  --query "SELECT * FROM system.kafka_consumers FORMAT Vertical"
```

Kafka UI: http://localhost:8080

### Schema Registry

После первых сообщений ожидаются subjects примерно такого вида:

- `orders-key`, `orders-value` (+ reference на импортируемые proto, напр. `common/money.proto`)
- `shipments-key`, `shipments-value` (+ их references)

Проверка value-схемы и блока `references`:

```bash
curl -s http://localhost:8081/subjects/orders-value/versions/latest | jq .
```

### Задержка появления данных

Kafka engine в ClickHouse флашит блоками (размер блока / таймаут). В демо задержка в несколько секунд до появления строк в `orders` / `orders_agg_1m` — нормальное поведение.

## Сборка, codegen и тесты

Регенерация format schemas и DDL ClickHouse (из protobuf):

```bash
dotnet build src/Sandbox.Contracts
```

Результат:

- `docker/clickhouse/format_schemas/` — копия `protos/`
- `docker/clickhouse/init/01_orders_queue.sql`, `02_shipments_queue.sql`, `03_pipeline.sql`

Конфиг codegen: `src/Sandbox.Contracts/clickhouse.codegen.json`.  
Инструкция по заполнению: [`src/Sandbox.Contracts/clickhouse.codegen.md`](src/Sandbox.Contracts/clickhouse.codegen.md).  
Пропуск codegen при сборке приложения в Docker: `-p:SkipClickHouseCodegen=true` (уже в `Dockerfile`).

Тесты генератора схем (нужен Docker для integration):

```bash
dotnet test tests/ClickHouseSchemaGen.UnitTests
dotnet test tests/ClickHouseSchemaGen.IntegrationTests
```

## Полезные команды

```bash
# Логи приложения
docker compose logs -f sandbox-app

# Логи ClickHouse (ошибки парсинга Protobuf / Kafka)
docker compose logs -f clickhouse

# Пересборка только приложения
docker compose up -d --build sandbox-app

# Envelope Confluent Protobuf (ожидается: 00 + 4 байта schema id + 00)
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic orders \
  --from-beginning --max-messages 1 --property print.key=false | xxd | head -3
```

## Структура репозитория

```
src/Sandbox.App/          # worker: PublishOrders, PublishShipments, ReadAggregates
src/Sandbox.Contracts/    # protobuf + clickhouse.codegen.json
tools/ClickHouseSchemaGen # proto3 → ClickHouse DDL
docker/clickhouse/        # init SQL + format schemas
docker/clickhouse-cluster/ # init SQL, migrations и конфиги кластерного варианта
scripts/verify-pipeline.sh
scripts/verify-cluster.sh
docker-compose.yml
docker-compose.cluster.yml
Dockerfile
.env.example
```
