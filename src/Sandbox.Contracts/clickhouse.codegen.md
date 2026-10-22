# Инструкция: `clickhouse.codegen.json`

Конфиг генератора ClickHouse DDL из protobuf (`tools/ClickHouseSchemaGen` / NuGet `ClickHouseSchemaGen.Tasks`).  
Файл: `[clickhouse.codegen.json](clickhouse.codegen.json)` в этом каталоге.

После правок перегенерируйте SQL:

```bash
dotnet build src/Sandbox.Contracts
```

Результат - файлы в `docker/clickhouse/init/` (пути задаются в конфиге).  
Пропуск codegen: `dotnet build -p:SkipClickHouseCodegen=true`.

Вне этого репозитория подключайте пакет `ClickHouseSchemaGen.Tasks` (MSBuild) и/или tools `ClickHouseSchemaGen.Cli` / `ClickHouseSchemaGen.Migrator` — см. раздел **NuGet-пакеты** в [README](../../README.md). Для CLI передайте `--assemblies` с DLL, где лежат типы из `messageType`.

---

## Минимальный состав

Обязателен только непустой массив `kafkaTables`. Остальное опционально.

```json
{
  "kafkaTables": [
    {
      "messageType": "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
      "tableName": "orders_queue",
      "protoFile": "order_event",
      "messageName": "OrderEvent",
      "outputPath": "../../docker/clickhouse/init/01_orders_queue.sql"
    }
  ]
}
```

Без секции `kafka` подставляются значения по умолчанию модели (`brokerList: kafka:9092` при отсутствии `namedCollection`, `topic: orders`, `groupName: clickhouse-orders`, `skipBytes: 6` и т.д.). Для другого топика секцию `kafka` задайте явно.

---



## Секции конфига


| Секция           | Обязательна        | Назначение                                         |
| ---------------- | ------------------ | -------------------------------------------------- |
| `defaults`       | нет                | Глобальные правила маппинга proto -> ClickHouse     |
| `kafkaTables`    | **да** (не пустой) | Kafka Engine таблицы (по одной на message)         |
| `fieldOverrides` | нет                | Глобальные overrides полей (мержатся с табличными) |
| `pipeline`       | нет                | MergeTree + materialized views + агрегаты + произвольный SQL |
| `migrations`     | нет                | пути снапшота и каталога SQL-миграций              |
| `cluster`        | нет                | кластерный ClickHouse (выключен, пока нет `name`)  |
| `extends`        | нет                | базовый конфиг, поверх которого применяется этот   |
| `outputDirectory`| нет                | каталог для всех init-скриптов (имя файла из `outputPath`) |


Имена `tableName` и пути `outputPath` в `kafkaTables` должны быть уникальны.

---

## `persistKafkaMeta` / `includeKafkaMeta`

Сохраняет Kafka message key и headers в MergeTree (виртуальные колонки queue в DDL не объявляются):

- `kafka_key String` ← `_key` (сырые байты ключа)
- для `kafkaTables[].key.format = "protobuf"` — ещё `kafka_key.<поле>` ← `_key.<поле>` (типизированные поля ключа, см. секцию `key`)
- `kafka_headers Map(String, String)` ← `mapFromArrays(\`_headers.name\`, \`_headers.value\`)`

Включается в `defaults.persistKafkaMeta` и/или `includeKafkaMeta: true` на MergeTree / MV. Опционально: `topic`, `partition`, `offset`, `timestampMs`.

### Дедуп `orders` (at-least-once Kafka engine)

Kafka engine доставляет at-least-once: при сбое между INSERT и commit offset тот же блок вставится повторно. Демо-конфиг сохраняет `partition` / `offset` (`kafka_partition`, `kafka_offset UInt64`) и хранит `orders` в `ReplacingMergeTree` с

```
ORDER BY (event_time, order_id, kafka_partition, kafka_offset)
```

- **Ключ дедупа — идентичность сообщения `(kafka_partition, kafka_offset)`** (топик один). `ReplacingMergeTree` схлопывает строки с одинаковым **полным** ключом сортировки; повторная доставка того же сообщения даёт идентичные `event_time` / `order_id`, поэтому префикс `(event_time, order_id)` дедупу не мешает и сохраняет индекс для запросов по времени. Повторная **публикация** продьюсером (новый offset) — другое сообщение и не схлопывается; дедуп по бизнес-ключу требует версии события и `ORDER BY order_id`.
- Схлопывание происходит при слияниях, поэтому точные чтения — `SELECT … FROM orders FINAL`; `count() FROM orders FINAL = uniqExact(kafka_partition, kafka_offset)`.
- Строки, перенесённые миграцией из старой `MergeTree`, получают `kafka_partition = kafka_offset = 0` и схлопываются по `(event_time, order_id)`.

**Агрегаты `orders_agg_1m`** (объявлены в [`pipeline.aggregates[]`](#aggregates)) — refreshable MV `REFRESH EVERY 1 MINUTE` с **полным пересчётом** `FROM orders FINAL` в обычную `MergeTree` (каждый refresh атомарно подменяет содержимое). Insert-триггерный `SummingMergeTree` суммировал бы каждую повторную вставку, а дедуп `orders` случается позже, при слиянии. Скользящее окно не выбрано: refresh без `APPEND` заменяет всю таблицу, поэтому окно стёрло бы старые минуты, а `APPEND` + окно снова требует дедупа цели. Цена: агрегаты отстают до минуты, каждый refresh читает весь `orders` (для демо приемлемо; для больших объёмов — партиционирование и пересчёт только свежих партиций). `ReadAggregates` читает `sum(...) GROUP BY minute, category` (в кластере строки шардов складываются) и логирует `system.view_refreshes.last_success_time`. Принудительно: `SYSTEM REFRESH VIEW orders_agg_mv; SYSTEM WAIT VIEW orders_agg_mv`.

Смена движка/ключа — manual-миграция `*_dedup_orders_by_kafka_offset.sql` (по одной на топологию): DETACH очереди → копия `orders` в новую `ReplacingMergeTree`-таблицу (в кластере через временную `Distributed`) → `EXCHANGE TABLES` → пересоздание `orders_agg_1m` / `orders_agg_mv` → ATTACH очереди и новый `orders_mv` с `_partition` / `_offset`.

## Миграции схемы

Секция `migrations` (пути по умолчанию относительно codegen.json):

- `snapshotPath` → `schema.snapshot.json`
- `migrationsDirectory` → `docker/clickhouse/migrations`
- `versionsOutputPath` → `00_schema_migrations.sql`

`00_schema_migrations.sql` создаёт таблицу `schema_migrations` и записывает baseline-миграции (`kind = 'migration'`). Префикс `00` зарезервирован: файл обязан идти раньше любого init-скрипта (валидатор конфига падает иначе), потому что каждый сгенерированный init-скрипт (`01_*`, `02_*`, …) в конце записывает себя строкой `kind = 'init'` (version — префикс файла, checksum — SHA-256 тела скрипта без этой строки). Сама таблица версий не числится миграцией: её создаёт и обновляет (`ALTER … ADD COLUMN IF NOT EXISTS kind`) Migrator до применения миграций, по аналогии с `VersionInfo` в FluentMigrator. Migrator учитывает только `kind = 'migration'`.

Workflow: правка proto → build падает на drift → `Cli migrate --name …` → review/commit → apply через `clickhouse-migrate` / Migrator. Только BACKWARD-compatible эволюция proto; `SkipClickHouseSnapshotCheck=true` — escape hatch.

Формат снапшота — `version: 2`: кроме v1-полей хранит `cluster`, `kafkaTables[].key` (`format`, `messageType`, `skipBytes`), per-table `shardingKey` / `materializedViewsWriteThroughDistributed` и `aggregates` (разрешённые типы колонок; поле опускается, если агрегатов нет). Снапшот `version: 1` читается с апгрейдом в памяти: недостающие поля берутся из текущего конфига (считается, что топология и ключи с момента v1 не менялись). `Cli migrate snapshot-upgrade --config … --assemblies …` переписывает файл в v2 без изменения схемы и без новой миграции — сделайте это **до** смены топологии или ключа, иначе изменение не будет замечено. Смена `key` (формат, тип, `skipBytes`) — пересоздание Kafka-таблицы и её MV (автоматически).

---

## `cluster` (кластерный ClickHouse)

По умолчанию выключен: пока `cluster.name` пустой, DDL и миграции генерируются ровно как для одной ноды. С `name` включается кластерный режим.

| Поле | По умолчанию | Описание |
| --- | --- | --- |
| `name` | — | имя кластера из `remote_servers`; для `Distributed(...)` и (в режиме `onCluster`) `ON CLUSTER` |
| `ddlMode` | `onCluster` | `onCluster` — каждый DDL с `ON CLUSTER`; `replicatedDatabase` — `CREATE DATABASE … ENGINE = Replicated(...)`, дальше DDL без `ON CLUSTER` (реплицируется движком БД) |
| `replicatedPath` | `/clickhouse/tables/{shard}/{database}/{table}` | Keeper-путь `Replicated*MergeTree` (`{table}` или `{uuid}`) |
| `replicaName` | `{replica}` | имя реплики storage-таблиц |
| `historyReplicatedPath` | `/clickhouse/tables/all/{database}/{table}` | Keeper-путь `schema_migrations`; **без** `{shard}` |
| `historyReplicaName` | `{shard}-{replica}` | имя реплики `schema_migrations` |
| `localTableSuffix` | `_local` | суффикс per-node storage-таблиц |
| `shardingKey` | `rand()` | ключ `Distributed` (переопределяется `mergeTreeTables[].shardingKey`) |
| `materializedViewsWriteThroughDistributed` | `false` | MV пишут в `Distributed` `t` (шардирование по `shardingKey`); по умолчанию пишут в `t_local`. Override: `mergeTreeTables[].materializedViewsWriteThroughDistributed` |
| `replicatedDatabaseName` | `kafka2ch` | только `ddlMode=replicatedDatabase` |
| `replicatedDatabasePath` | `/clickhouse/databases/{uuid}` | Keeper-путь Replicated database; `{uuid}` одинаков на всех нодах, т.к. БД создаётся одним `CREATE DATABASE … ON CLUSTER` |
| `replicatedDatabaseShardName` | `{shard}` | shard name Replicated database (2-й аргумент `Replicated`); реплики одного шарда должны иметь одинаковое значение |
| `replicatedDatabaseReplicaName` | `{replica}` | replica name Replicated database (3-й аргумент); уникален в пределах шарда |

Макросы `{shard}` / `{replica}` — в `<macros>` каждой ноды. Для production Keeper — ансамбль из **не менее 3** узлов (в compose-демо так и сделано).

**Цель MV (default = `_local`):** Kafka→raw MV на каждой ноде пишет в свой шард (`TO t_local`). Опция `materializedViewsWriteThroughDistributed` переключает `TO t`, чтобы строки перешардировались по `shardingKey` (дороже; нужно, если ключ шардирования важнее привязки к партиции Kafka). Refreshable MV агрегатов (`aggregates[]` и refreshable MV в `trailingSql`): в `onCluster` всегда `TO *_local` (даже при write-through); в `replicatedDatabase` — `default.*` с `ON CLUSTER`; источник — см. [`aggregates[]`](#aggregates).

**Trade-off размещения (R15 / TP-8).** Write-through пишет в `Distributed` асинхронно: Kafka engine коммитит offset, когда блок принят в очередь `Distributed`, а не доставлен на шард, — сбой ноды между этими моментами теряет строки. Поэтому демо-конфиги (включая `replicatedDatabase`) пишут в `*_local`: offset коммитится только после записи в локальную реплицированную таблицу. Цена — шард строки определяется тем, какая нода получила партицию Kafka, а не `shardingKey`: `Distributed` `orders` с `cityHash64(order_id)` в этом режиме только читает. Следствие для дедупа: повторная доставка после ребаланса consumer group может попасть на другой шард, а `ReplacingMergeTree`/`FINAL` схлопывает дубли только внутри шарда — такие дубли ловит `scripts/verify-cluster.sh` (`count() FROM orders FINAL` ≠ `uniqExact(order_id)` → fail). Если важнее перешардирование по ключу, включите write-through **и** `distributed_foreground_insert = 1` в профиле пользователя Kafka-MV (синхронная доставка, медленнее consume).

**Refreshable-агрегаты в `replicatedDatabase` (R15 / TP-8).** Refreshable MV и её цель создаются не в Replicated database, а в Atomic `default` с `ON CLUSTER`: `default.orders_agg_1m` — обычная `MergeTree`, `default.orders_agg_mv` читает глобальный `Distributed` `kafka2ch.orders FINAL`, каждая нода обновляется независимо и хранит полный агрегат кластера. Цена: каждый refresh — полный `orders FINAL` через `Distributed` на **каждой** из N нод (N× нагрузка на чтение; в compose N = 4) и N копий агрегата. Почему не внутри Replicated database: там refresh координируется по шардам (`default_replica_path` = `/clickhouse/tables/{uuid}/{shard}`), а refresh без `APPEND` подменяет цель DDL-операцией, которая реплицируется на **всю** базу. При реальных шардах (`replicatedDatabaseShardName = {shard}`) refresh каждого шарда заменяет цель и на чужих шардах: UUID таблиц расходятся, `SYSTEM WAIT VIEW` падает с `TABLE_UUID_MISMATCH` ([run 39680979057](https://github.com/Tirael/kafka2ch/actions/runs/39680979057), [run 39681723018](https://github.com/Tirael/kafka2ch/actions/runs/39681723018)); с одним именем шарда та же подмена оставляла данные только на шарде refresh-реплики. Для больших объёмов — заменить полный пересчёт на пересчёт свежих партиций или вынести агрегат в отдельный сервис. Подробности — [`aggregates[]`](#aggregates).

**`ddlMode=replicatedDatabase`:** `CREATE DATABASE … ON CLUSTER … ENGINE = Replicated('<replicatedDatabasePath>', '{shard}', '{replica}')` (топология БД совпадает с `remote_servers`: 2 шарда × 2 реплики в compose), затем `schema_migrations` в `default` с `ON CLUSTER` и общим Keeper-путём (история на всех шардах), `USE <replicatedDatabaseName>`; таблицы/MV — `Replicated*MergeTree` без аргументов (путь задаёт Replicated DB), без `ON CLUSTER`. `DETACH` очередей — `PERMANENTLY`. Migrator: `--ddl-mode replicatedDatabase` / `ClickHouse__DdlMode`, `ClickHouse__Database=<replicatedDatabaseName>`. Пример: [`clickhouse.codegen.cluster.replicated-db.json`](clickhouse.codegen.cluster.replicated-db.json); compose: `-f docker-compose.cluster.yml -f docker-compose.cluster.replicated-db.yml`.

**UDF `protobufWire*` и кластер.** SQL UDF не принадлежат БД, поэтому движок Replicated database их не реплицирует. В `onCluster` функции создаются с `ON CLUSTER`. В `replicatedDatabase` они создаются без `ON CLUSTER` (только на ноде, где выполнен init/миграция), поэтому на **всех** нодах обязателен `<user_defined_zookeeper_path>` (общий Keeper-путь UDF), иначе на остальных репликах функций нет. В compose его монтирует только overlay replicated-db ([`user-defined-replicated.xml`](../../docker/clickhouse-cluster/config/user-defined-replicated.xml)). Для `onCluster` этот параметр **не** задавайте: с реплицируемым хранилищем UDF ClickHouse отклоняет `CREATE FUNCTION … ON CLUSTER` (`ON CLUSTER is not allowed because user-defined functions are replicated automatically`).

Снапшот (`version: 2`) хранит секцию `cluster` (`name`, `ddlMode`, `localTableSuffix`, `shardingKey`, `materializedViewsWriteThroughDistributed`; `null` — single-node) и per-table `shardingKey` / `materializedViewsWriteThroughDistributed` у `mergeTreeTables`. Их смена — drift на build и **manual** `ClusterTopologyChanged` в `migrate`: генератор не умеет переносить данные между топологиями, миграция содержит только комментарий «нужна миграция данных/топологии». Пути Keeper и параметры Replicated database в снапшот не входят. Один проект на single-node и cluster → **отдельные** `migrations` / snapshot / `00_*.sql` на конфиг; генерация — одним `Cli migrate --all-topologies clickhouse.topologies.json --name …` (одна версия во все каталоги, snapshot'ы атомарно), apply — **отдельно для каждого** каталога.

### `extends` / `outputDirectory`

`extends` — путь к базовому конфигу (относительно текущего файла). Объекты сливаются рекурсивно, массивы и скаляры заменяются целиком. Относительные пути итогового конфига разрешаются от файла, переданного в `--config`. `outputDirectory` перенаправляет все init-скрипты (`kafkaTables[].outputPath`, `pipeline.outputPath`) в один каталог, сохраняя имена файлов.

Кластерный вариант стенда — [`clickhouse.codegen.cluster.json`](clickhouse.codegen.cluster.json):

```json
{
  "extends": "clickhouse.codegen.json",
  "outputDirectory": "../../docker/clickhouse-cluster/init",
  "cluster": { "name": "kafka2ch" },
  "migrations": {
    "snapshotPath": "../../docker/clickhouse-cluster/init/schema.snapshot.json",
    "migrationsDirectory": "../../docker/clickhouse-cluster/migrations",
    "versionsOutputPath": "../../docker/clickhouse-cluster/init/00_schema_migrations.sql"
  }
}
```

`dotnet build src/Sandbox.Contracts` генерирует single-node, cluster (`onCluster`) и cluster (`replicatedDatabase`) варианты. Миграция: `Cli migrate --all-topologies clickhouse.topologies.json --name …` — сразу для всех конфигов из [`clickhouse.topologies.json`](clickhouse.topologies.json) (или `--config a.json --config b.json …`); `Cli migrate verify --all-topologies clickhouse.topologies.json` проверяет, что версии после `migrationsBaseline` совпадают во всех каталогах. Apply: `Migrator --migrations <dir> --cluster kafka2ch [--ddl-mode onCluster|replicatedDatabase]` (env `ClickHouse__Cluster` / `ClickHouse__DdlMode` / `ClickHouse__Database`).

---



## `defaults`


| Поле                      | Тип       | По умолчанию | Описание                                                              |
| ------------------------- | --------- | ------------ | --------------------------------------------------------------------- |
| `maxFlattenDepth`         | int (> 0) | `3`          | Глубина flatten вложенных `message`                                   |
| `repeatedMessageStrategy` | string    | `"nested"`   | Стратегия для `repeated message`: `nested` | `arraytuple` | `flatten` |
| `optionalAsNullable`      | bool      | `true`       | `optional T` -> `Nullable(T)`                                          |
| `oneofPresence`           | bool      | `true`       | Колонка присутствия oneof (нужна для ProtobufSingle)                  |
| `enumMaxValuesForEnum8`   | int (> 0) | `127`        | Порог: меньше/равно -> `Enum8`, иначе `Enum16`                         |


Пример:

```json
"defaults": {
  "maxFlattenDepth": 3,
  "repeatedMessageStrategy": "nested",
  "optionalAsNullable": true,
  "oneofPresence": true,
  "enumMaxValuesForEnum8": 127
}
```



### Маппинг proto -> ClickHouse (по умолчанию)


| Proto3                      | ClickHouse                             |
| --------------------------- | -------------------------------------- |
| scalar                      | `String` / `Int32` / …                 |
| `optional T`                | `Nullable(T)`                          |
| `enum`                      | `Enum8` / `Enum16`                     |
| nested `message`            | плоские колонки ``parent.field``       |
| `repeated` scalar/enum      | `Array(T)`                             |
| `repeated` message          | `Nested(...)` (`flatten_nested = 0`)   |
| `map<K,V>`                  | `Map(K,V)`                             |
| `oneof`                     | ветки + `{oneofName} Enum8`            |
| `google.protobuf.Timestamp` | ``field.seconds``, ``field.nanos``     |
| `google.protobuf.*Value`    | `Nullable(T)`                          |
| `Struct` / `Any`            | `String` (JSON; лучше задать override) |


---



## `kafkaTables[]`

Одна запись = одна таблица `ENGINE = Kafka` и один выходной `.sql`.

### Обязательные поля


| Поле          | Пример                                               | Правила                                            |
| ------------- | ---------------------------------------------------- | -------------------------------------------------- |
| `messageType` | `"Sandbox.Contracts.OrderEvent, Sandbox.Contracts"`  | CLR-тип: `FullName, Assembly`                      |
| `tableName`   | `"orders_queue"`                                     | Идентификатор ClickHouse: `[a-zA-Z_][a-zA-Z0-9_]*` |
| `protoFile`   | `"order_event"`                                      | Имя `.proto` **без** пути и расширения             |
| `messageName` | `"OrderEvent"`                                       | Имя message в proto                                |
| `outputPath`  | `"../../docker/clickhouse/init/01_orders_queue.sql"` | Путь к генерируемому SQL (относительно этого JSON) |




### `kafka` (опционально)


| Поле                            | По умолчанию          | Описание                                                              |
| ------------------------------- | --------------------- | --------------------------------------------------------------------- |
| `brokerList`                    | `"kafka:9092"`        | Брокеры Kafka (`kafka_broker_list` в DDL); взаимоисключается с `namedCollection` |
| `namedCollection`               | —                     | Имя named collection ClickHouse с брокерами и секретами: `ENGINE = Kafka(<имя>)` без `kafka_broker_list` |
| `topic`                         | `"orders"`            | Топик                                                                 |
| `groupName`                     | `"clickhouse-orders"` | Consumer group                                                        |
| `skipBytes`                     | `6`                   | Пропуск Confluent Protobuf envelope (`00` + 4 байта schema id + `00`) |
| `numConsumers`                  | `1`                   | Число consumers (> 0)                                                 |
| `maxBlockSize`                  | —                     | `kafka_max_block_size` (1…10 000 000): строк в блоке до flush в MV    |
| `flushIntervalMs`               | —                     | `kafka_flush_interval_ms` (100…600 000): таймаут flush неполного блока |
| `pollMaxBatchSize`              | —                     | `kafka_poll_max_batch_size` (1…10 000 000, ≤ `maxBlockSize`)          |
| `threadPerConsumer`             | —                     | `kafka_thread_per_consumer` (`true` → `1`): свой поток и flush на consumer |
| `flattenNested`                 | `false`               | `flatten_nested` в ClickHouse                                         |
| `protobufOneofPresence`         | `true`                | `input_format_protobuf_oneof_presence`                                |
| `protobufFlattenGoogleWrappers` | `true`                | Flatten google wrappers                                               |
| `handleErrorMode`               | `"default"`           | `kafka_handle_error_mode`: `default` \| `stream` \| `dead_letter_queue` |
| `skipBrokenMessages`            | —                     | `kafka_skip_broken_messages` (≥ 0), только при `handleErrorMode: default` |

`handleErrorMode` и `skipBrokenMessages` попадают в `SETTINGS` только при значениях не по умолчанию. Их изменение пересоздаёт queue (`KafkaTableChanged`).

`maxBlockSize`, `flushIntervalMs`, `pollMaxBatchSize`, `threadPerConsumer` попадают в `SETTINGS` только если заданы (иначе действуют дефолты ClickHouse: `max_insert_block_size`, `stream_flush_interval_ms`, `max_block_size`, `0`), сохраняются в `schema.snapshot.json` и при изменении пересоздают queue (`KafkaTableChanged`). Как их подбирать — [docs/load-testing.md](../../docs/load-testing.md).

#### `namedCollection` (брокеры и секреты вне DDL)

Демо-конфиг использует `"namedCollection": "kafka_orders"`. Генератор пишет:

```sql
ENGINE = Kafka(kafka_orders)
SETTINGS
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'ProtobufSingle',
    ...
```

- В DDL, миграциях и `schema.snapshot.json` нет брокеров и секретов; snapshot хранит только имя коллекции (`"namedCollection": "kafka_orders"`, без `brokerList`). Смена брокеров/паролей внутри коллекции не порождает diff и миграцию (нужен рестарт/пересоздание consumer'ов ClickHouse, а не изменение схемы). Смена имени коллекции или переход `brokerList` ↔ `namedCollection` — `KafkaTableChanged` (автоматическое пересоздание queue, offsets группы сохраняются).
- `brokerList` и `namedCollection` взаимоисключаются (ошибка валидации). Без обоих — `kafka_broker_list = 'kafka:9092'` (как раньше).
- Коллекция должна существовать до `CREATE TABLE` на **каждой** ноде, где создаётся queue (`ON CLUSTER` / Replicated database): иначе `There is no named collection`. В compose её задаёт [`docker/clickhouse/config/kafka.xml`](../../docker/clickhouse/config/kafka.xml) (монтируется на все ноды), брокеры — из env `KAFKA_BROKER_LIST` (`from_env`, по умолчанию `kafka:9092`).
- Пользователю, выполняющему init/миграции, нужен грант `NAMED COLLECTION` на коллекцию. Пользователи из `users.xml` его не имеют без `<named_collection_control>1</named_collection_control>` ([`named-collections-users.xml`](../../docker/clickhouse/config/named-collections-users.xml) для `default` в compose); для SQL-пользователей — `GRANT NAMED COLLECTION ON kafka_orders TO migrator`. `show_named_collections_secrets` оставьте `0`.
- Precedence в ClickHouse 26.6: значения из `SETTINGS` перекрывают одноимённые ключи коллекции (`loadFromNamedCollection`, затем `loadFromQuery`). Держите в коллекции только подключение (`kafka_broker_list`, `kafka_security_protocol`, `kafka_sasl_*`, `<kafka>`), а `kafka_topic_list` / `kafka_group_name` / `kafka_format` / `kafka_schema` / `kafka_num_consumers` / `kafka_handle_error_mode` — в codegen (они в snapshot и диффятся). Ключи схемы, положенные в коллекцию, генератор не видит.
- Глобальная секция `<kafka>` сервера **не** мержится для таблиц на named collection: librdkafka-настройки (`session_timeout_ms`, `heartbeat_interval_ms`, TLS, OAUTHBEARER) кладите во вложенный `<kafka>` коллекции (точки → `_`).

SASL/TLS (пример для production; значения — из env/секретов, в git не коммитить):

```xml
<clickhouse>
    <named_collections>
        <kafka_orders>
            <kafka_broker_list from_env="KAFKA_BROKER_LIST"/>
            <kafka_security_protocol>SASL_SSL</kafka_security_protocol>
            <kafka_sasl_mechanism>SCRAM-SHA-512</kafka_sasl_mechanism>
            <kafka_sasl_username from_env="KAFKA_SASL_USERNAME"/>
            <kafka_sasl_password from_env="KAFKA_SASL_PASSWORD"/>
            <kafka>
                <ssl_ca_location>/etc/clickhouse-server/kafka/ca.pem</ssl_ca_location>
                <ssl_certificate_location>/etc/clickhouse-server/kafka/client.pem</ssl_certificate_location>
                <ssl_key_location>/etc/clickhouse-server/kafka/client.key</ssl_key_location>
                <ssl_endpoint_identification_algorithm>https</ssl_endpoint_identification_algorithm>
                <session_timeout_ms>60000</session_timeout_ms>
                <heartbeat_interval_ms>10000</heartbeat_interval_ms>
            </kafka>
        </kafka_orders>
    </named_collections>
</clickhouse>
```

`kafka_security_protocol`: `PLAINTEXT` \| `SSL` \| `SASL_PLAINTEXT` \| `SASL_SSL`; `kafka_sasl_mechanism`: `PLAIN` \| `SCRAM-SHA-256` \| `SCRAM-SHA-512` \| `GSSAPI` \| `OAUTHBEARER` (для OIDC — `sasl_oauthbearer_*` во вложенном `<kafka>`). Mutual TLS без SASL: `kafka_security_protocol = SSL` + `ssl_*`. При незаданной переменной `from_env` ClickHouse лишь пишет warning `Env variable is not set` и оставляет ключ пустым (queue не подключится), поэтому compose всегда передаёт `KAFKA_BROKER_LIST`.

При `handleErrorMode: "stream"` битое сообщение не останавливает потребление: генератор добавляет в `pipeline` таблицу `{tableName}_errors` (MergeTree: `_topic`, `_partition`, `_offset`, `_raw_message`, `_error`, `received_at`; в кластере — `*_local` + `Distributed`) и view `{tableName}_errors_mv … WHERE length(_error) > 0` (в кластере пишет в `*_local`), а все остальные view этой queue получают `WHERE length(_error) = 0`. Для `stream` нужна секция `pipeline`; имена `{tableName}_errors` / `{tableName}_errors_mv` (и `{tableName}_errors_local` в кластере) не должны быть заняты.


Пример с `kafka` и overrides:

```json
{
  "messageType": "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
  "tableName": "orders_queue",
  "protoFile": "order_event",
  "messageName": "OrderEvent",
  "outputPath": "../../docker/clickhouse/init/01_orders_queue.sql",
  "kafka": {
    "brokerList": "kafka:9092",
    "topic": "orders",
    "groupName": "clickhouse-orders",
    "skipBytes": 6,
    "numConsumers": 1,
    "flattenNested": false,
    "protobufOneofPresence": true,
    "protobufFlattenGoogleWrappers": true
  },
  "fieldOverrides": {
    "status": { "enum8": true },
    "price.currency": { "type": "LowCardinality(String)" }
  }
}
```

### `key` (опционально)

Формат ключа Kafka-сообщения. По умолчанию ключ — строка: в MV он доступен как виртуальная колонка `_key` (`String`), например `{ "source": "_key", "target": "message_key" }`.

Если ключ сериализован protobuf (отдельный `.proto`, независимый от value), задайте `format: "protobuf"`:

| Поле             | По умолчанию        | Описание                                                                                 |
| ---------------- | ------------------- | ---------------------------------------------------------------------------------------- |
| `format`         | `"string"`          | `string` \| `protobuf`                                                                   |
| `messageType`    | —                   | CLR-тип ключа `FullName, Assembly`. Обязателен для `protobuf`                            |
| `skipBytes`      | `kafka.skipBytes`   | Сколько байт пропустить перед payload ключа (Confluent envelope: `6`, «голый» protobuf: `0`) |
| `fieldOverrides` | `{}`                | Overrides полей ключа (те же свойства, что у value; пути — без префикса `_key.`)          |

```json
{
  "messageType": "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
  "tableName": "orders_queue",
  "protoFile": "order_event",
  "messageName": "OrderEvent",
  "outputPath": "../../docker/clickhouse/init/01_orders_queue.sql",
  "key": {
    "format": "protobuf",
    "messageType": "Sandbox.Contracts.OrderKey, Sandbox.Contracts",
    "skipBytes": 6,
    "fieldOverrides": {
      "order_id": { "type": "LowCardinality(String)" }
    }
  }
}
```

Kafka engine в ClickHouse парсит только value; ключ всегда приходит сырыми байтами в `_key`, а функции разбора protobuf для отдельной колонки в ClickHouse нет. Поэтому генератор:

- маппит поля ключа по тем же правилам, что и value (типы, `optional` -> `Nullable`, enum, flatten вложенных message, `Timestamp` -> `.seconds`/`.nanos`, wrappers, overrides) в колонки `_key.<путь поля>`: `_key.order_id`, `_key.scope.level`;
- в Kafka-таблицу колонки ключа не попадают (DDL queue не меняется);
- в MV колонки ключа используются в `source` и `expression` как обычные колонки queue; генератор подставляет вместо них выражение разбора `CAST(<decode over substring(_key, skipBytes + 1)> AS <тип>)`:

```json
{ "source": "_key.order_id", "target": "order_id" },
{ "source": "_key.scope.level", "target": "scope_level", "expression": "`_key.scope.level` * 10" }
```

- SQL UDF `protobufWire*` (разбор wire format, `CREATE OR REPLACE FUNCTION`) пишутся в pipeline-скрипт перед MV и в миграции перед пересоздаваемыми MV, если MV их использует;
- при `persistKafkaMeta.key` / `includeKafkaMeta` рядом с сырым `kafka_key String` добавляются типизированные колонки `kafka_key.<путь поля>` (`kafka_key.order_id`, `kafka_key.scope.level`) с маппингом из `_key.<путь поля>`; их FieldNumberPath — `kafka:_key:<номера полей>`, поэтому снапшот/миграции отслеживают их так же, как поля value.

Ограничения protobuf-ключа: поддерживаются singular scalar / enum / nested message / `google.protobuf.*Value`; `repeated`, `map`, `oneof`, `Struct`/`Any` и стратегии `Tuple`/`JsonFallback` дают ошибку генерации. Неизвестный enum-номер в ключе роняет вставку MV (как и для value в `ProtobufSingle`). ClickHouse сохраняет MV с уже развёрнутыми UDF, так что после создания MV от функций не зависит.

---



## `fieldOverrides`

Ключ - путь поля в proto (точки для вложенности):

- `status`
- `destination.country`
- `status_history`

Формат пути: `[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*`.


| Свойство   | Тип       | Описание                                                  |
| ---------- | --------- | --------------------------------------------------------- |
| `type`     | string    | Явный тип ClickHouse, например `"LowCardinality(String)"` |
| `enum8`    | bool      | Принудительно `Enum8`                                     |
| `strategy` | string    | Имя стратегии маппинга (имя enum `MappingStrategy`)       |
| `maxDepth` | int (> 0) | Локальный лимит flatten                                   |
| `nullable` | bool      | Принудительная nullability                                |


Overrides на корне конфига и в таблице **мержатся** (табличные перекрывают одноимённые корневые).

Примеры:

```json
"fieldOverrides": {
  "category": { "type": "LowCardinality(String)" },
  "status": { "enum8": true },
  "tags": { "type": "Array(LowCardinality(String))" },
  "destination.city": { "type": "LowCardinality(String)" }
}
```

---



## `pipeline` (опционально)

Генерирует один SQL-файл с MergeTree-таблицами, MV и произвольным хвостом.

```json
"pipeline": {
  "outputPath": "../../docker/clickhouse/init/02_pipeline.sql",
  "mergeTreeTables": [ ... ],
  "materializedViews": [ ... ],
  "aggregates": [ ... ],
  "trailingSql": "CREATE TABLE ..."
}
```



### `mergeTreeTables[]`


| Поле          | Описание                                                                                                                                                                           |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tableName`   | Имя MergeTree-таблицы                                                                                                                                                              |
| `orderBy`     | Выражение `ORDER BY`, например `"(event_time, order_id)"`                                                                                                                          |
| `engine`      | Опционально, по умолчанию `MergeTree`. Движок семейства MergeTree: `MergeTree`, `ReplacingMergeTree[(ver[, is_deleted])]`, `SummingMergeTree[((col, ...))]`, `AggregatingMergeTree`, `CollapsingMergeTree(sign)`. Без `Replicated`-префикса — см. ниже |
| `ttl`         | Опционально. Выражение table-level `TTL`, например `"event_time + INTERVAL 90 DAY"`. Должно ссылаться только на колонки этой таблицы (не на поля, которых нет в `columns`) |
| `shardingKey` | Опционально, только кластер. Ключ шардирования `Distributed`-таблицы (по умолчанию `cluster.shardingKey`)                                                                           |
| `sourceTable` | Опционально. Имя Kafka-таблицы (`kafkaTables[].tableName`) для автозаполнения колонок                                                                                              |
| `columns`     | Список `{ "name", "type" }`. **Пустой** -> взять все колонки из `sourceTable`                                                                                                       |


Пример с TTL и явным списком колонок:

```json
{
  "tableName": "orders",
  "orderBy": "(event_time, order_id)",
  "ttl": "event_time + INTERVAL 90 DAY",
  "columns": [
    { "name": "order_id", "type": "String" },
    { "name": "event_time", "type": "DateTime64(3)" }
  ]
}
```

Сгенерированный фрагмент:

```sql
ENGINE = MergeTree
ORDER BY (event_time, order_id)
TTL event_time + INTERVAL 90 DAY;
```

#### `engine`

```json
{
  "tableName": "orders",
  "engine": "ReplacingMergeTree(version)",
  "orderBy": "(order_id)"
}
```

Репликацию добавляет генератор по секции `cluster`, поэтому в `engine` указывается только базовый движок (`Replicated*` отклоняется валидатором):

| Топология | `ENGINE` для `"ReplacingMergeTree(version)"` |
| --- | --- |
| single-node | `ReplacingMergeTree(version)` |
| `cluster`, `ddlMode: onCluster` | `ReplicatedReplacingMergeTree('<replicatedPath>', '<replicaName>', version)` в `orders_local` + `Distributed` `orders` |
| `cluster`, `ddlMode: replicatedDatabase` | `ReplicatedReplacingMergeTree(version)` в `orders_local` + `Distributed` `orders` |

В `schema.snapshot.json` поле `engine` пишется только для не-дефолтного движка; снапшот без поля читается как `MergeTree` (drift не возникает). Смена `engine` — **manual**-изменение: `migrate` кладёт в миграцию закомментированный шаблон `CREATE TABLE <t>__engine_new AS <t> ENGINE = …` → `INSERT INTO … SELECT * FROM <t>` (в кластере — на каждом шарде) → `EXCHANGE TABLES` → `DROP TABLE <t>__engine_new`, остальные изменения той же таблицы тоже уходят в manual. Перед копированием остановите запись (DETACH Kafka-очередей, питающих таблицу).



### `materializedViews[]`


| Поле          | Описание                                                                                    |
| ------------- | ------------------------------------------------------------------------------------------- |
| `name`        | Имя MV                                                                                      |
| `sourceTable` | Должна совпадать с `kafkaTables[].tableName`                                                |
| `targetTable` | Должна совпадать с `mergeTreeTables[].tableName`                                            |
| `columns`     | Маппинг `{ "source", "target", "expression"? }`. **Пустой** -> все колонки `sourceTable` 1:1 |


- `source` - колонка/путь в Kafka-таблице (`price.amount`, `event_time.seconds`)
- `target` - колонка в MergeTree
- `expression` - опционально SQL вместо простого копирования

Примеры expression:

```json
{ "source": "status", "target": "status", "expression": "toString(status)" }
```

```json
{
  "source": "event_time.seconds",
  "target": "event_time",
  "expression": "toDateTime64(event_time.seconds + event_time.nanos / 1000000000.0, 3)"
}
```



### Автоколонки из queue (зеркало схемы)

Если нужен MergeTree + MV со **всеми** полями Kafka-таблицы без ручного перечисления - оставьте `columns` пустым и задайте `sourceTable`:

```json
"mergeTreeTables": [
  {
    "tableName": "orders_raw",
    "sourceTable": "orders_queue",
    "orderBy": "(order_id)"
  }
],
"materializedViews": [
  {
    "name": "orders_raw_mv",
    "sourceTable": "orders_queue",
    "targetTable": "orders_raw"
  }
]
```

Поведение:


| Объект              | Пустой `columns`                                                                                   |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| `mergeTreeTables`   | Копирует `name` + `type` из mapped-схемы `sourceTable` (включая ``price.amount``, Nested, Enum, …) |
| `materializedViews` | Строит `SELECT col AS col, … FROM sourceTable` по всем колонкам queue                              |


Если MergeTree заполняется автоколонками и в `materializedViews` нет view с `targetTable`, равным этой таблице, генератор сам добавляет зеркальный view `{tableName}_mv` (`orders_raw` -> `orders_raw_mv`). Явный view на ту же таблицу (в том числе с пустым `columns`) отключает автосоздание: используется только он.

Короткий вариант без секции `materializedViews`:

```json
"mergeTreeTables": [
  {
    "tableName": "orders_raw",
    "sourceTable": "orders_queue",
    "orderBy": "(order_id)"
  }
]
```

Правила:

- Для MergeTree при пустом `columns` поле `sourceTable` **обязательно** и должно совпадать с `kafkaTables[].tableName`.
- Непустой `columns` - только перечисленные поля. Автоматический view в этом случае не создаётся: для rename и expression нужен явный `materializedViews`.
- Зеркало копирует protobuf->ClickHouse типы queue as-is (Timestamp остаётся `*.seconds`/`*.nanos`, enum - `Enum8`/`Enum16`). Для преобразований (`toDateTime64`, `toString`, rename) задайте `columns` явно.
- `orderBy` / `ttl` по-прежнему задаются вручную и должны ссылаться на колонки итоговой таблицы.
- Имя автогенерируемого view (`{tableName}_mv`) не должно совпадать с уже объявленным `materializedViews[].name`.



### `aggregates[]`

Агрегат — first-class сущность: генератор сам строит целевую таблицу (типы колонок выводятся из `sourceTable`) и refreshable MV с полным пересчётом `FROM <sourceTable> FINAL`, в снапшот попадают поля агрегата, а `migrate` сравнивает их по отдельности. Демо `orders_agg_1m`:

```json
"aggregates": [
  {
    "name": "orders_agg_1m",
    "viewName": "orders_agg_mv",
    "sourceTable": "orders",
    "timeBucket": { "column": "event_time", "function": "toStartOfMinute", "alias": "minute" },
    "groupBy": [ "category" ],
    "measures": [
      { "function": "count", "alias": "orders_count" },
      { "function": "sum", "column": "amount", "alias": "total_amount" },
      { "function": "sum", "column": "quantity", "alias": "total_qty" }
    ],
    "refresh": { "every": "1 MINUTE" }
  }
]
```

| Поле | По умолчанию | Описание |
| --- | --- | --- |
| `name` | — | целевая таблица (`onCluster`: `name_local` + `Distributed` `name`; `replicatedDatabase`: `default.name` без `Distributed`) |
| `viewName` | `{name}_mv` | имя refreshable MV |
| `sourceTable` | — | одна из `pipeline.mergeTreeTables` |
| `engine` | `MergeTree` | движок цели (семейство MergeTree, без `Replicated`) |
| `orderBy` | `(alias корзины, groupBy…)`; `tuple()` без ключей | `ORDER BY` цели |
| `final` | `true`, если движок источника не `MergeTree` | `FROM source FINAL`; `final: true` над `MergeTree` — ошибка валидации |
| `timeBucket` | — | `column`, `function` (`toStartOfMinute` (default), `toStartOfFiveMinutes`, `toStartOfFifteenMinutes`, `toStartOfHour`, `toStartOfDay` → `DateTime`; `toDate`, `toMonday`, `toStartOfMonth` → `Date`), `alias`, опц. `type` |
| `groupBy` | `[]` | колонки источника; тип копируется из источника |
| `measures[]` | — (≥ 1) | `function`: `count` (без `column`), `sum`, `avg`, `min`, `max`, `any`, `uniq`, `uniqExact`; `column`, `alias`, опц. `type` |
| `refresh.mode` | `refreshable` | единственный режим: insert-триггерный MV посчитал бы повторные доставки Kafka до дедупа источника |
| `refresh.every` | — | `<n> SECOND\|MINUTE\|HOUR\|DAY\|WEEK\|MONTH\|YEAR` |

Вывод типов: `count`/`uniq`/`uniqExact` → `UInt64`; `sum` целых → `Int64`/`UInt64`, `Float*` → `Float64`; `avg` → `Float64`; `min`/`max`/`any` — тип колонки; `Nullable(T)` сохраняется. Что не выводится (например `sum` по `Decimal`/`String`) — ошибка генерации с просьбой задать `measures[].type`.

Колонки цели: корзина, `groupBy`, меры — в этом порядке; `SELECT` — те же выражения с `AS alias`, `GROUP BY` — ключи. Агрегаты рендерятся после MV пайплайна и до `trailingSql`.

**Топологии** (refresh без `APPEND` атомарно подменяет цель через `EXCHANGE`, что невозможно для `Distributed`):

| Топология | Объекты | Движок цели | Источник | Кто выполняет refresh |
| --- | --- | --- | --- | --- |
| single-node | `name`, `viewName` | `engine` | `source FINAL` | нода |
| `onCluster` (Atomic) | `name_local` + `Distributed` `name` (даже при write-through); MV `TO name_local` | **`engine` без `Replicated`** | `source_local FINAL` — каждая нода агрегирует свой шард | каждая нода независимо (uncoordinated); реплики шарда считают одно и то же из реплицированного источника |
| `replicatedDatabase` | `default.name` и `default.viewName` с `ON CLUSTER` (Atomic `default`, не Replicated database; без `*_local` / `Distributed`) | **`engine` без `Replicated`** | глобальный `Distributed` `<replicatedDatabaseName>.source FINAL` | каждая нода независимо (uncoordinated), каждая хранит полный агрегат кластера |

В `onCluster` `Distributed` `name` над `name_local` даёт итог по кластеру ровно один раз — читать через `sum(...) GROUP BY`. В `replicatedDatabase` читать локально: `SELECT … FROM default.name` (без `Distributed`, иначе итог умножится на число шардов); `ReadAggregates` — при `ReadAggregates__Database=default` (`docker-compose.cluster.replicated-db.yml`). Почему не внутри Replicated database: refresh без `APPEND` подменяет цель DDL-операцией всей базы и при реальных шардах падает с `TABLE_UUID_MISMATCH`.

**Миграции.** Снапшот хранит агрегат целиком (`viewName`, `sourceTable`, `engine`, `orderBy`, `final`, `refreshMode`/`refreshEvery`, `timeBucket`, `groupBy`/`measures` с разрешёнными типами); drift-check сравнивает каждое поле, `migrate` — по полям:

| Изменение | Тип | SQL |
| --- | --- | --- |
| новый агрегат | автоматически | `CREATE TABLE IF NOT EXISTS` (+ `Distributed` в `onCluster`) и MV |
| `refresh`, `viewName`, `sourceTable`, `final`, функция/колонка меры или корзины при тех же колонках цели | автоматически | `DROP VIEW IF EXISTS <старое имя>` + `CREATE MATERIALIZED VIEW` |
| колонки цели (добавить/удалить/переименовать меру, `groupBy`, alias, тип), `engine`, `orderBy` | **manual** | закомментированный шаблон: `DROP VIEW`, `DROP TABLE` (`onCluster`: ещё `name_local … SYNC`; `replicatedDatabase`: `default.name`), полный DDL цели и MV; следующий refresh заполнит таблицу заново |
| удалить агрегат | **manual** | закомментированные `DROP VIEW` / `DROP TABLE` |

**Перенос из `trailingSql`.** Если `trailingSql` удалён, а новые `aggregates[]` в single-node рендерятся ровно в прежний текст `trailingSql` (тот же SHA-256), `migrate` распознаёт перенос: снапшот обновляется (`trailingSqlHash` → `aggregates`), SQL-миграция не создаётся. Init-скрипты при этом байт-в-байт прежние во всех топологиях — так демо `orders_agg_1m` переехал из `trailingSql`. Если текст отличается, остаётся manual `TrailingSqlChanged` + автоматический `AggregateAdded`.

### `trailingSql`

Произвольный SQL, дописывается в конец файла — для редких объектов, которые не выражаются через `mergeTreeTables` / `materializedViews` / `aggregates` (словари, функции, нестандартные MV). Может быть многострочной строкой JSON с `\n`. В снапшоте хранится только хэш: любое изменение — manual `TrailingSqlChanged`. В кластерном режиме переписывается `ClusterTrailingSqlRewriter` (см. [`cluster`](#cluster-кластерный-clickhouse)): таблицы семейства MergeTree → `Replicated*` `t_local` + `Distributed` `t`, `FROM`/`TO` известных storage-таблиц → `*_local`. Refreshable MV (`CREATE MATERIALIZED VIEW v REFRESH … [APPEND] TO t AS …`) переписываются по тем же правилам топологий, что и [`aggregates[]`](#aggregates) (`onCluster`: цель `t_local` без `Replicated`, источник `t_local`; `replicatedDatabase`: `default.t` / `default.v` с `ON CLUSTER`, источник `<replicatedDatabaseName>.t`).

---



## Чеклист новой Kafka-таблицы

1. Добавьте `.proto` в `protos/` и убедитесь, что message собирается в `Sandbox.Contracts`.
2. Добавьте элемент в `kafkaTables` с `messageType`, `protoFile`, `messageName`, `tableName`, `outputPath`.
3. Заполните `kafka.topic` / `groupName` (и при необходимости `skipBytes`).
4. При необходимости задайте `fieldOverrides`. Если ключ сообщения — protobuf, добавьте секцию `key` (`format: "protobuf"`, `messageType`).
5. Если нужна аналитика - добавьте `mergeTreeTables` + `materializedViews` в `pipeline` (`sourceTable` / `targetTable` должны ссылаться на существующие имена). Для зеркала всех полей queue оставьте `columns` пустым и укажите `sourceTable` у MergeTree: view `{tableName}_mv` будет создан сам, если вы не описали view на эту таблицу.
6. Выполните `dotnet build src/Sandbox.Contracts` и проверьте сгенерированный SQL.
7. Пересоздайте ClickHouse init при необходимости (`docker compose` / volume init).

---



## Валидация

Конфиг проверяется FluentValidation при codegen. Частые ошибки:

- пустой `kafkaTables`
- дубли `tableName` или `outputPath`
- `protoFile` с путём или расширением (нужно только имя: `order_event`)
- неверный путь в ключе `fieldOverrides`
- MV ссылается на неизвестный `sourceTable` / `targetTable`
- MergeTree с пустым `columns` без `sourceTable` (нужен для автозаполнения)
- MergeTree `sourceTable` не совпадает с `kafkaTables[].tableName`
- Автогенерируемое имя view `{tableName}_mv` уже занято другим materialized view
- TTL ссылается на колонку, которой нет в `columns` (например `created_at`, когда в `columns` есть только `event_time`)
- `engine` не из семейства MergeTree (`MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`), с `Replicated`-префиксом, с неверным числом аргументов (`CollapsingMergeTree` без `sign`, `MergeTree(x)`) или с несбалансированными скобками
- `kafka.brokerList` вместе с `kafka.namedCollection` (взаимоисключаются); `namedCollection` не идентификатор (`[A-Za-z_][A-Za-z0-9_]*`)
- `kafka.maxBlockSize` / `pollMaxBatchSize` вне 1…10 000 000, `flushIntervalMs` вне 100…600 000, `pollMaxBatchSize` > `maxBlockSize`
- `kafka.handleErrorMode` не `default` / `stream` / `dead_letter_queue`; `skipBrokenMessages` < 0 или вместе с не-`default` режимом; `stream` без `pipeline` или с занятым именем `{tableName}_errors` / `{tableName}_errors_mv`
- `key.format` не `string` / `protobuf`; `key.format: protobuf` без `key.messageType`; `key.messageType` / `skipBytes` / `fieldOverrides` при строковом ключе
- MV ссылается на `_key.<поле>`, которого нет в protobuf-ключе (или ключ строковый)
- `repeatedMessageStrategy` не из списка `nested`  `arraytuple`  `flatten`
- `cluster.name` не идентификатор; `replicatedPath` без `{table}` / `{uuid}`; `historyReplicatedPath` с `{shard}`
- имя `<table><localTableSuffix>` совпадает с уже существующей таблицей / view
- `aggregates[]`: `sourceTable` не из `pipeline.mergeTreeTables`; нет мер; функция меры/корзины не из списка; `count` с `column` или другая функция без `column`; дубли колонок цели; `refresh.mode` не `refreshable`; `refresh.every` не `<n> UNIT`; `final: true` над `MergeTree`; имя агрегата или его view занято; при генерации — колонка, которой нет в источнике, или невыводимый тип меры

