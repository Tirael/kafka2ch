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

Без секции `kafka` подставляются значения по умолчанию модели (`brokerList: kafka:9092`, `topic: orders`, `groupName: clickhouse-orders`, `skipBytes: 6` и т.д.). Для другого топика секцию `kafka` задайте явно.

---



## Секции конфига


| Секция           | Обязательна        | Назначение                                         |
| ---------------- | ------------------ | -------------------------------------------------- |
| `defaults`       | нет                | Глобальные правила маппинга proto -> ClickHouse     |
| `kafkaTables`    | **да** (не пустой) | Kafka Engine таблицы (по одной на message)         |
| `fieldOverrides` | нет                | Глобальные overrides полей (мержатся с табличными) |
| `pipeline`       | нет                | MergeTree + materialized views + произвольный SQL  |
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

**Агрегаты `orders_agg_1m`** — refreshable MV `REFRESH EVERY 1 MINUTE` с **полным пересчётом** `FROM orders FINAL` в обычную `MergeTree` (каждый refresh атомарно подменяет содержимое). Insert-триггерный `SummingMergeTree` суммировал бы каждую повторную вставку, а дедуп `orders` случается позже, при слиянии. Скользящее окно не выбрано: refresh без `APPEND` заменяет всю таблицу, поэтому окно стёрло бы старые минуты, а `APPEND` + окно снова требует дедупа цели. Цена: агрегаты отстают до минуты, каждый refresh читает весь `orders` (для демо приемлемо; для больших объёмов — партиционирование и пересчёт только свежих партиций). `ReadAggregates` читает `sum(...) GROUP BY minute, category` (в кластере строки шардов складываются) и логирует `system.view_refreshes.last_success_time`. Принудительно: `SYSTEM REFRESH VIEW orders_agg_mv; SYSTEM WAIT VIEW orders_agg_mv`.

Смена движка/ключа — manual-миграция `*_dedup_orders_by_kafka_offset.sql` (по одной на топологию): DETACH очереди → копия `orders` в новую `ReplacingMergeTree`-таблицу (в кластере через временную `Distributed`) → `EXCHANGE TABLES` → пересоздание `orders_agg_1m` / `orders_agg_mv` → ATTACH очереди и новый `orders_mv` с `_partition` / `_offset`.

## Миграции схемы

Секция `migrations` (пути по умолчанию относительно codegen.json):

- `snapshotPath` → `schema.snapshot.json`
- `migrationsDirectory` → `docker/clickhouse/migrations`
- `versionsOutputPath` → `00_schema_migrations.sql`

`00_schema_migrations.sql` создаёт таблицу `schema_migrations` и записывает baseline-миграции (`kind = 'migration'`). Префикс `00` зарезервирован: файл обязан идти раньше любого init-скрипта (валидатор конфига падает иначе), потому что каждый сгенерированный init-скрипт (`01_*`, `02_*`, …) в конце записывает себя строкой `kind = 'init'` (version — префикс файла, checksum — SHA-256 тела скрипта без этой строки). Сама таблица версий не числится миграцией: её создаёт и обновляет (`ALTER … ADD COLUMN IF NOT EXISTS kind`) Migrator до применения миграций, по аналогии с `VersionInfo` в FluentMigrator. Migrator учитывает только `kind = 'migration'`.

Workflow: правка proto → build падает на drift → `Cli migrate --name …` → review/commit → apply через `clickhouse-migrate` / Migrator. Только BACKWARD-compatible эволюция proto; `SkipClickHouseSnapshotCheck=true` — escape hatch.

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

**Цель MV (default = `_local`):** Kafka→raw MV на каждой ноде пишет в свой шард (`TO t_local`). Опция `materializedViewsWriteThroughDistributed` переключает `TO t`, чтобы строки перешардировались по `shardingKey` (дороже; нужно, если ключ шардирования важнее привязки к партиции Kafka). Agg-MV из `trailingSql` всегда читают `FROM *_local`, даже при write-through.

**Trade-off размещения (R15 / TP-8).** Write-through пишет в `Distributed` асинхронно: Kafka engine коммитит offset, когда блок принят в очередь `Distributed`, а не доставлен на шард, — сбой ноды между этими моментами теряет строки. Поэтому демо-конфиги (включая `replicatedDatabase`) пишут в `*_local`: offset коммитится только после записи в локальную реплицированную таблицу. Цена — шард строки определяется тем, какая нода получила партицию Kafka, а не `shardingKey`: `Distributed` `orders` с `cityHash64(order_id)` в этом режиме только читает. Следствие для дедупа: повторная доставка после ребаланса consumer group может попасть на другой шард, а `ReplacingMergeTree`/`FINAL` схлопывает дубли только внутри шарда — такие дубли ловит `scripts/verify-cluster.sh` (`count() FROM orders FINAL` ≠ `uniqExact(order_id)` → fail). Если важнее перешардирование по ключу, включите write-through **и** `distributed_foreground_insert = 1` в профиле пользователя Kafka-MV (синхронная доставка, медленнее consume).

**`ddlMode=replicatedDatabase`:** `CREATE DATABASE … ON CLUSTER … ENGINE = Replicated('<replicatedDatabasePath>', '{shard}', '{replica}')` (топология БД совпадает с `remote_servers`: 2 шарда × 2 реплики в compose), затем `schema_migrations` в `default` с `ON CLUSTER` и общим Keeper-путём (история на всех шардах), `USE <replicatedDatabaseName>`; таблицы/MV — `Replicated*MergeTree` без аргументов (путь задаёт Replicated DB), без `ON CLUSTER`. `DETACH` очередей — `PERMANENTLY`. Migrator: `--ddl-mode replicatedDatabase` / `ClickHouse__DdlMode`, `ClickHouse__Database=<replicatedDatabaseName>`. Пример: [`clickhouse.codegen.cluster.replicated-db.json`](clickhouse.codegen.cluster.replicated-db.json); compose: `-f docker-compose.cluster.yml -f docker-compose.cluster.replicated-db.yml`.

Снапшот/drift кластер не учитывают. Один проект на single-node и cluster → **отдельные** `migrations` / snapshot / `00_*.sql` на конфиг; `Cli migrate --config …` и apply — **отдельно для каждого** конфига (это ожидаемо).

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

`dotnet build src/Sandbox.Contracts` генерирует single-node, cluster (`onCluster`) и cluster (`replicatedDatabase`) варианты. Миграция: `Cli migrate --config <тот же codegen.json> --name …` — по разу на каждый конфиг. Apply: `Migrator --migrations <dir> --cluster kafka2ch [--ddl-mode onCluster|replicatedDatabase]` (env `ClickHouse__Cluster` / `ClickHouse__DdlMode` / `ClickHouse__Database`).

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
| `brokerList`                    | `"kafka:9092"`        | Брокеры Kafka                                                         |
| `topic`                         | `"orders"`            | Топик                                                                 |
| `groupName`                     | `"clickhouse-orders"` | Consumer group                                                        |
| `skipBytes`                     | `6`                   | Пропуск Confluent Protobuf envelope (`00` + 4 байта schema id + `00`) |
| `numConsumers`                  | `1`                   | Число consumers (> 0)                                                 |
| `flattenNested`                 | `false`               | `flatten_nested` в ClickHouse                                         |
| `protobufOneofPresence`         | `true`                | `input_format_protobuf_oneof_presence`                                |
| `protobufFlattenGoogleWrappers` | `true`                | Flatten google wrappers                                               |


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



### `trailingSql`

Произвольный SQL, дописывается в конец файла (агрегаты, дополнительные MV и т.п.). Может быть многострочной строкой JSON с `\n`. В кластерном режиме переписывается (см. [`cluster`](#cluster-кластерный-clickhouse)).

**Refreshable MV** (`CREATE MATERIALIZED VIEW v REFRESH … [APPEND] TO t AS …`) в кластере переписываются особо — refresh без `APPEND` атомарно подменяет содержимое цели через `EXCHANGE`, что невозможно для `Distributed`, а ClickHouse поддерживает `Replicated*`-цель refresh только внутри Replicated database:

| Топология | `TO` | Движок `t_local` | Кто выполняет refresh |
| --- | --- | --- | --- |
| single-node | `t` | как в SQL | нода |
| `onCluster` (Atomic) | всегда `t_local` (даже при write-through) | **как в SQL, без `Replicated`** | каждая нода независимо (uncoordinated); реплики шарда считают одно и то же из реплицированного источника |
| `replicatedDatabase` | всегда `t_local` | `Replicated*` (без аргументов) | одна реплика на шард (coordinated, Keeper-путь `default_replica_path` = `/clickhouse/tables/{uuid}/{shard}`), результат реплицируется на остальные |

`FROM t FINAL` для известных storage-таблиц переписывается в `FROM t_local FINAL`: каждый шард агрегирует свои строки, `Distributed` `t` над `t_local` даёт сумму по кластеру (читать через `sum(...) GROUP BY`).

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
- `key.format` не `string` / `protobuf`; `key.format: protobuf` без `key.messageType`; `key.messageType` / `skipBytes` / `fieldOverrides` при строковом ключе
- MV ссылается на `_key.<поле>`, которого нет в protobuf-ключе (или ключ строковый)
- `repeatedMessageStrategy` не из списка `nested`  `arraytuple`  `flatten`
- `cluster.name` не идентификатор; `replicatedPath` без `{table}` / `{uuid}`; `historyReplicatedPath` с `{shard}`
- имя `<table><localTableSuffix>` совпадает с уже существующей таблицей / view

