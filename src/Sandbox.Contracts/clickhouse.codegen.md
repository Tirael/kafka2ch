# Инструкция: `clickhouse.codegen.json`

Конфиг генератора ClickHouse DDL из protobuf (`tools/ClickHouseSchemaGen`).  
Файл: `[clickhouse.codegen.json](clickhouse.codegen.json)` в этом каталоге.

После правок перегенерируйте SQL:

```bash
dotnet build src/Sandbox.Contracts
```

Результат - файлы в `docker/clickhouse/init/` (пути задаются в конфиге).  
Пропуск codegen: `dotnet build -p:SkipClickHouseCodegen=true`.

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


Имена `tableName` и пути `outputPath` в `kafkaTables` должны быть уникальны.

---

## `persistKafkaMeta` / `includeKafkaMeta`

Сохраняет Kafka message key и headers в MergeTree (виртуальные колонки queue в DDL не объявляются):

- `kafka_key String` ← `_key` (сырые байты ключа)
- для `kafkaTables[].key.format = "protobuf"` — ещё `kafka_key.<поле>` ← `_key.<поле>` (типизированные поля ключа, см. секцию `key`)
- `kafka_headers Map(String, String)` ← `mapFromArrays(\`_headers.name\`, \`_headers.value\`)`

Включается в `defaults.persistKafkaMeta` и/или `includeKafkaMeta: true` на MergeTree / MV. Опционально: `topic`, `partition`, `offset`, `timestampMs`.

## Миграции схемы

Секция `migrations` (пути по умолчанию относительно codegen.json):

- `snapshotPath` → `schema.snapshot.json`
- `migrationsDirectory` → `docker/clickhouse/migrations`
- `versionsOutputPath` → `99_schema_migrations.sql`

Workflow: правка proto → build падает на drift → `Cli migrate --name …` → review/commit → apply через `clickhouse-migrate` / Migrator. Только BACKWARD-compatible эволюция proto; `SkipClickHouseSnapshotCheck=true` — escape hatch.

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
  "messageType": "Sandbox.Contracts.ShipmentEvent, Sandbox.Contracts",
  "tableName": "shipments_queue",
  "protoFile": "shipment_event",
  "messageName": "ShipmentEvent",
  "outputPath": "../../docker/clickhouse/init/02_shipments_queue.sql",
  "kafka": {
    "brokerList": "kafka:9092",
    "topic": "shipments",
    "groupName": "clickhouse-shipments",
    "skipBytes": 6,
    "numConsumers": 1,
    "flattenNested": false,
    "protobufOneofPresence": true,
    "protobufFlattenGoogleWrappers": true
  },
  "fieldOverrides": {
    "status": { "enum8": true },
    "destination.country": { "type": "LowCardinality(String)" }
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
  "outputPath": "../../docker/clickhouse/init/03_pipeline.sql",
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
| `ttl`         | Опционально. Выражение table-level `TTL`, например `"event_time + INTERVAL 90 DAY"`. Должно ссылаться только на колонки этой таблицы (`shipments` -> `shipped_at`, не `event_time`) |
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

Произвольный SQL, дописывается в конец файла (агрегаты, дополнительные MV и т.п.). Может быть многострочной строкой JSON с `\n`.

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
- TTL ссылается на колонку, которой нет в `columns` (например `event_time` у `shipments`, где есть только `shipped_at`)
- `key.format` не `string` / `protobuf`; `key.format: protobuf` без `key.messageType`; `key.messageType` / `skipBytes` / `fieldOverrides` при строковом ключе
- MV ссылается на `_key.<поле>`, которого нет в protobuf-ключе (или ключ строковый)
- `repeatedMessageStrategy` не из списка `nested`  `arraytuple`  `flatten`

