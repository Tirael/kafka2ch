# Инструкция: `clickhouse.codegen.json`

Конфиг генератора ClickHouse DDL из protobuf (`tools/ClickHouseSchemaGen`).  
Файл: [`clickhouse.codegen.json`](clickhouse.codegen.json) в этом каталоге.

После правок перегенерируйте SQL:

```bash
dotnet build src/Sandbox.Contracts
```

Результат — файлы в `docker/clickhouse/init/` (пути задаются в конфиге).  
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

| Секция | Обязательна | Назначение |
|---|---|---|
| `defaults` | нет | Глобальные правила маппинга proto → ClickHouse |
| `kafkaTables` | **да** (не пустой) | Kafka Engine таблицы (по одной на message) |
| `fieldOverrides` | нет | Глобальные overrides полей (мержатся с табличными) |
| `pipeline` | нет | MergeTree + materialized views + произвольный SQL |

Имена `tableName` и пути `outputPath` в `kafkaTables` должны быть уникальны.

---

## `defaults`

| Поле | Тип | По умолчанию | Описание |
|---|---|---|---|
| `maxFlattenDepth` | int (> 0) | `3` | Глубина flatten вложенных `message` |
| `repeatedMessageStrategy` | string | `"nested"` | Стратегия для `repeated message`: `nested` \| `arraytuple` \| `flatten` |
| `optionalAsNullable` | bool | `true` | `optional T` → `Nullable(T)` |
| `oneofPresence` | bool | `true` | Колонка присутствия oneof (нужна для ProtobufSingle) |
| `enumMaxValuesForEnum8` | int (> 0) | `127` | Порог: меньше/равно → `Enum8`, иначе `Enum16` |

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

### Маппинг proto → ClickHouse (по умолчанию)

| Proto3 | ClickHouse |
|---|---|
| scalar | `String` / `Int32` / … |
| `optional T` | `Nullable(T)` |
| `enum` | `Enum8` / `Enum16` |
| nested `message` | плоские колонки `` `parent.field` `` |
| `repeated` scalar/enum | `Array(T)` |
| `repeated` message | `Nested(...)` (`flatten_nested = 0`) |
| `map<K,V>` | `Map(K,V)` |
| `oneof` | ветки + `{oneofName} Enum8` |
| `google.protobuf.Timestamp` | `` `field.seconds` ``, `` `field.nanos` `` |
| `google.protobuf.*Value` | `Nullable(T)` |
| `Struct` / `Any` | `String` (JSON; лучше задать override) |

---

## `kafkaTables[]`

Одна запись = одна таблица `ENGINE = Kafka` и один выходной `.sql`.

### Обязательные поля

| Поле | Пример | Правила |
|---|---|---|
| `messageType` | `"Sandbox.Contracts.OrderEvent, Sandbox.Contracts"` | CLR-тип: `FullName, Assembly` |
| `tableName` | `"orders_queue"` | Идентификатор ClickHouse: `[a-zA-Z_][a-zA-Z0-9_]*` |
| `protoFile` | `"order_event"` | Имя `.proto` **без** пути и расширения |
| `messageName` | `"OrderEvent"` | Имя message в proto |
| `outputPath` | `"../../docker/clickhouse/init/01_orders_queue.sql"` | Путь к генерируемому SQL (относительно этого JSON) |

### `kafka` (опционально)

| Поле | По умолчанию | Описание |
|---|---|---|
| `brokerList` | `"kafka:9092"` | Брокеры Kafka |
| `topic` | `"orders"` | Топик |
| `groupName` | `"clickhouse-orders"` | Consumer group |
| `skipBytes` | `6` | Пропуск Confluent Protobuf envelope (`00` + 4 байта schema id + `00`) |
| `numConsumers` | `1` | Число consumers (> 0) |
| `flattenNested` | `false` | `flatten_nested` в ClickHouse |
| `protobufOneofPresence` | `true` | `input_format_protobuf_oneof_presence` |
| `protobufFlattenGoogleWrappers` | `true` | Flatten google wrappers |

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

---

## `fieldOverrides`

Ключ — путь поля в proto (точки для вложенности):

- `status`
- `destination.country`
- `status_history`

Формат пути: `[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*`.

| Свойство | Тип | Описание |
|---|---|---|
| `type` | string | Явный тип ClickHouse, например `"LowCardinality(String)"` |
| `enum8` | bool | Принудительно `Enum8` |
| `strategy` | string | Имя стратегии маппинга (имя enum `MappingStrategy`) |
| `maxDepth` | int (> 0) | Локальный лимит flatten |
| `nullable` | bool | Принудительная nullability |

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

| Поле | Описание |
|---|---|
| `tableName` | Имя MergeTree-таблицы |
| `orderBy` | Выражение `ORDER BY`, например `"(event_time, order_id)"` |
| `columns` | Непустой список `{ "name", "type" }` |

### `materializedViews[]`

| Поле | Описание |
|---|---|
| `name` | Имя MV |
| `sourceTable` | Должна совпадать с `kafkaTables[].tableName` |
| `targetTable` | Должна совпадать с `mergeTreeTables[].tableName` |
| `columns` | Маппинг `{ "source", "target", "expression"? }` |

- `source` — колонка/путь в Kafka-таблице (`price.amount`, `event_time.seconds`)
- `target` — колонка в MergeTree
- `expression` — опционально SQL вместо простого копирования

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

### `trailingSql`

Произвольный SQL, дописывается в конец файла (агрегаты, дополнительные MV и т.п.). Может быть многострочной строкой JSON с `\n`.

---

## Чеклист новой Kafka-таблицы

1. Добавьте `.proto` в `protos/` и убедитесь, что message собирается в `Sandbox.Contracts`.
2. Добавьте элемент в `kafkaTables` с `messageType`, `protoFile`, `messageName`, `tableName`, `outputPath`.
3. Заполните `kafka.topic` / `groupName` (и при необходимости `skipBytes`).
4. При необходимости задайте `fieldOverrides`.
5. Если нужна аналитика — добавьте `mergeTreeTables` + `materializedViews` в `pipeline` (`sourceTable` / `targetTable` должны ссылаться на существующие имена).
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
- `repeatedMessageStrategy` не из списка `nested` \| `arraytuple` \| `flatten`
