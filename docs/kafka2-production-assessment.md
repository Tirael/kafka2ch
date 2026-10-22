# Classic Kafka vs Kafka2 (StorageKafka2): оценка для production

**Вердикт:** для пайплайна kafka2ch (и типичного MV ETL) оставаться на **классическом** `ENGINE = Kafka`. Kafka2 (`StorageKafka2`) официально всё ещё **experimental / not production ready**; уверенность в рекомендации «не включать в prod» — **высокая (~85%)**. Переход имеет смысл только как осознанный эксперимент на стенде с Keeper, когда нужна retryable-дедупликация батчей или static shard affinity — не как drop-in замена.

Источники: DDL/codegen репозитория Tirael/kafka2ch; [доки Kafka engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/kafka); [beta/experimental settings](https://clickhouse.com/docs/reference/settings/beta-and-experimental-features); changelog 24.8 (#57625); open issues/PRs ClickHouse (в т.ч. #81598, #84616, #75752, #116485, #82170, #119632).

---

## 1. Что использует kafka2ch сегодня

Проект генерирует и поднимает **только classic Kafka + Materialized View → MergeTree** (и дальше agg MV). Ни `kafka_keeper_path`, ни `kafka_replica_name`, ни `allow_kafka_offsets_storage_in_keeper` в коде/DDL нет.

### Очереди (init / codegen)

`KafkaTableGenerator` всегда пишет `ENGINE = Kafka` и фиксированный набор settings. Пример (`docker/clickhouse/init/01_orders_queue.sql`, аналогично `02_shipments_queue.sql`):

| Setting | Значение в репо |
| --- | --- |
| `kafka_broker_list` | `kafka:9092` |
| `kafka_topic_list` | `orders` / `shipments` |
| `kafka_group_name` | `clickhouse-orders` / `clickhouse-shipments` |
| `kafka_format` | `ProtobufSingle` |
| `kafka_schema` | `order_event:OrderEvent` / `shipment_event:ShipmentEvent` |
| `kafka_schema_registry_skip_bytes` | `6` |
| `kafka_num_consumers` | `1` |
| `flatten_nested` | `0` (если есть Nested) |
| `input_format_protobuf_oneof_presence` | `1` |
| `input_format_protobuf_flatten_google_wrappers` | `1` |

Топики в compose: **1 partition**. Offsets — в Kafka consumer group (classic).

### Пайплайн MV

`03_pipeline.sql`: `orders_queue` / `shipments_queue` → MV (`orders_mv`, `shipments_mv`) → MergeTree (`orders`, `shipments`) + agg MV. Виртуальные `_key` / `_headers.*` материализуются в обычные колонки. Документы (`PLAN.md`, `MIGRATIONS_PLAN.md`, `clickhouse.codegen.md`) описывают тот же classic-паттерн; для безопасных миграций queue — `DETACH` / recreate / `ATTACH`.

### Версии ClickHouse в проекте

| Контекст | Версия |
| --- | --- |
| docker-compose / `.env.example` | `clickhouse/clickhouse-server:26.6.3.62` (README: ≥ **26.6**) |
| Integration tests (`TestContainersEnv`, override `CLICKHOUSE_IMAGE`) | `clickhouse/clickhouse-server:26.6.3.62` |
| Минимальная фича для SR envelope | `kafka_schema_registry_skip_bytes` с **25.11+** |

Обе линейки (25.11 и 26.6) **уже содержат** Kafka2 (появился в **24.8**). Версия стенда **не** делает Kafka2 «готовым к prod» — статус experimental не снят.

Compose — одиночный ClickHouse без выделенного Keeper/Replicated DB; для Kafka2 понадобился бы рабочий Keeper с нужными feature flags.

---

## 2. Как включается и чем отличается Kafka2

Тот же SQL-движок `ENGINE = Kafka`. `StorageKafka2` выбирается, если заданы **оба** table-setting:

- `kafka_keeper_path` (рекомендуют `{uuid}`, напр. `/clickhouse/{database}/{uuid}`)
- `kafka_replica_name` (часто `{replica}`)

и session/query setting:

- `allow_kafka_offsets_storage_in_keeper = 1`  
  alias: `allow_experimental_kafka_offsets_storage_in_keeper`

Без флага CREATE падает с «experimental». В каталоге experimental features setting всё ещё listed as experimental (default `0`); docs: *«new, experimental Kafka engine»*, *«not production ready yet»*.

### Поведение

| Аспект | Classic (`StorageKafka`) | Kafka2 (`StorageKafka2`) |
| --- | --- | --- |
| Source of truth для resume | Kafka consumer group offsets | **ClickHouse Keeper** (Kafka offsets ещё коммитятся, но после create/restart/recovery берутся из Keeper) |
| При create таблицы | — | Первый старт может опереться на Kafka offsets |
| Retry при failed insert | Новый poll → риск другого набора сообщений / дублей | Перед insert пишется **intent size**; при ошибке повторяется **тот же объём** сообщений → проще дедуп на стороне MergeTree/Replicated |
| Exactly-once | Нет (at-least-once + возможные дубли) | **Не EOS**: maintainer’ы (#81598) — *repeatable inserts*, атомарный commit data+offsets отложен |
| Multi-consumer | `kafka_num_consumers` без обязательного thread-per-consumer | Несколько consumers **только** при `kafka_thread_per_consumer = 1` (один partition на consumer thread) |
| Shard affinity | Нет (или через разные group / ручное) | `kafka_partition_shard_num` + `kafka_shard_count` (только Kafka2) |
| Keeper | Не нужен | Обязателен; нужны Keeper extensions (`create_if_not_exists` и др.) — см. #75752 |
| Cloud | Classic / ClickPipes | Experimental; в Cloud обычно нельзя включить experimental |

### Ограничения Kafka2 (из официальных docs)

1. Быстрый DROP/CREATE или общий Keeper path у разных таблиц — риск поломки; best practice — `{uuid}` в path.
2. Multi-consumer без `kafka_thread_per_consumer` запрещён (repeatable read vs регулярный poll).
3. При affinity все шарды должны иметь одинаковый `kafka_shard_count`, иначе дыры/двойное чтение партиций.

---

## 3. Production readiness

### Статус

- С **24.8 LTS** (PR [#57625](https://github.com/ClickHouse/ClickHouse/pull/57625), changelog `v24.8.1`): experimental feature.
- На момент оценки (актуальные docs + experimental settings list): **всё ещё experimental**, формулировка *«not production ready yet»* не снята.
- Blog 24.8 обещал «exactly-once»; позже maintainers уточнили (#81598): целились в EOS, сейчас — **repeatable batch retry**, не атомарный EOS. Маркетинговый ярлык 24.8 не считать гарантией.

### Зрелость (сигналы)

Плюсы: активная разработка (rebalance на Keeper в 25.6 — #78726; bugfix потери сообщений при failed streaming cycle — [#116485](https://github.com/ClickHouse/ClickHouse/pull/116485), merged 2026-09); setting переименован в «короткий» `allow_kafka_offsets_storage_in_keeper` с experimental alias; shard affinity; Keeper feature-flag hygiene (#76004).

Минусы / blockers:

| Риск | Доказательство |
| --- | --- |
| Официально не GA / не supported как experimental | Docs + experimental settings list |
| Crash после потери сессии Keeper | Open [#84616](https://github.com/ClickHouse/ClickHouse/issues/84616) (в т.ч. на 25.6.x; label *experimental feature*) |
| Нет настоящего EOS с MV | [#81598](https://github.com/ClickHouse/ClickHouse/issues/81598) |
| Ограничение `thread_per_consumer` | Open [#82170](https://github.com/ClickHouse/ClickHouse/issues/82170) |
| Flaky starvation temporary locks / partition affinity | [#119632](https://github.com/ClickHouse/ClickHouse/issues/119632) |
| Keeper extensions должны быть включены | [#75752](https://github.com/ClickHouse/ClickHouse/issues/75752) |
| Недавний loss-window на failed poll | Исправлен #116485 — на старых патчах 25.x/ранних 26.x без бэкпорта риск silent skip |

### Caveats для MV ETL как в kafka2ch

1. **Дубли при retry всё ещё возможны** между успешным insert в MergeTree и commit intent/offset в Keeper; Kafka2 облегчает *повторяемость батча*, но идемпотентность/дедуп на приёмнике (ReplacingMergeTree, уникальные ключи, `insert_deduplication`) по-прежнему нужны, если дубли критичны.
2. **Cascaded MV** (`queue → raw → agg`): durability docs требуют `fsync_after_insert` (+ `fsync_part_directory`) на *каждой* MergeTree-цели батча; иначе окно «offset в Keeper закоммичен, page cache потерян → silent loss» остаётся и для Kafka2.
3. **Миграции queue** (DETACH/DROP/CREATE с тем же Keeper path) опаснее, чем classic: docs предупреждают про path clash; для kafka2ch-мигратора это новый класс рисков.
4. **Protobuf / `_key` / headers / `kafka_schema_registry_skip_bytes`** работают на уровне формата и virtual columns — к Kafka2 нейтральны; codegen сегодня hardcode classic settings.
5. **1 partition + 1 consumer** в демо: выгода Kafka2 минимальна; classic проще и достаточна.
6. **25.11 vs 26.6**: для classic kafka2ch релевантны 25.11+ (skip_bytes) и pin 26.6 в e2e. Для Kafka2 оба подходят «по наличию фичи», но open crash (#84616) и свежие фиксы (#116485) делают выбор **свежего патча 26.6+** обязательным *если* экспериментировать — не достаточным для prod.

---

## 4. Когда Kafka2 помогает / когда оставаться на classic

### Имеет смысл пробовать Kafka2 (стенд / canary)

- Нужны **повторяемые retry** одного и того же блока после failed MV insert (меньше «случайных» дублей от разного poll).
- Несколько шардов ClickHouse читают один топик с **static partition affinity** (`kafka_partition_shard_num` / `kafka_shard_count`).
- Уже есть зрелый Keeper (с extensions), Replicated/Atomic DB, готовность жить с experimental и отдельным мониторингом Keeper paths / locks.
- Готовы дедуп на MergeTree и ужесточить `fsync_*` на всех MV-целях.

### Оставаться на classic (рекомендация для kafka2ch / большинства prod)

- Нужен **поддерживаемый** ingestion path (docs, Cloud, операционный опыт).
- Паттерн **queue + MV + MergeTree** уже работает; топик с малым числом партиций; at-least-once + редкие дубли приемлемы или закрыты дедупом downstream.
- Нет выделенного Keeper / не хотите зависеть от Keeper feature flags и session expiry.
- Частые schema migrations с recreate Kafka queue (как в migrator kafka2ch).
- Нужна простота multi-consumer без жёсткого `kafka_thread_per_consumer`.

### Production recommendation

| | |
| --- | --- |
| **Рекомендация** | **Classic Kafka engine** для production и для текущего kafka2ch |
| **Уверенность** | **Высокая (~85%)** |
| **Блокеры Kafka2 для prod** | (1) Docs: experimental, not production ready; (2) setting в experimental list, без GA; (3) open crash on Keeper session loss (#84616); (4) нет EOS (#81598); (5) operational: Keeper flags, path lifecycle, affinity/thread constraints; (6) недавние correctness fixes (#116485) — признак ещё «сырой» зрелости |
| **Когда пересмотреть** | Снятие experimental / перевод в beta+official support; закрытие #84616 и ключевых correctness issues; changelog «production ready» в docs; зелёный soak на вашей нагрузке и версии CH |

Для ClickHouse Cloud ingestion предпочтительнее ClickPipes / поддерживаемые пути, не experimental Kafka2.

---

## Краткое сравнение (одной таблицей)

| | Classic | Kafka2 |
| --- | --- | --- |
| Включение | DDL без keeper settings | `allow_kafka_offsets_storage_in_keeper` + `kafka_keeper_path` + `kafka_replica_name` |
| Offsets | Kafka | Keeper (+ mirror в Kafka) |
| Retry / dedup intent | Слабее | Intent size → repeatable batch |
| EOS | Нет | Нет (пока) |
| Prod status | Основной путь | Experimental |
| kafka2ch today | Да | Нет |

**Итог для пользователя:** стабильность Kafka2 для production пока **недостаточная**; для kafka2ch-пайплайна — **остаёмся на classic**, пока ClickHouse явно не выведет StorageKafka2 из experimental и не закроет Keeper-crash / EOS-gap.
