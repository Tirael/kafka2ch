# ADR-0001: путь ingestion Kafka → ClickHouse

| | |
| --- | --- |
| **Статус** | **Proposed** — решение за владельцем репозитория |
| **Дата** | 2026-10-21 |
| **Пункт плана** | W3-4 (ATAM remediation), риск-тема T1 |
| **Связано** | [architecture-tradeoff-analysis.md](../architecture-tradeoff-analysis.md) (AD-1, TP-1, TP-6, SP-1..SP-3, SP-16, R1, R2, R5, R15, P2-1, P2-2), [kafka2-production-assessment.md](../kafka2-production-assessment.md), [clickpipes-how-to.md](../clickpipes-how-to.md) |
| **PR, от которых зависит решение** | W1-2 [#66](https://github.com/Tirael/kafka2ch/pull/66), W1-3 [#67](https://github.com/Tirael/kafka2ch/pull/67), W1-4 [#65](https://github.com/Tirael/kafka2ch/pull/65), W1-7 [#68](https://github.com/Tirael/kafka2ch/pull/68), W2-3 [#77](https://github.com/Tirael/kafka2ch/pull/77), W2-9 [#74](https://github.com/Tirael/kafka2ch/pull/74) |

## 1. Контекст

Сейчас ingestion целиком внутри ClickHouse: `orders_queue` (`ENGINE = Kafka`, `ProtobufSingle`, `kafka_schema_registry_skip_bytes = 6`) → `orders_mv` → `orders` → агрегаты (AD-1 ATAM). ATAM выделил тему **T1 «семантика доставки не спроектирована явно»**:

- **R1** — at-least-once без дедупа: повтор блока после ребаланса/рестарта завышает `orders` и агрегаты;
- **R2** — «ядовитое» сообщение останавливает консьюмер;
- **R15** — в replicated-db режиме MV писали через асинхронный `Distributed`, offset коммитился до доставки на шард;
- **R5** — SQL UDF `protobufWire*` (декод ключа) не реплицировались в replicated-db режиме;
- **TP-1 / TP-6 / SP-3 / SP-16** — CPU ingestion (включая синхронные MV и UDF-декод ключа) делит ресурсы с запросами.

Пункт P2-1 ATAM откладывал вопрос «остаться на Kafka engine или вынести ingestion наружу» до закрытия P0. Эти P0/P1-исправления сейчас в работе:

| PR | Что меняет для ingestion |
| --- | --- |
| W1-3 [#67](https://github.com/Tirael/kafka2ch/pull/67) | `orders` = `ReplacingMergeTree` с `(kafka_partition, kafka_offset)` в ключе сортировки; точные чтения через `FINAL`; агрегаты — refreshable MV `REFRESH EVERY 1 MINUTE ... FROM orders FINAL` (полный пересчёт); replicated-db: MV пишут в `*_local` (закрывает R1, R15) |
| W1-2 [#66](https://github.com/Tirael/kafka2ch/pull/66) | `handleErrorMode` (`stream` / `dead_letter_queue`) и `skipBrokenMessages`; в `stream` генерируются `{queue}_errors` + `{queue}_errors_mv`, основные MV фильтруют `length(_error) = 0` (закрывает R2) |
| W1-4 [#65](https://github.com/Tirael/kafka2ch/pull/65) | UDF реплицируются через Keeper (`user_defined_zookeeper_path`) в replicated-db режиме (закрывает R5) |
| W1-7 [#68](https://github.com/Tirael/kafka2ch/pull/68) | Корректное имя шарда в `ENGINE = Replicated(path, shard, replica)` |
| W2-3 [#77](https://github.com/Tirael/kafka2ch/pull/77) | Kafka named collections: брокеры/секреты вне DDL, snapshot и миграций |
| W2-9 [#74](https://github.com/Tirael/kafka2ch/pull/74) | Health-check по `system.kafka_consumers` + `{queue}_errors`, lag через Kafka Admin, Prometheus-эндпоинт и алерты |

Вопрос ADR: **после этих PR достаточно ли Kafka engine, или ingestion надо переносить в ClickHouse Kafka Connect Sink, собственный .NET consumer или ClickPipes?**

## 2. Драйверы решения (критерии)

| # | Критерий | Что считаем «достаточно» |
| --- | --- | --- |
| C1 | **Дубли** после W1-3 | Точные чтения (`FINAL`, агрегаты) не завышены; сырые повторы до слияния допустимы |
| C2 | **CPU ingestion vs запросы** | Ingestion (consume + parse + MV + UDF) не деградирует latency запросов сверх бюджета (порог — §5) |
| C3 | **DLQ** | Битое сообщение не останавливает поток, сохраняется с raw-payload, есть алерт; нужна ли переигровка в Kafka — отдельное требование |
| C4 | **Schema Registry** | Корректный разбор Confluent wire format; нужна ли валидация по schema id и несколько типов сообщений в топике |
| C5 | **Эксплуатационная сложность** | Число движущихся частей, деплой, on-call, знание стека командой (.NET) |
| C6 | **Exactly-once** | Нужно ли «ровно один раз» на уровне вставки (без `FINAL`, для insert-triggered MV) |
| C7 | **Эволюция схемы с codegen** | Сохраняется ли единый источник правды (`.proto` + `clickhouse.codegen.json`) и автоматические миграции |
| C8 | **Мульти-топология** | single-node / `ON CLUSTER` / replicated-db одинаково поддерживаются; понятно, куда ложатся строки и когда коммитится offset |
| C9 | **Наблюдаемость** | Lag, ошибки, состояние консьюмеров видны в метриках без `clickhouse-client` |

## 3. Рассмотренные варианты

### A. Kafka engine (как сейчас) + W1-2 + W1-3 + W2-9

Очередь `ENGINE = Kafka` на каждой ноде, MV → `*_local`, дедуп хранения `ReplacingMergeTree` по `(kafka_partition, kafka_offset)`, агрегаты refreshable MV поверх `orders FINAL`, ошибки парсинга — `kafka_handle_error_mode = 'stream'` в `{queue}_errors` ([Kafka engine settings](https://clickhouse.com/docs/engines/table-engines/integrations/kafka)), здоровье — W2-9.

**Плюсы**
- 0 дополнительных сервисов; ingestion описан DDL и полностью покрыт codegen/миграциями (C5, C7).
- После W1-3 точные чтения корректны при любых повторах: ключ дедупа детерминирован (партиция+offset), не зависит от границ блока (C1).
- Шардирование естественно по партициям, MV пишут в `*_local` → «offset закоммичен ⇒ строка в реплицируемой таблице» во всех трёх топологиях (C8).
- DLQ в ClickHouse-таблице с `_raw_message`/`_error` (C3), алерты и lag из W2-9 (C9).

**Минусы**
- **Не exactly-once.** Дубли физически пишутся и живут до слияния; любые чтения без `FINAL` и любые insert-triggered MV видят повторы (C6). Поэтому агрегаты — полный пересчёт по `FINAL` раз в минуту: стоимость растёт с объёмом `orders`, свежесть агрегатов ≥ интервала refresh.
- **CPU ingestion в CH.** Парсинг Protobuf, синхронная MV-цепочка и UDF-декод ключа (`arrayFold` по байтам, SP-16) исполняются на нодах, обслуживающих запросы (C2). Изоляция — только через отдельные ноды/реплики под ingestion.
- **Schema Registry не используется**: schema id игнорируется, схема берётся из `format_schemas`, конверт пропускается фиксированными 6 байтами → инвариант «сериализуемый message первый в `.proto`» (SP-4/SP-17) и раскладка `.proto` по нодам (R7) остаются (C4).
- DLQ — таблица в ClickHouse, а не топик Kafka: переигровка битых сообщений — вручную (C3).
- Каждое изменение proto → recreate очереди и MV (DETACH/await/DROP/CREATE), не-транзакционный DDL (T2).

### B. ClickHouse Kafka Connect Sink (exactly-once через KeeperMap)

Официальный коннектор ClickHouse ([docs](https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink)) в отдельном Kafka Connect-кластере (JVM, Kafka Connect ≥ 2.7, Java ≥ 11; ClickHouse ≥ 23.3). `exactlyOnce=true` хранит состояние партиций в KeeperMap-таблице `connect_state` и опирается на дедупликацию вставок ClickHouse: при сбое повторяется *идентичный* батч, который отбрасывается по хешу блока ([design](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md)).

**Плюсы**
- Exactly-once на уровне вставки (в пределах окна дедупа) — можно читать без `FINAL` и вернуть инкрементальные агрегаты (C6, C1).
- Нативный Schema Registry через `io.confluent.connect.protobuf.ProtobufConverter`: schema id учитывается, `skip_bytes` и `format_schemas` не нужны (C4).
- DLQ в топик Kafka: `errors.tolerance=all` + `errors.deadletterqueue.topic.name` (C3); JMX-метрики (lag, errors, DLQ) (C9).
- Ingestion вне ClickHouse (только INSERT по HTTP) — CPU парсинга уходит из CH (C2). Нет queue/MV-recreate при миграциях.

**Минусы**
- Новая платформа: Connect-кластер, плагины/конвертеры (Protobuf-конвертер не во всех сборках), своя конфигурация, деплой и on-call; JVM-стек вне компетенции .NET-команды (C5).
- Условия exactly-once: буферизация (`bufferCount`) несовместима с `exactlyOnce=true`; с async insert только `wait_for_async_insert=1`; при перемотке offset надо чистить KeeperMap (`tolerateStateMismatch`); целевая таблица должна вести лог дедупа — `Replicated*` по умолчанию, plain `MergeTree` только с `non_replicated_deduplication_window > 0` ([dedup on retries](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries)); KeeperMap требует `keeper_map_path_prefix` в конфиге сервера ([KeeperMap](https://clickhouse.com/docs/engines/table-engines/special/keeper-map)) и `keeperOnCluster` для self-hosted кластера (C6, C8).
- **Codegen теряет половину смысла** (C7): маппинг Protobuf → колонки делает конвертер (nested → `Tuple`/`Array`, `oneof` — отдельные nullable-поля, без `Variant`), а не `DenormalizationPlanner`. Денормализация из codegen (flatten, wrappers, `oneofPresence`) либо переносится в MV над staging-таблицей, либо меняется форма таблиц. Kafka-DDL генератора становится не нужен; MergeTree/MV-миграции остаются.
- Ключ и meta: партиция/offset — стандартным SMT `InsertField` (`partition.field`, `offset.field`); переноса полей **ключа** в value в стандартном наборе SMT Apache Kafka нет — нужен сторонний или свой SMT (C7).
- Мульти-топология (C8): коннектор вставляет в одну таблицу на топик; в кластере — либо `Distributed` (дедуп и «ack ⇒ данные на шарде» зависят от синхронной пересылки, требует проверки), либо прямые вставки в `*_local` одного шарда без шардирования по ключу.

### C. Собственный .NET consumer (batch insert + токены дедупа)

Сервис на `Confluent.Kafka` + `Confluent.SchemaRegistry.Serdes.Protobuf` (уже в `Directory.Packages.props`) и клиенте ClickHouse (`ClickHouse.Client`, либо официальный `ClickHouse.Driver` — [docs](https://clickhouse.com/docs/integrations/csharp)). Батчи строятся детерминированно по партиции; вставка с `insert_deduplication_token = '<topic>:<partition>:<first>-<last>'` (та же схема, что у ClickPipes exactly-once); offset коммитится вручную только после подтверждения вставки. Опционально `async_insert=1, wait_for_async_insert=1`: с 26.2 ClickHouse дедуплицирует и асинхронные вставки, включая зависимые MV ([async inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)).

**Плюсы**
- Exactly-once в пределах окна дедупа (`replicated_deduplication_window` / `_seconds` или `non_replicated_deduplication_window`), включая MV-цели (`deduplicate_blocks_in_dependent_materialized_views`) (C6).
- Нативный SR-десериализатор для value **и ключа** — уходят SQL UDF `protobufWire*` (R5, R14, SP-16) и `skip_bytes` (C4).
- Codegen остаётся .NET: тот же `MessageDescriptor`, можно генерировать C#-маппинг строк из того же плана, что и DDL (C7). DLQ — в топик или таблицу, по выбору (C3). Метрики — в существующий стек W2-9 (C9).
- Масштабирование ingestion отдельно от CH (C2).

**Минусы**
- Свой сервис: код консьюмера, ребаланс (`partitions revoked` → сброс незакоммиченных батчей), ретраи с теми же границами батча, backpressure, деплой и on-call (C5). Это тот же класс логики, который в варианте B уже написан и поддерживается ClickHouse.
- Exactly-once держится на дисциплине реализации: батч при повторе обязан покрывать тот же диапазон offset'ов, повтор должен уложиться в окно дедупа (C6).
- Мульти-топология (C8) — те же вопросы, что у B: шардирование и гарантия доставки при вставке через `Distributed` или маршрутизация по шардам в приложении.
- Новый runtime-контракт генератора (C#-маппинг) — расширение `ClickHouseSchemaGen`, а не только конфигурация.

### D. ClickPipes (ClickHouse Cloud)

Managed-ingestion в Cloud ([create Kafka ClickPipe](https://clickhouse.com/docs/integrations/clickpipes/kafka/create-kafka-clickpipe)). По умолчанию at-least-once; **опционально exactly-once**: high-water mark + pending ranges во внутреннем state store и детерминированный токен `topic:partition:firstOffset-lastOffset`, ограниченный окном `replicated_deduplication_window` (10 000 блоков) / `replicated_deduplication_window_seconds` (1 ч) целевой таблицы ([best practices → delivery semantics](https://clickhouse.com/docs/integrations/clickpipes/kafka/best-practices)). Нативный SR, structured keys, error table, масштабирование отдельно от сервиса.

> Уточнение к [clickpipes-how-to.md](../clickpipes-how-to.md): там ClickPipes описан как только at-least-once; актуальные docs добавляют опциональный exactly-once режим.

**Плюсы** — всё из B (EOS, SR, DLQ/error table, изоляция CPU) без собственного Connect-кластера (C5); Terraform/OpenAPI.

**Минусы** — **только ClickHouse Cloud**; self-hosted compose-стенд и кластерные топологии репозитория (C8) неприменимы; маппинг Protobuf делает ClickPipes, денормализация codegen не переносится 1:1 (C7); сетевой доступ Cloud → Kafka/SR (PrivateLink/туннель).

### Отклонены без детального разбора

- **Kafka2 (`StorageKafka2`)** — experimental, не EOS; см. [kafka2-production-assessment.md](../kafka2-production-assessment.md).
- **Vector / Redpanda Connect** — ограниченная поддержка Protobuf + SR, at-least-once; не решает C4/C6 лучше A.

## 4. Сравнение

| Критерий | A. Kafka engine + W1-2/W1-3/W2-9 | B. Kafka Connect Sink | C. .NET consumer | D. ClickPipes |
| --- | --- | --- | --- | --- |
| C1 дубли (точные чтения) | Да, через `FINAL` / refresh | Да, без `FINAL` | Да, без `FINAL` | Да (EOS-режим) |
| C2 CPU вне CH | Нет | Да | Да | Да |
| C3 DLQ | Таблица `{queue}_errors` | Топик DLQ | На выбор | Error table |
| C4 SR / schema id | Нет (`skip_bytes`) | Да | Да | Да |
| C5 сложность эксплуатации | Минимальная | Высокая (JVM Connect) | Средняя (свой сервис) | Низкая, но Cloud |
| C6 exactly-once | Нет | Да, в окне дедупа | Да, в окне дедупа | Да, в окне дедупа |
| C7 codegen | Полностью | Частично (MergeTree/MV) | Полностью + C#-маппинг | Частично |
| C8 мульти-топология | Все 3, проверено | Нужно проектировать | Нужно проектировать | Только Cloud |
| C9 наблюдаемость | W2-9 | JMX | Своя (в стек W2-9) | Cloud UI / API |

## 5. Решение (предлагаемое)

**Остаться на Kafka engine (вариант A)** после влития W1-2, W1-3, W1-4, W1-7, W2-3, W2-9. Внешний consumer не вводить, пока не сработает один из триггеров ниже.

Обоснование: после W1-3 требование C1 (корректные точные чтения) выполнено без новой инфраструктуры; R2 и R5 закрыты W1-2 и W1-4; наблюдаемость — W2-9. Оставшиеся минусы A (CPU в CH, нет SR, нет EOS на уровне вставки) пока не подтверждены как проблема измерениями, а цена выхода из A — новая runtime-платформа (B) или новый сервис (C) и частичная потеря codegen.

**Условия решения:**
1. W1-2, W1-3, W1-4 влиты; E2E (W1-1) зелёный во всех трёх топологиях.
2. Чтения, требующие точности, идут через `FINAL` или агрегаты из refreshable MV; это задокументировано для потребителей данных.
3. Алерты W2-9 (`no commits`, `parse errors`, `no assignments`) включены.

**Триггеры пересмотра** (пороги — начальные, калибруются нагрузочным стендом W3-5 на ×10 потоке):

| # | Триггер | Как измерять | Куда смотреть |
| --- | --- | --- | --- |
| TR-1 | Нужны чтения без `FINAL` или инкрементальные insert-triggered агрегаты (бизнес-требование) | — | C (self-hosted) или D (Cloud) |
| TR-2 | Refresh `orders_agg_mv` p95 > 50 % интервала (> 30 с при `EVERY 1 MINUTE`), либо требуемая свежесть агрегатов < интервала | `system.view_refreshes` (W2-9) | TR-1 |
| TR-3 | Ingestion занимает > 30 % CPU ноды на пике, или p95 эталонных запросов (`ReadAggregates`) под ingestion хуже базового > 25 % | W3-5: прогон «запросы без ingestion» vs «с ingestion ×10», CPU ноды по `system.asynchronous_metrics` / cgroup | Сначала выделенные под ingestion реплики/ноды в A; если нельзя — C или B |
| TR-4 | Lag не возвращается к ≤ 60 с после всплеска ×10 при `kafka_num_consumers` = число партиций на ноду (SP-1, SP-2) | `system.kafka_consumers` + end offsets (W2-9) | Тюнинг `kafka_max_block_size`/`kafka_flush_interval_ms` (W3-5), затем C/B |
| TR-5 | Нужна переигровка битых сообщений из топика DLQ или проверка schema id / несколько типов сообщений в топике / message не первый в `.proto` (SP-4) | Требование | C (если .NET-команда) или B |
| TR-6 | Переезд на ClickHouse Cloud | — | D |
| TR-7 | Доля ошибок парсинга > 0,1 % сообщений в сутки устойчиво | `{queue}_errors` (W1-2, W2-9) | Разбор причин; при системной несовместимости схем — TR-5 |

Между B и C при срабатывании TR-1/TR-3/TR-5 предварительно предпочтителен **C**: команда и codegen уже на .NET, Confluent SR-сериализация уже в проекте, ключ декодируется без SQL UDF. B выбирать, если в организации уже есть эксплуатируемый Kafka Connect.

## 6. Последствия

**Положительные**
- Нет новых runtime-компонентов; codegen остаётся единственным источником DDL и миграций для всех трёх топологий.
- Семантика доставки становится явной: at-least-once на входе + идемпотентное хранение (W1-3) + DLQ-таблица (W1-2) + алерты (W2-9).

**Отрицательные / принимаемые риски**
- Дубли физически хранятся до слияния; потребители обязаны читать через `FINAL` или агрегаты. Стоимость `FINAL` и полного refresh растёт с объёмом (TR-2).
- CPU ingestion и UDF-декода ключа остаётся на нодах ClickHouse (TP-1, TP-6) — подтверждается или опровергается только W3-5 (TR-3).
- Schema id не валидируется; дисциплина раскладки `.proto` по нодам (R7) — через preflight миграций (W2-2), а не через SR.
- DDL-lifecycle очередей (DETACH/DROP/CREATE) остаётся частью каждой миграции (T2).

**Последующие действия**
- W3-5: снять метрики для TR-2..TR-4 и зафиксировать пороги в этом ADR (перевести в `Accepted` или `Superseded`).
- При срабатывании триггера — новый ADR с выбором C/B/D; для C — прототип consumer'а в `src/` с E2E-тестом exactly-once (рестарт после вставки до commit → `count() = uniqExact(kafka_partition, kafka_offset)` без `FINAL`).

## 7. Источники

- ClickHouse Kafka engine, `kafka_handle_error_mode`: https://clickhouse.com/docs/engines/table-engines/integrations/kafka
- ClickHouse Kafka Connect Sink (exactly-once, KeeperMap, DLQ, Protobuf, ограничения): https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink
- Дизайн exactly-once коннектора: https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md
- KeeperMap (`keeper_map_path_prefix`): https://clickhouse.com/docs/engines/table-engines/special/keeper-map
- Дедупликация вставок при ретраях (окна, `insert_deduplication_token`, MV): https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries
- Асинхронные вставки (дедуп с 26.2): https://clickhouse.com/docs/optimize/asynchronous-inserts
- ClickPipes for Kafka, delivery semantics: https://clickhouse.com/docs/integrations/clickpipes/kafka/best-practices
- ClickHouse C# client: https://clickhouse.com/docs/integrations/csharp
- Apache Kafka `InsertField` SMT: https://kafka.apache.org/documentation/#connect_transforms
