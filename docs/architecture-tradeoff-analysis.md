# kafka2ch: архитектурный trade-off анализ (ATAM)

Объект анализа: репозиторий `Tirael/kafka2ch`, `origin/main` @ `f00d285` (после PR #57/#58/#59). Все пути ниже относительно корня репозитория.
Связанные документы: [tla-tlc-generator-verification.md](./tla-tlc-generator-verification.md) (дефекты, найденные TLC), [kafka2-production-assessment.md](./kafka2-production-assessment.md), [clickpipes-how-to.md](./clickpipes-how-to.md).

---

## 0. Резюме (TL;DR)

**Главный архитектурный выбор:** ingestion целиком *внутри ClickHouse* (classic `ENGINE = Kafka` + MV → MergeTree), а сложность эволюции схемы вынесена в *build-time* инструмент (`ClickHouseSchemaGen`: proto → DDL, snapshot → diff → SQL-миграция, Migrator). Это даёт минимум движущихся частей в runtime и единый источник правды (`.proto` + `clickhouse.codegen.json`), но платит за это семантикой доставки (at-least-once без дедупа), хрупкостью DDL-операций над очередями и связностью «CPU ingestion = CPU запросов».

**Топ-5 trade-off'ов**
1. **Kafka engine vs внешний consumer** (Kafka Connect / Vector / свой .NET consumer / ClickPipes): простота эксплуатации и отсутствие отдельного сервиса ↔ at-least-once, нет DLQ по умолчанию, нет нативного Schema Registry (хак `skip_bytes = 6`), ingestion делит ресурсы с запросами.
2. **Codegen из скомпилированных C#-контрактов** ↔ привязка к .NET/рефлексии (`ProtoDescriptorResolver` в `tools/ClickHouseSchemaGen/ClickHouseSchemaGenerator.cs:242`) и к MSBuild (`tools/ClickHouseSchemaGen.Tasks/build/ClickHouseSchemaGen.Tasks.targets`).
3. **Snapshot-diff миграции с идентичностью по `FieldNumberPath`** ↔ snapshot на *каждую топологию* (3 каталога миграций уже разошлись), timestamp-версии + строгий линейный порядок → конфликты веток.
4. **DETACH/DROP/CREATE очередей в миграции** (MergeTree только `ALTER`) ↔ не-транзакционный DDL ClickHouse; восстановление best-effort через `SHOW CREATE` (`tools/ClickHouseSchemaGen.Migrator/MigrationRunner.cs:261-342`).
5. **Кластер: `ON CLUSTER` + ReplicatedMergeTree/Distributed vs Replicated database** ↔ размещение данных определяется назначением Kafka-партиций (MV пишут в `*_local`), а write-through `Distributed` в Replicated DB-режиме размывает гарантию «offset закоммичен ⇒ строка в MergeTree».

**Топ-риски**
- R1 Дубликаты при ребалансе/рестарте (at-least-once) в `MergeTree` `orders` и пересчёт в `SummingMergeTree` `orders_agg_1m` — нет `_partition/_offset`, нет Replacing/дедупа (`docker/clickhouse/init/02_pipeline.sql:1-16, 99-118`).
- R2 «Ядовитое» сообщение останавливает консьюмер: в генераторе нет `kafka_handle_error_mode` / `kafka_skip_broken_messages` (`tools/ClickHouseSchemaGen/Models/CodegenConfig.cs`, `KafkaSettingsConfig`).
- R3 Дрейф версий ClickHouse: тесты на `26.5.6.64` (`tests/ClickHouseSchemaGen.IntegrationTests/TestContainersEnv.cs:12`), runtime на `26.6.3.62`, README требует ≥ 26.6.
- R4 В CI нет end-to-end проверки Kafka → ClickHouse (ни одного Kafka-контейнера в integration-тестах; `scripts/verify-*.sh` запускаются вручную).
- R5 Replicated DB-режим: SQL UDF `protobufWire*` создаются без `ON CLUSTER` и без `user_defined_zookeeper_path` (`docker/clickhouse-cluster/init-replicated-db/02_pipeline.sql:23-84`) — *потенциально* отсутствуют на остальных репликах (требует проверки, см. R5).

**Топ-рекомендации (P0)**
1. Дедуп/идемпотентность: персистить `_partition`/`_offset` (уже поддержано `persistKafkaMeta`), raw-таблица → `ReplacingMergeTree` по `(_partition, _offset)` или окно `insert_deduplication_token`; агрегаты строить с учётом повторов.
2. Политика ошибок парсинга: добавить в `KafkaSettingsConfig` `handleErrorMode` (`stream` / `dead_letter_queue`) + MV ошибок + алерт.
3. Выровнять версию ClickHouse между тестами, compose и README; одна переменная/константа.
4. E2E-тест в CI: Testcontainers Kafka + Schema Registry + ClickHouse, публикация реального Confluent-Protobuf (с ключом), проверка `orders`/агрегатов и миграции с recreate очереди под нагрузкой.

---

## 1. Контекст и границы

| Компонент | Код | Роль |
| --- | --- | --- |
| Producer / reader | `src/Sandbox.App/Features/PublishOrders/*`, `.../ReadAggregates/*` | Confluent Protobuf serializer + Schema Registry; чтение агрегатов через `GROUP BY` |
| Контракты | `src/Sandbox.Contracts/protos/*.proto`, `clickhouse.codegen*.json` | единый источник схемы |
| Генератор | `tools/ClickHouseSchemaGen/**` | mapping proto → CH-типы, план, DDL, snapshot, diff, policy, SQL миграций |
| MSBuild / CLI | `tools/ClickHouseSchemaGen.Tasks`, `tools/ClickHouseSchemaGen.Cli` | codegen на `dotnet build`, `generate` / `migrate` |
| Migrator | `tools/ClickHouseSchemaGen.Migrator/MigrationRunner.cs` | применение `*.sql`, `schema_migrations`, lifecycle очередей |
| Runtime DDL | `docker/clickhouse/init/*.sql`, `docker/clickhouse-cluster/init*/` | сгенерированные init-скрипты |
| Кластер | `docker-compose.cluster*.yml`, `docker/clickhouse-cluster/config/*.xml`, `bootstrap.sh` | 2 shard × 2 replica + 3 Keeper |
| Тесты / CI | `tests/**`, `.github/workflows/ci.yml` | unit (~150 фактов), integration на Testcontainers (16), Linux+Windows |

Поток данных: `PublishOrders` → Kafka `orders` (Confluent wire format) → `orders_queue` (`ENGINE = Kafka`, `ProtobufSingle`, `kafka_schema_registry_skip_bytes = 6`, `docker/clickhouse/init/01_orders_queue.sql:27-38`) → `orders_mv` → `orders` (MergeTree) → `orders_agg_mv` → `orders_agg_1m` (SummingMergeTree) (`docker/clickhouse/init/02_pipeline.sql`).

---

## 2. Бизнес-драйверы

1. **D1 — Аналитика по событиям с минимальной задержкой и минимумом инфраструктуры.** Нет отдельного ingestion-сервиса; Kafka → ClickHouse «встроенными средствами» (`PLAN.md`, «Цель»).
2. **D2 — Единый контракт Protobuf + Schema Registry** для продьюсеров и ClickHouse без ручного дублирования схемы (`PLAN.md`, «Ключевое техническое решение»).
3. **D3 — Эволюция схемы без потери данных**, воспроизводимо и проверяемо в CI (`MIGRATIONS_PLAN.md`, «Договорённости»: «никто не правит схему вручную; живая база == init + миграции»).
4. **D4 — Горизонтальное масштабирование и отказоустойчивость** (кластерный режим, Keeper ×3, `README.md` «Кластерный ClickHouse»).
5. **D5 — Переиспользуемость генератора** вне репозитория (NuGet: library, MSBuild task, dotnet tools; `README.md` «NuGet-пакеты», `samples/NuGetCodegenSmoke`).
6. **D6 — Кросс-платформенная разработка** (Windows + Linux; канонический checksum, LF-политика; `.gitattributes`, `SqlChecksum`).

---

## 3. Quality attribute utility tree

Приоритет: (Важность для бизнеса, Сложность реализации) — H/M/L.

| Атрибут | Уточнение | Сценарий (стимул → ответ → мера) | Приоритет |
| --- | --- | --- | --- |
| **Корректность данных** | Отсутствие потерь | Рестарт ноды CH во время consume → после восстановления все сообщения в `orders`; потери = 0 | **(H, M)** |
| | Отсутствие дубликатов | Ребаланс consumer group / сбой после insert до commit → `count() = uniqExact(order_id)`; агрегаты не завышены | **(H, H)** |
| | Корректный декод ключа | Protobuf-ключ c Confluent-конвертом → `kafka_key.order_id` совпадает с value `order_id` в 100% строк | (M, M) |
| **Эволюционируемость** | Добавление поля proto | Новое поле в `OrderEvent` → `dotnet build` падает на drift → `migrate` генерирует `ADD COLUMN` + recreate queue/MV; без ручного SQL; данные MergeTree сохранены | **(H, M)** |
| | Опасные изменения | Смена типа ключевой колонки / удаление поля → миграция помечена manual, snapshot не двигается без `--allow-manual` | **(H, M)** |
| | Параллельная разработка | Две ветки добавляют по миграции → merge без ручной перенумерации, migrator применяет обе | (M, M) |
| **Доступность ingestion** | Устойчивость к «ядовитому» сообщению | Битый payload в топике → потребление продолжается, сообщение в DLQ, алерт ≤ 5 мин | **(H, L)** |
| | Миграция без простоя | Recreate очереди → пауза потребления ≤ 60 с (`ConsumerWaitTimeout`), lag догоняется, потерь нет | **(H, M)** |
| | Сбой посреди миграции | Ошибка между `DROP TABLE q` и `CREATE TABLE q` → очередь и MV восстановлены, потребление возобновлено | **(H, H)** |
| **Масштабируемость** | Рост throughput ×10 | Добавить партиции/ноды → линейный рост; ingestion не деградирует latency запросов > X% | (M, H) |
| **Эксплуатируемость** | Наблюдаемость | Lag, ошибки парсинга, состояние консьюмеров видны в метриках без `clickhouse-client` | (M, L) |
| | Конфигурация окружений | Смена брокеров/секретов на prod без новой schema-миграции | (M, M) |
| **Тестируемость** | Регрессии генератора | Любое изменение маппинга ловится unit/snapshot-тестами < 5 мин | (H, L) |
| | Реализм | Пайплайн Kafka→CH проверяется в CI на той же версии CH, что и prod | **(H, M)** |
| **Переносимость / reuse** | Внешний consumer пакета | Проект без доступа к репо подключает `ClickHouseSchemaGen.Tasks` и генерирует DDL | (M, M) |
| | Не-.NET продьюсеры | Контракт из `.proto`/descriptor set без C#-сборки | (L, M) |
| **Безопасность** | Секреты | SASL/TLS-учётки Kafka не попадают в сгенерированный DDL / git | (M, M) |

---

## 4. Ключевые архитектурные решения

### AD-1. Ingestion: classic Kafka engine + Materialized View → MergeTree

**Решение.** `KafkaTableGenerator` всегда генерирует `ENGINE = Kafka` с фиксированным набором settings (`tools/ClickHouseSchemaGen/Generation/KafkaTableGenerator.cs`; модель `KafkaSettingsConfig` в `tools/ClickHouseSchemaGen/Models/CodegenConfig.cs`: broker list, topic, group, `skipBytes`, `numConsumers`, `flattenNested`, два protobuf-флага). Offsets — в Kafka consumer group. MV — единственный путь из очереди в хранилище.

**Альтернативы.**

| Вариант | Плюсы | Минусы | Вердикт для kafka2ch |
| --- | --- | --- | --- |
| **Kafka engine (classic)** — текущий | 0 доп. сервисов; DDL-управляемо; consumer group на каждой ноде → естественный шардинг по партициям | at-least-once; нет нативного SR (нужен `skip_bytes`); ошибки парсинга блокируют поток без `kafka_handle_error_mode`; CPU ingestion в CH; DDL-recreate при каждом изменении proto | Оставить для self-hosted, но закрыть R1/R2 |
| **Kafka2 / `StorageKafka2`** (experimental) | Offsets в Keeper, retryable-дедупликация батчей | Официально experimental; ограничения на MV/реплики | Не для prod — см. [kafka2-production-assessment.md](./kafka2-production-assessment.md) |
| **ClickPipes** | Managed, нативный Schema Registry (без `skip_bytes`), DLQ, масштаб отдельно от сервиса | Только ClickHouse Cloud; генерация Kafka-DDL теряет смысл; остаются MergeTree/MV-миграции | Опция при переезде в Cloud — см. [clickpipes-how-to.md](./clickpipes-how-to.md) |
| **Kafka Connect + ClickHouse Sink** | Exactly-once (state в KeeperMap), SR-конвертеры (ключ декодируется нативно), DLQ, изоляция ресурсов | Отдельный Connect-кластер (JVM), ещё одна система конфигураций; эволюция таблиц всё равно на вас | Лучший вариант при жёстком требовании «без дублей» |
| **Vector / Redpanda Connect** | Лёгкий, трансформации, DLQ, метрики | Protobuf+SR поддержка ограничена/плагинная; at-least-once | Если нужна предобработка/роутинг |
| **Свой .NET consumer** (`Confluent.Kafka` + `ClickHouse.Client` bulk insert) | Полный контроль: SR-десериализация, дедуп-токены `insert_deduplication_token`, ретраи, DLQ, ключ без SQL UDF | Свой сервис: код, деплой, масштабирование, мониторинг | Разумно, если команда .NET и нужна строгая семантика |

**Sensitivity points.**
- SP-1 `kafka_num_consumers` × число нод vs число партиций (`docker-compose.cluster.yml`: 4 партиции на 4 ноды) — избыточные консьюмеры простаивают, недостаток — lag.
- SP-2 Размер/таймаут блока flush (`kafka_max_block_size`, `kafka_flush_interval_ms`) — задержка vs размер парта; генератор их не параметризует (в репо упоминание только в `PLAN.md:387`).
- SP-3 Тяжесть MV-цепочки: MV исполняются синхронно в потоке консьюмера → любая дорогая MV (в т.ч. UDF-декод ключа, AD-6) снижает throughput.

**Trade-offs.** TP-1: простота runtime ↔ семантика доставки и изоляция ресурсов.

**Риски.** R1 (дубликаты), R2 (poison message), R6 (секреты/брокеры в DDL — см. AD-3).
**Не-риски.** NR-1: потеря данных при recreate очереди закрыта процедурой `DETACH` → `await:kafka_consumers_empty` → recreate; offsets коммитятся после записи блока в MV (`MigrationSqlGenerator.cs:33-37`, `MigrationRunner.cs:379-409`).

---

### AD-2. Confluent wire format через `kafka_schema_registry_skip_bytes = 6` и локальные `.proto`

**Решение.** ClickHouse парсит `ProtobufSingle` по `format_schemas/*.proto`, пропуская 6 байт конверта (magic + schema id + 1 байт message-index). `.proto` синхронизируются MSBuild-таргетом `SyncProtoSchemas` в `docker/clickhouse/format_schemas/` (`PLAN.md`, раздел про импорты).

**Альтернативы.** (a) `ProtobufConfluent` в CH — отсутствует (PR #94750 закрыт); (b) продьюсер без конверта (`PLAN.md`, «План Б») — теряется совместимость с остальными SR-консьюмерами; (c) внешний consumer с SR-десериализатором (AD-1).

**Sensitivity points.**
- SP-4 Фиксированная длина конверта валидна только если сериализуемый message — **первый** в `.proto` (иначе message-index > 1 байта). Это инвариант контракта, не проверяемый ни генератором, ни CI.
- SP-5 ClickHouse **игнорирует schema id**: схема берётся с диска. Порядок деплоя «`.proto` на все ноды → миграция → продьюсер» нигде не принуждается; Migrator не проверяет наличие/версию format schemas.

**Trade-off.** TP-2: совместимость с экосистемой SR ↔ ручная дисциплина раскладки `.proto` по нодам.
**Риск.** R7: рассинхрон format_schemas между нодами кластера → разный парсинг/ошибки на части нод.
**Не-риск.** NR-2: forward-совместимость — новые поля, неизвестные CH, protobuf-парсер пропускает; `ProtoCompatibilityValidator` блокирует несовместимые изменения (`tools/ClickHouseSchemaGen/Migration/ProtoCompatibilityValidator.cs`).

---

### AD-3. Codegen proto → DDL (рефлексия по сборкам, MSBuild task, NuGet)

**Решение.** Генератор получает `MessageDescriptor` из скомпилированных C#-типов (`ProtoDescriptorResolver`, `tools/ClickHouseSchemaGen/ClickHouseSchemaGenerator.cs:242-260`), маппит поля стратегиями (`tools/ClickHouseSchemaGen/Mapping/*Strategy.cs`, `ClickHouseTypeResolver.cs`, `DenormalizationPlanner.cs`), строит `ResolvedSchemaPlan` и рендерит DDL. MSBuild task загружается через `RoslynCodeTaskFactory` из shadow-копии под `obj/` (`ClickHouseSchemaGen.Tasks.targets:64-141`), на build проверяется drift snapshot'а. Конфиги наследуются через `extends` (`clickhouse.codegen.cluster.json` → `clickhouse.codegen.json`). Валидация — FluentValidation (`tools/ClickHouseSchemaGen/Validation/CodegenConfigValidator.cs`).

**Альтернативы.**

| Вариант | Плюсы | Минусы |
| --- | --- | --- |
| **Codegen из C#-сборок** — текущий | Тот же descriptor, что использует продьюсер; build-time drift-check; типобезопасность конфига | Только .NET; рефлексия и `Assembly.LoadFrom` в MSBuild-ноде (блокировки, MSB3027/MSB3231 — обходятся shadow-копией); нужен build контрактов перед генерацией |
| Codegen из `.proto` / descriptor set (`protoc --descriptor_set_out`, `buf`) | Язык-независимо; не нужен build C# | Нужно встроить protoc/buf; дубль резолва импортов |
| Ручной DDL | Полный контроль, любые фичи CH | Дублирование схемы, дрейф proto↔DDL, нет автоматических миграций |
| Внешние инструменты (dbt-clickhouse, Atlas/Liquibase-подобные) | Зрелые миграции, plan/apply | Не знают про proto/Kafka engine, ключ/meta |

**Sensitivity points.**
- SP-6 Правила маппинга (`maxFlattenDepth`, `repeatedMessageStrategy`, `optionalAsNullable`, `oneofPresence`, `enumMaxValuesForEnum8`) меняют *форму таблиц*; смена дефолта = массовая миграция.
- SP-7 Окруженческие значения в схеме: `brokerList`, `groupName` зашиты в DDL и в snapshot (`docker/clickhouse/init/schema.snapshot.json`), и их смена порождает `KafkaTableChanged` → recreate (`SchemaDiffer.cs:77-84`). Нет поддержки named collections / SASL / TLS settings.

**Trade-off.** TP-3: единый источник правды и автоматизация ↔ замыкание на .NET-экосистему и ограниченный набор поддерживаемых settings (всё, что не моделировано, уходит в `trailingSql` или требует доработки генератора).
**Риски.** R6: секреты/адреса окружения в сгенерированном коде; R8: «escape hatch» `trailingSql` непрозрачен для diff (только `TrailingSqlChanged` → manual, `MigrationSqlGenerator.cs:232-235`).
**Не-риски.** NR-3: защита от дрейфа — build падает, если план ≠ snapshot (`SnapshotDriftChecker`), а коллизии имён объектов теперь валидируются (`CodegenConfigValidator.cs:36-37, 52, 61-62, 73-101`; исправление TLA-дефекта 4.1).

---

### AD-4. Snapshot-based миграции (diff плана, идентичность по номерам полей)

**Решение.** Committed `schema.snapshot.json` = разрешённый план; `migrate` = `ToPlan(oldSnapshot)` → `BuildPlan(config)` → `ProtoCompatibilityValidator` → `SchemaDiffer` → `MigrationPolicy` (auto/manual) → `MigrationSqlGenerator`; snapshot сдвигается только при отсутствии manual-изменений или с `--allow-manual` (`tools/ClickHouseSchemaGen/Migration/MigrationOrchestrator.cs:49-116`). Идентичность колонки — `FieldNumberPath` (rename = `RENAME COLUMN`). Автоматика: `ADD`, `RENAME`, `MODIFY` для Safe/Rewrite (`MigrationSqlGenerator.cs:57-85`); Kafka-таблицы и MV пересоздаются, MergeTree — только `ALTER`. Snapshot цепочкой `parentChecksum`.

**Альтернативы.**

| Вариант | Плюсы | Минусы |
| --- | --- | --- |
| **Snapshot-diff** — текущий | Автогенерация безопасных изменений; ревью SQL в PR; детерминизм; формально проверено TLC | Snapshot ≠ живая БД (нет introspection); per-topology snapshots; ограниченный набор change-типов |
| Ручные миграции (Flyway-style) | Любые операции, явный контроль | Дрейф proto↔DDL, ошибки rename/ORDER BY ловятся только в рантайме |
| Diff против живой БД (introspection `system.columns`) | Видит реальный дрейф | Нужен доступ к prod из CI; невоспроизводимо; сложно для кластера |
| Diff двух `.pb` (descriptor sets) | Без snapshot-файла | Явно отклонено в `MIGRATIONS_PLAN.md` (нет expanded-плана, meta, MV) |

**Sensitivity points.**
- SP-8 `TypeCompatibility` определяет, что уходит в автоматику. После #57 знаковость учитывается (`TypeCompatibility.cs:52-56`), но `Rewrite` (DateTime→DateTime64, Enum8→Enum16) применяется **автоматически** как `MODIFY COLUMN` = мутация, без `mutations_sync` и без оценки объёма.
- SP-9 Версия = UTC-timestamp `yyyyMMddHHmmss` (`MigrationOrchestrator.cs:86-88`) + Migrator отвергает pending-миграцию старше последней применённой (`MigrationRunner.cs:77-85`) → ветка, смёрженная позже, но сгенерированная раньше, **блокирует deploy**.
- SP-10 Snapshot не хранит `cluster` и `key` конфиг: `ToPlan` собирает `CodegenConfig` без `Cluster` и подставляет `Key` из шаблона (`SchemaSnapshotMapper.cs:72-79, 201-223`). Смена топологии или key-формата не детектируется как изменение (частично видна через колонки `_key.*`).
- SP-11 По snapshot'у и каталогу миграций **на каждую топологию** (`clickhouse.codegen.json`, `.cluster.json`, `.cluster.replicated-db.json`). Уже сейчас single-node содержит 2 миграции, кластерные — 1 (`docker/clickhouse/migrations/` vs `docker/clickhouse-cluster/migrations*/`).

**Trade-off.** TP-4: детерминированный, ревьюируемый, формально проверенный diff ↔ snapshot как *модель*, а не факт (живая БД может разойтись), и N копий истории для N топологий.
**Риски.** R9 (конфликты веток/линейность), R10 (дорогие автоматические мутации), R11 (расхождение топологий).
**Не-риски.** NR-4: все дефекты diff/policy из TLA-отчёта (§2.1–2.5, §3.1–3.2) исправлены — см. §5.

---

### AD-5. Migrator: `schema_migrations`, канонический checksum, lifecycle очередей

**Решение.** Однопроходный раннер: `EnsureMigrationsTable` → загрузка applied (`kind = 'migration'`) → для каждого файла по имени: проверка checksum (SHA-256 канонического текста `SqlChecksum`: LF, без BOM и хвостовых пробелов), запрет out-of-order, применение по statement'ам, запись в историю (`MigrationRunner.cs:43-92`). Маркер `-- await:kafka_consumers_empty` ждёт до 60 с пустого `system.kafka_consumers` (в т.ч. `clusterAllReplicas`). Перед `DETACH/DROP` снимается `SHOW CREATE`; при ошибке — `RestoreDroppedObjectsAsync` и `ReattachDetachedQueuesAsync` (`MigrationRunner.cs:143-202, 261-377`). Init-скрипты помечают уже вшитые миграции как applied (`docker/clickhouse/init/00_schema_migrations.sql`, `applied_at = toDateTime(0)`) — baseline-модель.

**Альтернативы.** Flyway/Liquibase с ClickHouse-драйвером (нет знания про очереди и `ON CLUSTER`-нюансы); `clickhouse-migrations`/golang-migrate (то же); применение миграций из приложения на старте (отклонено в `MIGRATIONS_PLAN.md` — отдельный one-shot сервис `clickhouse-migrate`, `docker-compose.yml:66-82`).

**Sensitivity points.**
- SP-12 Нет блокировки: два параллельных Migrator'а (два деплоя, два пода) применят одну миграцию дважды; single-node история — обычный `MergeTree ORDER BY version` без уникальности.
- SP-13 Не-атомарность «apply → record»: сбой после последнего statement'а и до `RecordAppliedAsync` → повторное применение. Компенсируется идемпотентностью SQL (`ADD COLUMN IF NOT EXISTS`, `RENAME COLUMN IF EXISTS`, `DROP ... IF EXISTS` + `CREATE`).
- SP-14 Восстановление после сбоя — best-effort: ошибки `SHOW CREATE` и restore глотаются/логируются (`MigrationRunner.cs:279-281, 326-340`); MV восстанавливаются старым DDL поверх, возможно, уже *новой* очереди.
- SP-15 `ON CLUSTER` DDL частично успешен (часть нод), Migrator этого не различает.

**Trade-off.** TP-5: простой, понятный раннер со знанием Kafka-специфики ↔ отсутствие транзакций/локов и «не-атомарный» lifecycle очереди.
**Риски.** R12 (двойное применение), R13 (частичное восстановление/частичный `ON CLUSTER`).
**Не-риски.** NR-5: CRLF/LF не ломает checksum (`SqlChecksum`, `tests/ClickHouseSchemaGen.UnitTests/SqlChecksumTests.cs`); NR-6: TLA-дефект 5.1 (`DROP` → сбой `CREATE`) смягчён восстановлением из сохранённого DDL и покрыт integration-тестом (`tests/ClickHouseSchemaGen.IntegrationTests/MigrationQueueLifecycleIntegrationTests.cs:101`).

---

### AD-6. Kafka key / meta: `persistKafkaMeta` + декод protobuf-ключа SQL UDF

**Решение.** Виртуальные колонки `_key`, `_headers`, опц. `_topic/_partition/_offset/_timestamp_ms` пробрасываются MV в обычные колонки MergeTree (`KafkaMetaColumnFactory`, `FieldNumberPath` вида `kafka:_key`). Protobuf-ключ (`key.format = "protobuf"`) раскладывается на `kafka_key.<поле>` выражениями на SQL UDF `protobufWire*` поверх `substring(_key, skipBytes + 1)` (`tools/ClickHouseSchemaGen/Generation/ProtobufWireSqlFunctions.cs`, `KafkaKeyBindings.cs`; результат — `docker/clickhouse/init/02_pipeline.sql:18-96`).

**Альтернативы.** (a) хранить только сырой `kafka_key` и декодировать в приложении (исходный вариант `MIGRATIONS_PLAN.md`); (b) класть нужные поля ключа в value (дублирование в контракте, но бесплатно для CH); (c) второй Kafka-«взгляд» невозможен — engine парсит только value; (d) внешний consumer с SR-десериализацией ключа.

**Sensitivity points.**
- SP-16 Стоимость UDF: `protobufWireField` делает `arrayFold` по `range(length(msg))` с varint-разбором на каждом шаге — O(len(key)) на поле на строку, в синхронной MV (SP-3). Для коротких ключей приемлемо, для длинных/многопольных — заметно.
- SP-17 Тот же инвариант «первый message в файле» и фиксированный `skipBytes` для ключа (`substring(_key, 7)`).
- SP-18 UDF — серверные глобальные объекты, не принадлежат БД: их жизненный цикл не связан с `DROP DATABASE` и с Replicated database (см. R5).

**Trade-off.** TP-6: «всё в CH, без кода приложения» ↔ CPU-стоимость и самописный парсер wire-формата (поддержка packed/groups/неизвестных wire types минимальна — `length(m) + 1` как sentinel).
**Риски.** R5 (UDF в Replicated DB), R14 (корректность самописного парсера на краевых случаях: отрицательные int32 как 10-байтовый varint, packed repeated, sint/zigzag).
**Не-риски.** NR-7: TLA-дефекты рассинхрона meta между auto-MV/MergeTree и explicit-MV (§1.1–1.2) исправлены (`MaterializedViewAutoGenerator.cs:58`, `PipelineColumnExpander.cs:82-89`, `ClickHouseSchemaGenerator.cs:191, 207`); декод ключа покрыт `ProtobufKeyDecodingClickHouseIntegrationTests` на реальном CH.

---

### AD-7. Кластерный режим: `ON CLUSTER` + ReplicatedMergeTree/Distributed vs Replicated database

**Решение.** `ClusterDdl` (`tools/ClickHouseSchemaGen/Generation/ClusterDdl.cs`) поддерживает два `ddlMode`:
- `onCluster` (по умолчанию): `*_local` на `ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')`, `Distributed` поверх, все DDL с `ON CLUSTER`; Kafka-очередь и MV на каждой ноде с общим `kafka_group_name`; **MV пишут в `*_local`** (`docker/clickhouse-cluster/init/02_pipeline.sql:85-96`).
- `replicatedDatabase`: `CREATE DATABASE ... ENGINE = Replicated`, DDL без `ON CLUSTER`, `DETACH ... PERMANENTLY`, MV пишут **через `Distributed`** с `cityHash64(order_id)` (`src/Sandbox.Contracts/clickhouse.codegen.cluster.replicated-db.json`).
- История `schema_migrations` — один «shard all» Replicated-путь (`ClusterConfig.DefaultHistoryReplicatedPath`).

**Альтернативы.**

| Вариант | Плюсы | Минусы |
| --- | --- | --- |
| **ReplicatedMergeTree + Distributed + `ON CLUSTER`** | Классика, прозрачно; MV в `*_local` = гарантия «offset ⇒ строка в реплицируемой таблице» | Distributed DDL queue: частичные сбои; `ON CLUSTER` для каждого объекта, включая UDF; шардирование де-факто по Kafka-партициям (`rand()`-ключ только для прямых insert'ов) |
| **Replicated database** | DDL реплицируется автоматически, новые реплики догоняют схему | Ограничения (`DETACH PERMANENTLY`, не все DDL), UDF вне БД; write-through Distributed: асинхронная доставка по умолчанию |
| MV → `*_local` в обоих режимах | Сильнейшая гарантия доставки | Данные ложатся на шард консьюмера, а не по бизнес-ключу (co-location по `order_id` только если продьюсер партиционирует по ключу и партиции стабильно закреплены) |
| MV → `Distributed` (write-through) | Размещение по бизнес-ключу, локальные JOIN/агрегации по `order_id` | Доп. хоп; при `distributed_foreground_insert = 0` offset коммитится до доставки на целевой шард → риск потери при потере диска ноды; «дубль-усиление» при ретраях |
| ClickHouse Cloud / SharedMergeTree | Нет шардов/Keeper в зоне ответственности | Cloud only (+ ClickPipes вместо Kafka engine) |

**Sensitivity points.**
- SP-19 `materializedViewsWriteThroughDistributed` + `distributed_foreground_insert` — переключают гарантию доставки и размещение данных.
- SP-20 Назначение партиций консьюмерам (`kafka_group_name` общий) — при ребалансе меняется шард, куда падают новые строки ключа; SummingMergeTree-агрегаты per-shard, финальная сумма через Distributed корректна только с `GROUP BY` (что и делает `ReadAggregates`).
- SP-21 Keeper-ансамбль (3 ноды) — single point of coordination для реплик, Replicated DB и истории миграций.

**Trade-off.** TP-7: автоматическая репликация DDL (Replicated DB) ↔ слабее гарантия доставки и сложнее модель объектов; TP-8: локальная запись MV ↔ размещение данных не по бизнес-ключу.
**Риски.** R5, R11, R13, R15 (асинхронная Distributed-доставка в replicated-db режиме).
**Не-риски.** NR-8: кластерные варианты покрыты integration-тестами на 2 нодах + Keeper (`ClusterSchemaClickHouseIntegrationTests.cs`, `ClusterReplicatedDatabaseIntegrationTests.cs`), включая проверку колонок на всех репликах (`ClusterReplicatedDatabaseIntegrationTests.cs:84-95`).

---

### AD-8. `trailingSql` как escape hatch + `ClusterTrailingSqlRewriter`

**Решение.** Агрегаты (`orders_agg_1m`, `orders_agg_mv`) задаются строкой SQL в конфиге (`src/Sandbox.Contracts/clickhouse.codegen.json:92`); в кластере regex-переписчик превращает `CREATE TABLE ... MergeTree` в `*_local` + `Distributed`, MV — на `*_local`, неподдерживаемые statement'ы отвергает (`tools/ClickHouseSchemaGen/Generation/ClusterTrailingSqlRewriter.cs:5-50`).

**Альтернативы.** Моделировать агрегаты в конфиге (как MergeTree/MV), отдельные hand-written SQL-файлы вне генератора, dbt-модели.

**Sensitivity / trade-off.** SP-22: regex-переписывание чувствительно к форматированию SQL; TP-9: гибкость ↔ отсутствие diff (только hash → manual).
**Риск.** R8: изменение агрегата не порождает исполняемой миграции; переписчик может молча не распознать вариант синтаксиса (backticks, `db.table`, CTE).
**Не-риск.** Неподдерживаемые конструкции в кластере падают явно (`ClusterTrailingSqlRewriter.cs:45-47`).

---

### AD-9. Стратегия тестирования (unit + Testcontainers + формальная верификация)

**Решение.** Unit-тесты на каждый слой (маппинг, план, валидатор, diff/policy, SQL-генерация, split/checksum; `tests/ClickHouseSchemaGen.UnitTests/*`, ~150 `[Fact]/[Theory]`); integration на реальном ClickHouse через Testcontainers (single-node, 2-нодовый кластер + Keeper, Replicated DB, lifecycle очередей, декод ключа; 16 тестов). Тестовые контракты изолированы от `src/Sandbox.Contracts` и `docker/` (`tests/ClickHouseSchemaGen.TestContracts`, PR #58/#59). Дополнительно — TLA+/TLC-модели ([отчёт](./tla-tlc-generator-verification.md)).

**Альтернативы.** Golden-file / snapshot-тесты DDL (Verify), property-based (FsCheck) для diff/policy, docker-compose e2e в CI, контрактные тесты с SR.

**Sensitivity points.** SP-23 Версия образа CH в тестах (`TestContainersEnv.cs:12`: `26.5.6.64`) ≠ runtime (`README.md`: `26.6.3.62`, требование ≥ 26.6). SP-24 Ни один integration-тест не поднимает Kafka/Schema Registry: Kafka-таблицы создаются, но сообщения не потребляются; MV проверяются прямыми `INSERT` (`ClusterReplicatedDatabaseIntegrationTests.cs:67-70`).

**Trade-off.** TP-10: быстрые детерминированные тесты без брокера ↔ неподтверждённые в CI свойства самого ingestion (skip_bytes, декод реального Confluent-ключа, flush/commit при recreate, ребаланс).
**Риски.** R3, R4.
**Не-риски.** NR-9: высокое покрытие генератора и формальная проверка основных инвариантов; NR-10: тесты не зависят от артефактов `docker/` (PR #58).

---

### AD-10. CI

**Решение.** `.github/workflows/ci.yml`: матрица Linux (build + unit + integration) и Windows (build + unit, `core.autocrlf=false`), `concurrency` с отменой, артефакты TRX.

**Пробелы.** Нет: `scripts/pack-verify.sh` (упаковка NuGet + `samples/NuGetCodegenSmoke`), `docker compose` smoke (`scripts/verify-pipeline.sh`, `verify-cluster.sh`), проверки «сгенерированные init/миграции/snapshot актуальны» как отдельного шага (покрывается build'ом `Sandbox.Contracts` только если codegen включён), публикации пакетов/версионирования (`VersionPrefix 0.1.0` в `Directory.Build.props`), dependency/security scan, кэша NuGet.

**Trade-off.** TP-11: короткий и дешёвый CI ↔ D5 (NuGet reuse) и D1 (реальный пайплайн) не проверяются автоматически.
**Риск.** R4, R16 (регрессия упаковки Tasks/Cli/Migrator незаметна до ручного `pack-verify.sh`).

---

## 5. Статус дефектов из TLA+/TLC-отчёта на `f00d285`

Все дефекты отчёта адресованы PR #57 (`26fab62`). Проверено по коду:

| § отчёта | Дефект | Статус | Доказательство |
| --- | --- | --- | --- |
| 1.1 | auto-MV игнорирует `table.IncludeKafkaMeta` | Исправлен | `MaterializedViewAutoGenerator.cs:58` (`Resolve(queueMeta, null, original.IncludeKafkaMeta)`) |
| 1.2 | explicit-MV игнорирует `view.IncludeKafkaMeta` | Исправлен | `PipelineColumnExpander.cs:82-89`, `ClickHouseSchemaGenerator.cs:207` |
| 2.1 | Drift ≠ Diff (`SourceTable`, `Origin`) | Исправлен | `SchemaDiffer.cs:123-127, 175-183`; новый `SourceTableChanged` (`MigrationSqlGenerator.cs:79-82`) |
| 2.2 | удаление Kafka-таблицы роняет `ToPlan` | Исправлен | `SchemaSnapshotMapper.cs:40` (`TryGetValue`, без исключения) |
| 2.3 | добавленная Kafka-таблица не создаётся | Исправлен | `MigrationSqlGenerator.cs:114-130` (`AppendNewKafkaTables`) |
| 2.4 | no-op цикл ORDER BY / old vs new имя | Исправлен | `MigrationPolicy.cs:55-64, 104-118` (union old+new ORDER BY, проверка `OldName`/`NewName`) |
| 2.5 | новая первая колонка уходит в конец | Исправлен | `MigrationSqlGenerator.cs:258-260` (`FIRST`) |
| 3.1 | перезапись `FieldNumberPath` между таблицами | Исправлен | `ProtoCompatibilityValidator.cs:112-131` (словарь per table) |
| 3.2 | `Int64 → UInt8` = Safe | Исправлен | `TypeCompatibility.cs:52-56, 111-123` (signed/bits) |
| 4.1 | коллизии имён объектов | Исправлен | `CodegenConfigValidator.cs:36-37, 52, 61-62, 73-101` |
| 5.1 | невосстановимое состояние после `DROP` | **Смягчён** (best-effort) | `MigrationRunner.cs:149-202, 261-342`; остаточные риски — SP-14, R13 |

Вывод: формальная верификация окупилась (11 реальных дефектов на уровне инвариантов). Остаточный архитектурный риск сместился из *логики генератора* в *операционную семантику* (доставка, ошибки парсинга, конкурентный деплой, версии CH), которую TLA-модели не покрывали. Рекомендуется держать TLA-модели в репо и прогонять TLC в CI (P2-4).

---

## 6. Сводка sensitivity и trade-off points

**Sensitivity points:** SP-1 консьюмеры/партиции; SP-2 flush-параметры; SP-3 синхронные MV; SP-4/SP-17 инвариант «первый message» и фиксированный skip; SP-5 порядок деплоя `.proto`; SP-6 дефолты маппинга; SP-7 окруженческие значения в DDL; SP-8 auto-`Rewrite`; SP-9 timestamp-версии + линейность; SP-10 snapshot без cluster/key; SP-11 snapshot на топологию; SP-12 нет лока; SP-13 apply≠record; SP-14 best-effort restore; SP-15 частичный `ON CLUSTER`; SP-16 стоимость UDF; SP-18 UDF вне БД; SP-19 write-through Distributed; SP-20 ребаланс; SP-21 Keeper; SP-22 regex rewriter; SP-23 версия CH в тестах; SP-24 нет Kafka в тестах.

**Trade-off points** (одна точка влияет на несколько атрибутов):
- TP-1 Kafka engine: простота/латентность ↑ ↔ корректность (дубли)/доступность (poison)/изоляция ↓.
- TP-2 `skip_bytes`: SR-совместимость ↑ ↔ эксплуатационная дисциплина раскладки `.proto` ↓.
- TP-3 codegen из C#: эволюционируемость/единый источник ↑ ↔ переносимость на не-.NET ↓, покрытие settings ↓.
- TP-4 snapshot-diff: детерминизм/безопасность ↑ ↔ параллельная разработка, мульти-топология ↓.
- TP-5 Migrator: простота ↑ ↔ атомарность/конкурентность ↓.
- TP-6 UDF-декод ключа: «без кода приложения» ↑ ↔ throughput ingestion ↓, сопровождаемость ↓.
- TP-7 Replicated DB: эксплуатация DDL ↑ ↔ гарантии доставки (write-through) и UDF-согласованность ↓.
- TP-8 MV → `*_local`: доставка ↑ ↔ размещение по бизнес-ключу ↓.
- TP-9 `trailingSql`: гибкость ↑ ↔ эволюционируемость ↓.
- TP-10/TP-11 лёгкие тесты/CI: скорость ↑ ↔ реализм ↓.

---

## 7. Риски и не-риски

### Риски

| ID | Риск | Атрибут | Вероятн./Влияние | Код |
| --- | --- | --- | --- | --- |
| R1 | Дубли при ребалансе/рестарте → завышенные агрегаты | Корректность | H / H | `docker/clickhouse/init/02_pipeline.sql:1-16, 99-118`; `persistKafkaMeta.partition/offset = false` в `clickhouse.codegen.json:11-13` |
| R2 | Poison message останавливает потребление (нет `kafka_handle_error_mode`/`skip_broken_messages`) | Доступность | M / H | `KafkaSettingsConfig` в `Models/CodegenConfig.cs`; `KafkaTableGenerator.cs` |
| R3 | Дрейф версий CH: тесты 26.5.6.64 vs runtime 26.6.3.62 | Тестируемость/корректность | H / M | `TestContainersEnv.cs:12`, `README.md` (`.env`) |
| R4 | Нет e2e Kafka→CH в CI | Тестируемость | H / M | `.github/workflows/ci.yml`; integration-тесты без Kafka |
| R5 | Replicated DB: UDF создаются без `ON CLUSTER`, нет `user_defined_zookeeper_path` → на других репликах функций может не быть (MV упадёт при первом batch'е с ключом или при репликации DDL). Тест не ловит: Kafka не потребляется, MV-источник не срабатывает | Доступность | M / H (**требует проверки**) | `ClusterDdl.cs:108-111` (`UsesOnCluster` false), `init-replicated-db/02_pipeline.sql:23-84`, `docker/clickhouse-cluster/config/*.xml` |
| R6 | Брокеры/группа в DDL и snapshot; нет named collections/SASL → секреты в git или смена окружения = schema-миграция | Безопасность/эксплуатация | M / M | `SchemaDiffer.cs:77-84`, `schema.snapshot.json` |
| R7 | Рассинхрон `format_schemas` между нодами/с миграцией | Корректность | M / H | `PLAN.md` (`SyncProtoSchemas`); Migrator не проверяет |
| R8 | `trailingSql` вне diff; regex-rewriter | Эволюционируемость | M / M | `MigrationSqlGenerator.cs:232-235`, `ClusterTrailingSqlRewriter.cs` |
| R9 | Timestamp-версии + запрет out-of-order → блок деплоя после merge веток | Эволюционируемость | M / M | `MigrationOrchestrator.cs:86`, `MigrationRunner.cs:77-85` |
| R10 | Автоматические `MODIFY COLUMN` (Rewrite) = тяжёлые мутации без контроля | Доступность/производительность | L / H | `MigrationSqlGenerator.cs:72-78`, `TypeCompatibility.cs:28-29, 65-68` |
| R11 | Расхождение snapshot'ов/миграций между топологиями; snapshot не знает cluster/key | Эволюционируемость | H / M | `docker/clickhouse*/migrations*`, `SchemaSnapshotMapper.cs:72-79` |
| R12 | Конкурентный Migrator → двойное применение | Корректность схемы | L / M | `MigrationRunner.cs:43-92` (нет лока) |
| R13 | Частичное восстановление / частичный `ON CLUSTER` | Доступность | L / H | `MigrationRunner.cs:279-281, 326-340` |
| R14 | Самописный protobuf-парсер на SQL: краевые случаи wire-формата | Корректность | L / M | `ProtobufWireSqlFunctions.cs` |
| R15 | Write-through Distributed: offset коммитится до доставки на шард | Корректность | M / H | `clickhouse.codegen.cluster.replicated-db.json` (`materializedViewsWriteThroughDistributed: true`) |
| R16 | Упаковка NuGet не проверяется в CI | Переносимость | M / M | `scripts/pack-verify.sh` вне CI |

### Не-риски
NR-1 recreate очереди без потерь (DETACH → await → recreate); NR-2 forward-совместимость proto + блокировка несовместимых изменений; NR-3 build-time drift-check и валидация коллизий; NR-4 diff/policy исправлены и формально проверены; NR-5 канонический checksum кросс-платформенен; NR-6 восстановление после сбоя миграции покрыто тестом; NR-7 meta-консистентность MV/MergeTree; NR-8 кластерные DDL проверены на 2 нодах + Keeper; NR-9 высокое unit-покрытие генератора; NR-10 тесты изолированы от `docker/`; NR-11 миграции идемпотентны (`IF [NOT] EXISTS`), что компенсирует SP-13; NR-12 чтение агрегатов через `GROUP BY` поверх SummingMergeTree корректно при отложенных слияниях (`PLAN.md`, таблица решений).

---

## 8. Risk themes

1. **T1 — Семантика доставки не спроектирована явно** (R1, R2, R15). Архитектура опирается на at-least-once Kafka engine, но ни хранилище, ни агрегаты не рассчитаны на повторы и битые сообщения. Угрожает драйверам D1/D3 (корректная аналитика).
2. **T2 — Операционный жизненный цикл DDL вне транзакций** (R5, R7, R12, R13, SP-15). Миграции корректны «по модели», но деплой (порядок `.proto` → миграция, конкурентность, частичные сбои на кластере, глобальные UDF) не оркеструется. Угрожает D3/D4.
3. **T3 — Модель (snapshot) против реальности (БД, топологии, окружения)** (R6, R9, R11, SP-10). Snapshot — единственный источник «старого» состояния, но он не знает о топологии, окружении и живом дрейфе; история дублируется на каждую топологию.
4. **T4 — Тестовое покрытие сильное для генератора и слабое для пайплайна** (R3, R4, R16). Формальная верификация и unit-тесты закрыли логические ошибки; e2e-свойства ingestion и упаковки не проверяются автоматически.

---

## 9. Приоритизированные рекомендации

Сложность — по затрагиваемым компонентам и инвазивности.

### P0 — до использования вне демо

| # | Рекомендация | Закрывает | Что менять |
| --- | --- | --- | --- |
| P0-1 | **Идемпотентность хранения.** Включить `persistKafkaMeta.partition/offset` (уже есть в модели), raw-таблица `ReplacingMergeTree` с ключом, включающим `(kafka_partition, kafka_offset)` или бизнес-ключ+версию; агрегаты — `AggregatingMergeTree` с `uniqState`/пересборкой из дедуплицированного источника, либо refreshable MV поверх `FINAL`. Документировать семантику | R1, T1 | `clickhouse.codegen.json`, `MergeTreeTableGenerator` (engine уже параметризуем через `ClusterDdl.StorageEngine`), `trailingSql`; diff: смена engine = manual (`OriginChanged`/новое change-type) |
| P0-2 | **Политика ошибок парсинга.** Добавить в `KafkaSettingsConfig` `handleErrorMode` (`default`/`stream`/`dead_letter_queue`) и `skipBrokenMessages`; при `stream` — автогенерация MV `_error/_raw_message` → `*_errors`; учесть в `SchemaDiffer.CollectKafkaChangeReasons` и snapshot | R2, T1 | `Models/CodegenConfig.cs`, `KafkaTableGenerator.cs`, `SchemaDiffer.cs:64-104`, `SchemaSnapshot*` |
| P0-3 | **Одна версия ClickHouse** для compose, Testcontainers и README (например, читать `CLICKHOUSE_IMAGE` из `.env.example` в `TestContainersEnv`) + матрица «min supported / current» | R3, T4 | `TestContainersEnv.cs:12`, `.env.example`, `README.md` |
| P0-4 | **E2E-тест пайплайна в CI**: Testcontainers Kafka + Schema Registry + ClickHouse; публикация через `Confluent.SchemaRegistry.Serdes.Protobuf` (value+protobuf-ключ); проверки: `orders` = N, `kafka_key.order_id = order_id`, агрегаты; затем миграция с recreate очереди под потоком — без потерь | R4, R5, R7, T4 | новый проект/тест в `tests/ClickHouseSchemaGen.IntegrationTests`, `Directory.Packages.props` (`Testcontainers.Kafka`) |
| P0-5 | **Проверить и закрыть R5**: в `replicatedDatabase` режиме создавать UDF с `ON CLUSTER` (или включить `user_defined_zookeeper_path` в `config/*.xml`) и добавить тест «MV на реплике ch2 обрабатывает batch с ключом» | R5, T2 | `ClusterDdl.ProtobufWireFunctionDefinitions` (`ClusterDdl.cs:108-111`), `docker/clickhouse-cluster/config/` |

### P1 — эксплуатационная зрелость

| # | Рекомендация | Закрывает |
| --- | --- | --- |
| P1-1 | **Лок Migrator'а**: `KeeperMap`-таблица или `INSERT`-lease в `schema_migrations` (kind=`lock`, TTL), проверка перед apply; запись «started/finished» для обнаружения прерванных миграций | R12, SP-13 |
| P1-2 | **Preflight миграции**: проверка `format_schemas` (наличие файла + хэш через `file()`/`system`-запрос или sidecar) на всех нодах (`clusterAllReplicas`) до recreate очереди; dry-run `CREATE TABLE ... AS`-валидация во временной БД | R7, R13 |
| P1-3 | **Вынести окружение из схемы**: named collections для Kafka (`ENGINE = Kafka(kafka_nc, ...)`) — broker list, SASL/TLS вне git и вне snapshot; diff игнорирует значения коллекции | R6, T3 |
| P1-4 | **Мульти-топологическая генерация из одного `migrate`**: один логический snapshot + рендер SQL под каждую топологию (или явная проверка, что все `clickhouse.codegen*.json` имеют одинаковое число миграций); хранить `cluster`/`key` в snapshot и диффить их | R11, SP-10 |
| P1-5 | **Версионирование миграций, устойчивое к веткам**: проверка в CI «новая миграция > последней в main», или допуск out-of-order для независимых миграций (по `parentChecksum` snapshot-цепочки как источнику порядка) | R9 |
| P1-6 | **Контроль мутаций**: `Rewrite`-изменения генерировать как manual либо с `SETTINGS mutations_sync = 2` и предупреждением об объёме; аналогично для `ON CLUSTER` проверять `system.distributed_ddl_queue` на ошибки по всем хостам | R10, R13, SP-15 |
| P1-7 | **CI**: добавить `scripts/pack-verify.sh` (pack + `samples/NuGetCodegenSmoke`), шаг «git diff --exit-code после codegen всех трёх конфигов», кэш NuGet | R16, R11 |
| P1-8 | **Наблюдаемость**: экспорт `system.kafka_consumers` (lag, `exceptions`, `last_poll_time`), счётчики `*_errors`, алерты на «нет commit > N минут» | R2, T1 |

### P2 — стратегические

| # | Рекомендация | Когда |
| --- | --- | --- |
| P2-1 | Оценить **внешний consumer** (Kafka Connect ClickHouse Sink с exactly-once или свой .NET consumer с `insert_deduplication_token`) при требованиях «без дублей» и изоляции ресурсов; генератор переиспользуется для MergeTree/MV-части (Kafka-DDL становится опциональным выходом). Разбор и триггеры — [ADR-0001](./adr/0001-ingestion-path.md) | Если P0-1 не хватает или ingestion мешает запросам |
| P2-2 | ClickPipes — при переезде в ClickHouse Cloud (нативный SR, ключ без UDF) | Cloud |
| P2-3 | Вход генератора из `.proto`/descriptor set (protoc/buf) параллельно с C#-сборками — для не-.NET продьюсеров | Внешние потребители пакета |
| P2-4 | Перенести TLA+-модели в репозиторий и гонять TLC в CI (nightly), добавить модель деплоя (Migrator ∥ Migrator, partial `ON CLUSTER`) | Поддержание инвариантов при развитии |
| P2-5 | Моделировать агрегаты в конфиге вместо `trailingSql` (MV + Summing/AggregatingMergeTree как first-class), чтобы diff покрывал их | R8 |
| P2-6 | Kafka2 — только как canary-эксперимент на стенде, не для prod ([kafka2-production-assessment.md](./kafka2-production-assessment.md)) | — |

---

## 10. Оговорки

- R5 выведен из кода и конфигов (UDF без `ON CLUSTER`, нет `user_defined_zookeeper_path`, тест не прогоняет Kafka через MV на второй реплике); фактическое поведение ClickHouse 26.6 при репликации `CREATE MATERIALIZED VIEW` с неизвестной на реплике функцией не проверялось запуском.
- Оценки вероятности/влияния — экспертные, для демо-нагрузки; для prod их надо перекалибровать по реальным объёмам и SLA.
- Анализ статический (код + конфиги + сгенерированные артефакты на `f00d285`), без нагрузочного прогона.
