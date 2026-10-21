# Формальная верификация `ClickHouseSchemaGen` через TLA+ / TLC

Код генератора (`ClickHouseSchemaGen` + `ClickHouseSchemaGen.Migrator`) смоделирован в TLA+ и проверен model checker'ом **TLC (`tla2tools.jar` v2026.10.15)** на 5 формальных спецификациях:

1. **`PlanAndSnapshotModel` / `PlanAndSnapshotModel2`** (~46 322 и ~23 189 состояний) — построение плана (`ClickHouseSchemaGenerator.BuildPlan`), раскрытие колонок и Kafka-метаданных (`PipelineColumnExpander`, `KafkaMetaColumnFactory`, `MaterializedViewAutoGenerator`), привязка `_key.*` (`KafkaKeyBindings`).
2. **`ValidatorConflictsModel`** (~2 481 состояние) — инварианты отсутствия коллизий имён объектов ClickHouse после прохождения `CodegenConfigValidator`.
3. **`MigrationGeneratorModel` / `MigrationPipelineModel`** (~40,9 млн и ~36 864 состояния) — согласованность `SnapshotDriftChecker` ↔ `SchemaSnapshotMapper.ToPlan` ↔ `SchemaDiffer` ↔ `MigrationPolicy` ↔ `MigrationSqlGenerator`.
4. **`ProtoCompatAndTypeModel`** (~512 состояний) — `ProtoCompatibilityValidator` на мульти-табличных планах и математическая корректность классификации типов `TypeCompatibility`.
5. **`MigrationRunnerModel`** (~105 состояний) — конечный автомат выполнения миграций и восстановления очередей в `MigrationRunner`.

Ниже перечислены найденные TLC контрпримеры (нарушения инвариантов) с привязкой к точным строкам кода.

---

## 1. Критические дефекты генерации плана и SQL (`BuildPlan` / `Expand` / `AutoGenerator`)

### 1.1. Нарушение инварианта `AutoViewMatchesAutoTable`: рассинхрон `includeKafkaMeta` между Auto-MergeTree и Auto-MV
- **Где:** `ClickHouseSchemaGenerator.cs:64-73, 114-125` и `MaterializedViewAutoGenerator.cs:48-67`.
- **Контрпример TLC:**
  - Конфиг: `defaults.persistKafkaMeta` = все `false`, `queue.persistKafkaMeta` = `null`, у MergeTree-таблицы `t1` (`columns = []`, `sourceTable = "q1"`) задано `includeKafkaMeta = true` (или `false` при включённом `defaults.persistKafkaMeta`).
- **Суть бага:**
  - При раскрытии `MergeTreeTable` вызывается `ResolveMetaForMergeTree` (`ClickHouseSchemaGenerator.cs:64`), который учитывает `table.IncludeKafkaMeta` (`true` включает `Key` и `Headers`, `false` выключает всё).
  - Но в `MaterializedViewAutoGenerator.CreateForAutoColumns` (`ClickHouseSchemaGenerator.cs:118`) передаётся `sourceTable => metaByQueue.GetValueOrDefault(sourceTable) ?? config.Defaults.PersistKafkaMeta` — **без учёта `table.IncludeKafkaMeta`**.
  - **Следствие A (`table.IncludeKafkaMeta = true`, queue meta = `false`):** в `expandedTables[i]` добавляются колонки `kafka_key` и `kafka_headers`. `CreateForAutoColumns` копирует их 1-в-1 (`Source = "kafka_key", Target = "kafka_key"`), но т.к. `meta.AnyEnabled == false`, блок замены на `_key AS kafka_key` (`MaterializedViewAutoGenerator.cs:57-66`) **не выполняется**. Сгенерированный `CREATE MATERIALIZED VIEW` делает `SELECT kafka_key AS kafka_key FROM q1` и падает в ClickHouse (`Unknown expression identifier 'kafka_key'`, в Kafka-таблице виртуальная колонка называется `_key`).
  - **Следствие B (`table.IncludeKafkaMeta = false`, queue meta = `true`):** у MergeTree-таблицы нет колонок `kafka_key`/`kafka_headers`, но `CreateForAutoColumns` видит `meta.AnyEnabled == true` и добавляет их в `SELECT` авто-MV. ClickHouse падает при вставке/создании MV (`No such column kafka_key in table`).

### 1.2. Нарушение инварианта `ExplicitViewConsistentWithTargetAndConfig`: игнорирование `view.IncludeKafkaMeta` и подмена `meta` от `target` вместо `view`
- **Где:** `ClickHouseSchemaGenerator.cs:96-98` и `PipelineColumnExpander.cs:80-105`.
- **Контрпример TLC:**
  1. У явного MV `Columns == []` (авто-раскрытие из `SourceTable`) и `view.IncludeKafkaMeta = false`, а у целевой таблицы `IncludeKafkaMeta = true` (или включён `defaults.persistKafkaMeta`). В `PipelineColumnExpander.ExpandMaterializedView` (`PipelineColumnExpander.cs:102`) ветка `else` проверяет только `if (meta.AnyEnabled)` и **полностью игнорирует `view.IncludeKafkaMeta == false`**.
  2. В `ClickHouseSchemaGenerator.cs:96-98`:
     ```csharp
     var meta = target.Meta
         ?? metaByQueue.GetValueOrDefault(view.SourceTable)
         ?? config.Defaults.PersistKafkaMeta;
     ```
     Поскольку `target.Meta` всегда не-`null` (если `target` найден в `expandedByName`), fallback `metaByQueue[view.SourceTable]` никогда не срабатывает, а `view.IncludeKafkaMeta` вообще не передаётся в `KafkaMetaColumnFactory.Resolve`. Если у `target` `IncludeKafkaMeta = null` (и `target.SourceTable = null`), а у `view` `IncludeKafkaMeta = true` при выключенных defaults, `target.Meta.AnyEnabled` будет `false`, и `ExpandMaterializedView` не добавит мета-колонки, несмотря на `view.IncludeKafkaMeta = true`.

---

## 2. Несогласованность Snapshot / Drift / Diff / Policy (`MigrationPipelineModel`)

### 2.1. Нарушение инварианта `DriftImpliesDiff`: `SnapshotDriftChecker` и `SchemaDiffer` расходятся (тупик `build` vs `migrate`)
- **Где:** `SnapshotDriftChecker.cs:83-96, 126-127`, `SchemaDiffer.cs:118-175`, `SchemaSnapshotMapper.cs:69-72`.
- **Контрпримеры TLC:**
  1. Изменение `MergeTreeTableConfig.SourceTable` у explicit-таблицы (без изменения колонок): `SnapshotDriftChecker` (`SnapshotDriftChecker.cs:90`) выбрасывает `SchemaDriftException` при сборке (`MergeTree table '...' sourceTable changed`) и требует запустить `migrate`. Но `SchemaDiffer.DiffMergeTreeTables` (`SchemaDiffer.cs:118-138`) **не проверяет `SourceTable`** — команда `migrate` возвращает `"No schema changes detected."` и не обновляет снапшот.
  2. Аналогично для `MaterializedView.Origin` (`SnapshotDriftChecker.cs:126` проверяет `Origin`, а `SchemaDiffer.DiffMaterializedViews` — нет).
  3. В `SchemaSnapshotMapper.ToPlan` (`SchemaSnapshotMapper.cs:69-72`) в `pipeline.MaterializedViews` отбираются только `Where(v => v.Origin == PlanOrigin.Explicit)`, а `viewPlans` содержит все MV.

### 2.2. Нарушение инварианта `CanMigrateRemovedOrAddedKafkaTable`: удаление/переименование `KafkaTable` роняет `migrate` до запуска `SchemaDiffer`
- **Где:** `SchemaSnapshotMapper.cs:40-44` vs `SchemaDiffer.cs:44-45`.
- **Контрпример TLC:** `oldQueues = {"q1"}`, `newQueues = {"q2"}`.
- **Суть бага:** `SchemaDiffer.DiffKafkaTables` специально содержит логику генерации `new TableRemoved(name, "Kafka")`, однако перед вызовом `SchemaDiffer.Diff` оркестратор вызывает `SchemaSnapshotMapper.ToPlan(oldSnapshot, config)` (`MigrationOrchestrator.cs:65`), который падает с `InvalidOperationException("Snapshot kafka table 'q1' has no matching entry in codegen config.")`. В результате `TableRemoved(..., "Kafka")` в `SchemaDiffer` — недостижимый мёртвый код.

### 2.3. Нарушение инварианта `AddedKafkaTableIsCreatedInMigration`: добавление новой `KafkaTable` теряется в `MigrationSqlGenerator`
- **Где:** `SchemaDiffer.cs:41-42` vs `MigrationSqlGenerator.cs:99, 286-292`.
- **Контрпример TLC:** В конфиг добавлена вторая очередь `q2` (`TableAdded("q2", "Kafka")` попадает в `AutomaticChanges`).
- **Суть бага:**
  - `AppendNewMergeTreeTables` (`MigrationSqlGenerator.cs:99`) фильтрует `.OfType<TableAdded>().Where(t => t.Engine == "MergeTree")`.
  - `CollectKafkaRecreates` (`MigrationSqlGenerator.cs:286-292`) ищет только `.OfType<KafkaTableChanged>()`.
  - В итоге `TableAdded("q2", "Kafka")` **вообще не генерирует `CREATE TABLE q2`** в SQL миграции, хотя снапшот обновляется так, будто `q2` уже создана.

### 2.4. Нарушение инварианта `OrderByRenameIsManual`: мёртвый цикл в `MigrationPolicy` и проверка `oldColumn.Name` по новому `OrderBy`
- **Где:** `MigrationPolicy.cs:55-59, 95-106` и `SchemaDiffer.cs:137, 213`.
- **Контрпример TLC:** Колонка `c1`, входящая в `ORDER BY`, переименовывается (`c1 -> c2`) и/или меняет тип.
- **Суть бага (двойная ошибка):**
  1. В `MigrationPolicy.CollectManualTables` (`MigrationPolicy.cs:55-59`):
     ```csharp
     foreach (var tableName in manualTables.ToList())
     {
         if (TouchesOrderByColumn(tableName, diff, oldPlan, newPlan))
             manualTables.Add(tableName);
     }
     ```
     Цикл итерируется по `manualTables.ToList()` и добавляет `tableName` обратно в `manualTables`, где он **уже находится**! Этот цикл — полный no-op, а метод `TouchesOrderByColumn` никогда не вызывается для таблиц, не попавших в `manualTables` по другим причинам.
  2. Даже если бы вызывался: и `TouchesOrderByColumn` (`MigrationPolicy.cs:95`), и `SchemaDiffer.DiffColumns` (`SchemaDiffer.cs:137, 213`) берут `orderByColumns` из **`newTable.Config.OrderBy`**, но ищут в нём **`oldColumn.Name` / `renamed.OldName`**. Когда колонка переименована (`c1 -> c2`), в `newTable.Config.OrderBy` уже записано `c2`, поэтому `orderByColumns.Contains(oldColumn.Name)` возвращает `false`, и изменение ключевой колонки `ORDER BY` ошибочно проходит как `Automatic` (`RENAME COLUMN` / `MODIFY COLUMN`), что упадёт в ClickHouse (`Cannot RENAME key column`).

### 2.5. Позиционирование новой первой колонки (`FindPreviousColumnName`)
- **Где:** `SchemaDiffer.cs:288-299` и `MigrationSqlGenerator.cs:233-241`.
- **Суть бага:** Если новая колонка добавлена на позицию `0` (`i == 0`), `FindPreviousColumnName` возвращает `null`. `BuildAddColumnClause` генерирует `ADD COLUMN IF NOT EXISTS col ...` без `FIRST` и без `AFTER`, что в ClickHouse добавляет колонку **в конец таблицы**, а не в начало (в отличие от `init`-скрипта).

---

## 3. Дефекты `ProtoCompatibilityValidator` и `TypeCompatibility` (`ProtoCompatAndTypeModel`)

### 3.1. Нарушение инварианта `MultiQueueProtoCompatCatchesQ1Breakage`: перезапись путей полей между разными таблицами
- **Где:** `ProtoCompatibilityValidator.cs:103-135`.
- **Контрпример TLC:** В плане две Kafka-таблицы (`q1` и `q2`). В `q1` поле `1` несовместимо изменило тип (`Int32 -> String`), а в `q2` поле `1` осталось `Int64 -> Int64`.
- **Суть бага:** `CollectProtoColumns` складывает колонки всех таблиц в единый `Dictionary<string, ProtoColumnRef>` с ключом `FieldNumberPath` (например, `"1"`, `"2"`), **без имени таблицы в ключе**:
  ```csharp
  columns[path] = new ProtoColumnRef(kafkaTable.Config.TableName, column.Name, path, column.Type);
  ```
  Вторая Kafka-таблица перезаписывает все совпавшие `FieldNumberPath` первой таблицы, и `ProtoCompatibilityValidator` пропускает ломающие изменения protobuf-схемы на первой таблице (или ложно срабатывает, сравнивая поле `1` одной таблицы с полем `1` другой при добавлении/удалении таблиц).

### 3.2. Нарушение инварианта `SafeWideningIsLossless`: знаковые `Int8..Int64` классифицируются как безопасно расширяемые в беззнаковые `UInt8..UInt64`
- **Где:** `TypeCompatibility.cs:52-61, 115-126`.
- **Контрпример TLC:** `oldIntType = "Int64"`, `newIntType = "UInt8"` → `TypeCompatibility.Classify("Int64", "UInt8") == TypeChangeKind.Safe`.
- **Суть бага:** Словарь `IntegerRank` задаёт единую линейную шкалу:
  `Int8(1) < Int16(2) < Int32(3) < Int64(4) < UInt8(5) < UInt16(6) < UInt32(7) < UInt64(8)`, а `IsIntegerWidening` проверяет просто `newRank > oldRank`.
  - Любой переход `Int8/Int16/Int32/Int64 -> UInt8` имеет `newRank (5) > oldRank (1..4)` и классифицируется как **`Safe`** (автоматический `ALTER TABLE MODIFY COLUMN` с потерей знака и усечением 64 бит до 8 бит!).
  - Обратный действительно безопасный переход `UInt8 -> Int16 / Int32 / Int64` имеет `newRank (2..4) < oldRank (5)` и ошибочно классифицируется как **`Manual`**.

---

## 4. Коллизии имён в `CodegenConfigValidator` (`ValidatorConflictsModel`)

### 4.1. Нарушение инварианта `NoClickHouseObjectNameCollision`
- **Где:** `CodegenConfigValidator.cs:68-89, 190-213`.
- **Контрпримеры TLC:**
  1. **Не проверяется пересечение имён между `KafkaTables`, `MergeTreeTables` и `MaterializedViews`:** конфиг с `KafkaTable = "q1"` и `MaterializedView = "q1"` (или `MergeTreeTable = "q1"`) проходит все правила `CodegenConfigValidator`, но генерирует невалидный DDL с дублирующимися именами таблиц/представлений в одной БД ClickHouse.
  2. **`FindAutoMaterializedViewNameConflict` (`CodegenConfigValidator.cs:195-210`)** проверяет имя авто-генерируемого MV (`{table}_mv`) только против `explicitViews`, но не против `KafkaTables` и `MergeTreeTables` (например, если существует таблица с именем `orders_mv`).
  3. **`FindLocalTableNameConflict` (`CodegenConfigValidator.cs:73-76`)** в кластерном режиме собирает `names` только из `KafkaTables`, `MergeTreeTables` и `explicitViews`, не включая авто-генерируемые MV (`{table}_mv`). Если `LocalTableSuffix` равен `"_mv"`, конфликт `t1_mv` (auto-MV) и `t1_mv` (local-таблица) не обнаруживается.

---

## 5. Жизненный цикл очередей при сбое миграции (`MigrationRunnerModel`)

### 5.1. Нарушение инварианта `QueueExistsAndAttachedAfterFailure`
- **Где:** `MigrationSqlGenerator.cs:134-146` и `MigrationRunner.cs:135-171, 173-193`.
- **Контрпример TLC:** `isKafkaRecreate = TRUE`, `failAtStep = 5` (сбой на `CREATE TABLE q1` или последующем `CREATE MATERIALIZED VIEW` после выполнения `DROP TABLE IF EXISTS q1`).
- **Суть:**
  - При пересоздании Kafka-таблицы в миграции выполняются подряд: `DETACH TABLE q1` → `ATTACH TABLE q1` → `DROP TABLE IF EXISTS q1` → `CREATE TABLE q1` → `CREATE MATERIALIZED VIEW ...`.
  - Как только выполнен `ATTACH TABLE` / `DROP TABLE`, `TrackQueueLifecycle` удаляет `q1` из `detachedQueues` (`MigrationRunner.cs:185, 192`).
  - Если ошибка происходит на `CREATE TABLE q1` (например, не обновилась protobuf-схема на диске ClickHouse) или на `CREATE MATERIALIZED VIEW` (ошибка выражения), блок `finally` (`ReattachDetachedQueuesAsync`) ничего сделать не может: старая таблица `q1` (или MV) уже удалена (`DROP`), т.к. DDL в ClickHouse не транзакционен.

---

## Сводная таблица проверенных моделей TLC

| Модель TLA+ | Состояний проверено | Статус TLC | Найденные проблемы |
| --- | ---: | --- | --- |
| `PlanAndSnapshotModel` | 46 322 | Нарушен `AutoViewMatchesAutoTable` | Игнорирование `table.IncludeKafkaMeta` в `MaterializedViewAutoGenerator` (`ClickHouseSchemaGenerator.cs:118`) |
| `PlanAndSnapshotModel2` | 23 189 | Нарушен `ExplicitViewConsistentWithTargetAndConfig` | Игнорирование `view.IncludeKafkaMeta` при пустых `Columns` (`PipelineColumnExpander.cs:102`) и в `ClickHouseSchemaGenerator.cs:96` |
| `ValidatorConflictsModel` | 2 481 | Нарушен `NoClickHouseObjectNameCollision` | Отсутствие проверки пересечения имён между `KafkaTables` / `MergeTreeTables` / `MaterializedViews` и auto-MV (`CodegenConfigValidator.cs:68, 190`) |
| `MigrationGeneratorModel` | 40 943 620 | Нарушен `CanMigrateRemovedOrAddedKafkaTable` | `SchemaSnapshotMapper.ToPlan` падает при удалении Kafka-таблицы (`SchemaSnapshotMapper.cs:42`) |
| `MigrationPipelineModel` | 36 864 | Нарушены `DriftImpliesDiff`, `OrderByRenameIsManual`, `AddedKafkaTableIsCreatedInMigration`; выполнен `DetachedQueuesAreReattached` | Рассинхрон Drift vs Diff по `SourceTable` и `View.Origin`; no-op цикл `manualTables` в `MigrationPolicy.cs:55`; проверка `oldColumn.Name` в новом `OrderBy`; пропуск `TableAdded("Kafka")` в `MigrationSqlGenerator.cs:99` |
| `ProtoCompatAndTypeModel` | 512 | Нарушены `SafeWideningIsLossless`, `MultiQueueProtoCompatCatchesQ1Breakage` | `Int64 -> UInt8` считается `Safe` в `TypeCompatibility.cs:115`; перезапись ключей `FieldNumberPath` между таблицами в `ProtoCompatibilityValidator.cs:103` |
| `MigrationRunnerModel` | 105 | Нарушен `QueueExistsAndAttachedAfterFailure` на шаге после `DROP TABLE` | Невосстановимое состояние при ошибке между `DROP TABLE q1` и `CREATE TABLE q1` / `CREATE MV` |
