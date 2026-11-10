# Runbook: миграции схемы ClickHouse

Краткий on-call / deploy playbook для `ClickHouseSchemaGen.Migrator`. Подробности codegen — [clickhouse.codegen.md](../src/Sandbox.Contracts/clickhouse.codegen.md#миграции-схемы), обзор — [README § Миграции](../README.md#миграции-схемы).

## Порядок (prod / staging)

1. **Разложить `.proto`** на **все** ноды ClickHouse в `format_schema_path` (compose: `/var/lib/clickhouse/user_files/format_schemas`). Хэш должен совпасть с маркером `-- require:format_schema … sha256=…` в миграции.
2. **Сгенерировать** миграцию (если ещё нет): `Cli migrate --all-topologies …` → review/commit SQL + snapshot.
3. **Один** Job / one-shot `clickhouse-migrate` / `Migrator` **без** `--skip-preflight`:
   - preflight: `format_schema` на всех репликах + dry-run queue/MV;
   - лок (`KeeperMap` или best-effort lease) → `kind = 'started'` → statements → `kind = 'migration'` в `schema_migrations`.
4. **Проверить:**
   ```sql
   SELECT version, checksum, applied_at FROM schema_migrations
   WHERE kind = 'migration' ORDER BY version DESC LIMIT 5;
   SELECT * FROM system.kafka_consumers WHERE table = 'orders_queue';
   ```
5. **Data-quality gate (обязателен, P1-2 / N1 A1).** После того как в `orders` есть строки — прогнать gate; exit ≠ 0 → **не** продвигать релиз, page on-call (тот же канал, что Prometheus critical):
   ```bash
   # кластер (staging/prod или уже поднятый compose)
   VERIFY_ASSUME_STACK=1 ./scripts/run-verify-gate.sh
   # single-node
   VERIFY_ASSUME_STACK=1 VERIFY_TOPOLOGY=single ./scripts/run-verify-gate.sh
   ```
   Проверка: `scripts/verify-cluster.sh` / `scripts/verify-single.sh` (`count() = uniqExact(order_id) FROM orders FINAL`). Nightly в репозитории — workflow **Verify data gate** (`.github/workflows/verify-data-gate.yml`); на живом staging — тот же cron/Job против кластера (не «по желанию»). См. [observability.md § N1](observability.md#n1-межшардовые-дубли-a1-accept).
6. Только после зелёного migrate **и** зелёного data gate — **продьюсеры** / нагрузка (если продьюсеры уже работают — gate всё равно обязателен перед promote).

## Запреты

| Нельзя | Почему |
| --- | --- |
| Два параллельных Migrator Job'а | Даже с локом — не деплойте два; один Job на окружение |
| `--skip-preflight` в prod | Обход проверки `.proto` → recreate очереди без схемы |
| Править DDL руками вне миграций | Живая база = init релиза + applied migrations |
| Стартовать producers до конца migrate | Consumer может читать со старой/битой schema |

## Rollback / сбой mid-flight

Migrator перед `DETACH`/`DROP` снимает `SHOW CREATE`. При ошибке:

1. Читает отчёт `restored / not restored` в логе / `MigrationFailedException`.
2. Пытается `ATTACH` отсоединённых очередей и восстановить DROP'нутые объекты из snapshot DDL.
3. Строка `kind = 'started'` без `migration` → при следующем запуске warning «interrupted»; SQL должен быть **идемпотентным**, Job можно перезапустить (один).

Если auto-restore не поднял очередь:

```sql
-- убедиться, что таблица detached, затем:
ATTACH TABLE orders_queue;           -- single-node / onCluster local
-- или SHOW CREATE из бэкапа / лога Migrator и CREATE заново
SELECT * FROM system.kafka_consumers WHERE table = 'orders_queue';
```

Полный откат данных MergeTree — вне Migrator (бэкап / `EXCHANGE` / ручной SQL); Migrator не делает `Down()`.

## Staging drill (обязателен до prod)

Цель: убедиться, что partial failure между `DROP` и `CREATE` очереди восстанавливается или имеет ручной playbook.

1. На staging запустить migrate с instrumentation / `kill -9` Migrator **после** `DROP TABLE` queue и **до** успешного `CREATE`.
2. Проверить состояние (см. матрицу ниже): очередь снова `ATTACH`/`CREATE`; при необходимости — один повторный Job.
3. Повторный migrate дописывает `schema_migrations` без дубля checksum.
4. Зафиксировать результат drill (дата, топология, auto vs manual) в on-call notes.

### Результат разбора P1-3 (код + тесты; live `kill -9` — подтверждение на staging)

Дата: 2026-11-10. Топология: single-node recreate SQL (`ATTACH` → `DROP TABLE` → `CREATE TABLE` без `IF NOT EXISTS` → `CREATE MV`), `origin/main` @ `f481363` + этот PR. Live `kill -9` в этой среде **не** гонялся (нет Docker/staging CH); интеграционные тесты exception-path остаются источником для restore при пойманном исключении.

| Момент смерти процесса | Объекты | Следующий Job |
| --- | --- | --- |
| Пока очередь detached (в т.ч. длинный `INSERT` в dedup-миграции) | Detached queue; MV могут быть сняты | Heals: `DETACH IF EXISTS` / `ATTACH` / дальше по файлу |
| После `ATTACH`/`CREATE`, до `CREATE MV` | Attached queue **без** `orders_mv` | Heals переигровкой файла; страница `ClickHouseKafkaQueueWithoutMv` / ConsumerHealth, если окно > 5m |
| **Между `DROP TABLE` и `CREATE TABLE` (drill)** | neither attached nor detached (`system.tables` / `system.detached_tables` пусты) | Migrator **пропускает** `ATTACH`, если очередь нигде не числится → `DROP IF EXISTS` / `CREATE` / `CREATE MV`. Без пропуска `ATTACH` мог оборвать replay |
| Exception после `CREATE` с **другой** схемой колонок | Новая queue; старый MV **намеренно** не restore | Как в `GivenQueueRecreatedWithOtherSchemaBeforeFailure`; руками старый MV на новую схему не вешать |

Пойманное исключение в том же процессе: `RestoreDroppedObjectsAsync` / `ReattachDetachedQueuesAsync` (см. `MigrationQueueLifecycleIntegrationTests`). Снимок `SHOW CREATE` живёт только в памяти процесса — после `kill -9` его нет; восстановление = **один** повторный Job.

Перед повтором: дождаться `--lock-ttl` (дефолт 1800 с) или удалить lease мёртвого владельца (`schema_migrations_lock` / `kind='lock'`) **только** когда процесс точно мёртв. Не красть live lease.

Журнал (не алерт по возрасту — длинный dedup держит `started` весь `INSERT`):

```sql
SELECT version, name, min(applied_at) AS started_at
FROM schema_migrations
WHERE kind = 'started'
  AND version NOT IN (SELECT version FROM schema_migrations WHERE kind = 'migration')
GROUP BY version, name;
```

Проверка «очередь без MV» / «очереди нет»:

```sql
SELECT name, engine, dependencies_table
FROM system.tables
WHERE database = currentDatabase() AND name = 'orders_queue';

SELECT table FROM system.detached_tables
WHERE database = currentDatabase() AND table = 'orders_queue';
```

## Алерты рядом с migrate

Пока идёт Job: ожидаемы краткие `NoAssignments` / отсутствие commits. После Job — правила из [observability.md](observability.md) (`NoCommits`, `ParseErrors`, `ViewRefresh*`, `OrdersQueueErrorsGrowth`, **`ClickHouseKafkaQueueWithoutMv`**, **`ClickHouseKafkaQueueMissing`**) должны быть в prod Prometheus → Alertmanager → pager.
