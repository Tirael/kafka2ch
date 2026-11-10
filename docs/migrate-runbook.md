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
5. Только после зелёного migrate — **продьюсеры** / нагрузка.

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

1. На staging запустить migrate с instrumentation / `kill -9` Migrator **после** `DETACH`/`DROP` queue и **до** успешного `CREATE`.
2. Проверить: очередь снова `ATTACH`/`CREATE` (auto-restore или ручной шаг выше); `system.kafka_consumers` непустой; lag не растёт бесконечно.
3. Повторный migrate дописывает `schema_migrations` без дубля checksum.
4. Зафиксировать результат drill (дата, топология, auto vs manual) в on-call notes.

## Алерты рядом с migrate

Пока идёт Job: ожидаемы краткие `NoAssignments` / отсутствие commits. После Job — правила из [observability.md](observability.md) (`NoCommits`, `ParseErrors`, `ViewRefresh*`, `OrdersQueueErrorsGrowth`) должны быть в prod Prometheus → Alertmanager → pager.
