# TLA+ models

TLC-checked models of the generator (`tools/ClickHouseSchemaGen`) and the migrator
(`tools/ClickHouseSchemaGen.Migrator`). CI runs them nightly, on demand, and on pull
requests that touch `specs/**` (`.github/workflows/tla.yml`).

## Running

```bash
scripts/check-tla.sh
```

Requires Java 11+ and `curl`. The script downloads the pinned `tla2tools.jar`
(version and sha256 are set at the top of the script), verifies the checksum, runs TLC
on every `specs/*.cfg`, and prints a table of distinct states and results. Logs go
to `artifacts/tlc/`. Set `TLA2TOOLS_JAR` to use an already downloaded jar.

To run one config by hand:

```bash
cd specs
java -cp ../artifacts/tlc/tla2tools-1.7.4.jar tlc2.TLC -config MigratorLock.lease-expiry.cfg MigratorLock.tla
```

## Config conventions

`<Module>[.<variant>].cfg` checks `<Module>.tla`.

- No header: TLC must finish with `No error has been found`.
- First line `\* EXPECT-VIOLATION: <Invariant>`: TLC must report exactly
  `Invariant <Invariant> is violated`. Any other result fails CI. These configs keep
  the counterexamples reproducible:
  - `*.buggy-*`: the code before the fix (PR #57). Each model has an
    `Impl` constant; `"buggy"` keeps the old logic, `"fixed"` is the current code.
  - `*.open-*`: a property that the current code does not satisfy (known open finding).
    When the code is fixed, CI fails on that config. Move the invariant into the main
    config and delete the `open` config.
  - `MigratorLock.unlocked-*`, `MigratorLock.lease-expiry*`, `MigratorLock.locked-partial-reapply`:
    the migrator without the lock, and the known limits of the lock (see below).

Every model is small enough for CI. The whole suite runs in under 30 s.

## Models

### `KafkaMetaResolution.tla` — Kafka meta columns in tables and MVs

Code: `KafkaMetaColumnFactory.Resolve`, `ClickHouseSchemaGenerator`
(`ResolveMetaForMergeTree`, `ResolveMetaForMaterializedView`, auto-MV callback),
`PipelineColumnExpander`, `MaterializedViewAutoGenerator.CreateForAutoColumns`.

There is one Kafka table `q`, one MergeTree table `t` and an optional explicit MV
`q -> t`. Each initial state is one config: default `persistKafkaMeta` flags, the
queue's per-flag override (`true` / `false` / unset), and `t.includeKafkaMeta` /
`view.includeKafkaMeta` (`true` / `false` / unset). It also chooses whether `t` and the
view use auto or explicit columns, and whether `t.sourceTable` is set. The plan is a pure
function of the config.

| Invariant | Meaning |
| --- | --- |
| `AutoViewMatchesAutoTable` | The auto MV writes exactly the auto table's columns, and every `SELECT` source exists in the Kafka table (`_key`, not `kafka_key`). Defect 1.1. |
| `ExplicitViewConsistentWithTargetAndConfig` | Explicit MV: sources exist; `includeKafkaMeta: false` gives no meta mappings; `true` gives some. Defect 1.2. |
| `ExplicitViewTargetsExistingColumns` | Every column an explicit MV writes exists in its target table. **Open.** |

Open finding (`KafkaMetaResolution.open-explicit-target.cfg`): an explicit MV whose
meta resolves to enabled (via `view.includeKafkaMeta: true`, or via queue/default flags
when the MV has auto columns) maps `_key AS kafka_key`. The target is an explicit-columns
table with `includeKafkaMeta` unset or `false`, so it has no `kafka_key`.
`CodegenConfigValidator` has no rule for this. Confirmed with a `BuildPlan` probe on the
current code.

### `MigrationDiffDrift.tla` — `build` drift check vs `migrate`

Code: `SnapshotDriftChecker`, `SchemaSnapshotMapper.ToPlan`, `SchemaDiffer`,
`MigrationPolicy`, `MigrationSqlGenerator` (new Kafka tables).

The pipeline has Kafka table `q1`, an optional `q2`, and MergeTree table `t` with
columns `id` and `c1`. `t` is auto or explicit, has source `q1`, and is fed by an MV
(auto or explicit). `ORDER BY` is `id`, `c1`, or `toStartOfHour(c1)`. The new config is
the old snapshot plus at most two edits: add/remove `q2`, a Kafka setting,
`persistKafkaMeta`, `t.sourceTable`, table origin, MV origin, rename `c1 -> c2`, or
`Int32 -> Int64`.

| Invariant | Meaning |
| --- | --- |
| `DriftImpliesDiff` | If `build` reports drift (and tells the user to run `migrate`), `migrate` maps the snapshot and produces a non-empty diff, so it never says "No schema changes detected". Defect 2.1. |
| `CanMigrateRemovedOrAddedKafkaTable` | Adding or removing a Kafka table is mapped and diffed. Defect 2.2. |
| `AddedKafkaTableIsCreatedInMigration` | A `TableAdded(Kafka)` change emits `CREATE TABLE`. Defect 2.3. |
| `OrderByRenameIsManual` | A rename or type change of a column used in `ORDER BY` (also inside a function) is manual. Defect 2.4. |

The main config includes the `persistMeta` edit. That case was an open finding until
W2-4 ([#73](https://github.com/Tirael/kafka2ch/pull/73)): a queue-only
`persistKafkaMeta` change used to trip `SnapshotDriftChecker` while `migrate` saw no
diff. With the snapshot-v2 fix, `DriftImpliesDiff` holds for `persistMeta` as well.

### `TypeCompatibility.tla` — integer widening

Code: `TypeCompatibility.Classify` / `IsIntegerWidening` (plain and `Nullable(...)`
integer types).

Widths 8/16/32/64 are scaled to 2/3/4/5 bits. Each type's actual value range is
enumerated, so "lossless" means value-set containment and is not a copy of the rule
being checked.

| Invariant | Meaning |
| --- | --- |
| `SafeWideningIsLossless` | `Safe` (automatic `MODIFY COLUMN`) never loses values. Defect 3.2 (`Int64 -> UInt8` was `Safe`). |
| `LosslessIntegerWideningIsSafe` | A lossless plain integer widening (`UInt8 -> Int16`) is `Safe`. |

### `MigratorLock.tla` — two Migrators, partial `ON CLUSTER`, W2-1 lock

Code: `MigrationRunner.ApplyAsync`, `MigrationLock` (KeeperMap path) from W2-1
([#69](https://github.com/Tirael/kafka2ch/pull/69)). If #69 changes before merge,
update the `"locked"` variant.

Several Migrator processes (`Migrators`) apply the same pending migration to `Nodes`.
The DDL lands on each node separately (`ON CLUSTER`), and the statement can fail after
some nodes have executed it (`FailOnCluster`). A process can crash in any phase that
holds the lock. The operator can rerun a finished or crashed process (`MaxRuns`). Time
is discrete (`clock`, `MaxTime`).

- `Variant = "unlocked"`: main before W2-1. `LoadApplied`, execute on each node,
  insert a `migration` row.
- `Variant = "locked"`: W2-1.
  - Lock: insert `(key, owner, expires_at = now + TTL)` with
    `keeper_map_strict_mode = 1`, taken before `LoadApplied`. A holder whose lease has
    expired is removed with `DELETE ... WHERE owner = '<stale>'`, then the insert is
    retried.
  - Journal: a `started` row before the DDL and a `migration` row after it.
  - Release happens in `finally`, and deletes only the process's own row.
  - The owner string contains a per-process GUID, so each run is a distinct owner.
- `LeaseOutlivesMigration = TRUE` stops time from passing the lease end while the
  holder is alive. This models the documented assumption that `--lock-ttl` is longer
  than the longest migration. A crashed holder's lease still expires, so the takeover
  path is checked.

| Invariant | Meaning |
| --- | --- |
| `MutualExclusion` | At most one process is between `started` and `migration` (or executing DDL). |
| `NoDoubleApply` | A node executes the DDL more than once only to recover an interrupted run (partial `ON CLUSTER` failure or crash). |
| `ReapplyOnlyAfterInterruptedWarning` | Re-executing DDL on a node happens only in a run that logged "interrupted run; re-applying it". |
| `HistoryRecordedOnce` | At most one `migration` row. |
| `PartialApplyIsJournaled` | A partially applied migration is always visible in the journal (`started` without `migration`). |
| `RecordedImpliesAppliedEverywhere` | A `migration` row means every node executed the DDL. |
| `EachNodeExecutesOnce` | Stronger: no node executes the DDL twice at all. |

Results:

| Config | Result |
| --- | --- |
| `MigratorLock.cfg` (locked, lease outlives migration) | All invariants except `EachNodeExecutesOnce` hold. |
| `MigratorLock.unlocked-double-apply` | `NoDoubleApply` fails: both processes load an empty history and both execute on `ch1` (the double-apply #69 fixes). |
| `MigratorLock.unlocked-mutual-exclusion` | `MutualExclusion` fails. |
| `MigratorLock.unlocked-partial-on-cluster` | `PartialApplyIsJournaled` fails: after DDL lands on `ch1` only, nothing in the history shows it. |
| `MigratorLock.lease-expiry` | `MutualExclusion` fails: the lease is not renewed, so a holder that outlives `TTL` loses it to a second process while it is still applying. |
| `MigratorLock.lease-expiry-double-apply` | `NoDoubleApply` fails for the same reason, without any failure in between. |
| `MigratorLock.locked-partial-reapply` | `EachNodeExecutesOnce` fails even with the lock: recovery after a partial `ON CLUSTER` failure re-runs the whole file, including statements that already ran on some nodes. Recovery is only safe when the DDL is idempotent (`IF NOT EXISTS` / `IF EXISTS`) or the leftovers are cleaned up by hand. |

The lease-expiry configs document a limit of #69 that its README already states ("TTL
must exceed the longest migration"). Making them pass needs lease renewal (a heartbeat
that extends `expires_at` while the holder is alive) plus a holder check before each
statement.

Not modelled: the single-node best-effort lock (lease rows in `schema_migrations` when
KeeperMap is unavailable), lock-acquire timeouts (a timeout only stops a waiter), and
statement order inside a migration file.

## Distinct states (TLC 1.7.4, local run)

| Config | Distinct states | Result |
| --- | ---: | --- |
| `KafkaMetaResolution` | 13 608 | pass |
| `KafkaMetaResolution.buggy-autoview` | initial state | `AutoViewMatchesAutoTable` violated |
| `KafkaMetaResolution.buggy-explicitview` | initial state | `ExplicitViewConsistentWithTargetAndConfig` violated |
| `KafkaMetaResolution.open-explicit-target` | initial state | `ExplicitViewTargetsExistingColumns` violated |
| `MigrationDiffDrift` | 1 020 | pass |
| `MigrationDiffDrift.buggy-*` (4 configs) | initial state | the named invariant violated |
| `TypeCompatibility` | 256 | pass |
| `TypeCompatibility.buggy-*` (2 configs) | initial state | the named invariant violated |
| `MigratorLock` | 225 811 | pass |
| `MigratorLock.unlocked-double-apply` | 395 | `NoDoubleApply` violated |
| `MigratorLock.unlocked-mutual-exclusion` | 122 | `MutualExclusion` violated |
| `MigratorLock.unlocked-partial-on-cluster` | 56 | `PartialApplyIsJournaled` violated |
| `MigratorLock.lease-expiry` | 2 547 | `MutualExclusion` violated |
| `MigratorLock.lease-expiry-double-apply` | 8 777 | `NoDoubleApply` violated |
| `MigratorLock.locked-partial-reapply` | 3 067 | `EachNodeExecutesOnce` violated |

States explored before a violation depend on the number of TLC workers.

## Scope

These models are abstractions. Column sets, configs and migration steps are reduced to
the attributes each invariant depends on. A passing model shows that the modelled
decision logic is consistent; it does not prove the C# implementation correct.
Port of the earlier one-off TLC study still to do: name collisions in
`CodegenConfigValidator` (defect 4.1), queue restore in `MigrationRunner` after a failed
recreate (defect 5.1), and per-table proto compatibility (defect 3.1). These defects are
fixed and covered by unit and integration tests.
