# Load testing (Kafka → ClickHouse)

How to run the single-node load stand, what it measures, and how Kafka throughput settings in `KafkaSettingsConfig` were chosen.

Default demo flow is **2 msg/s**. The stand publishes a large backlog and a steady stream at **×10** (20 msg/s by default) and records drain time, consumer lag (by offsets), ClickHouse CPU, RSS, part churn, and end-to-end latency.

## Prerequisites

- Docker and Docker Compose v2
- `.env` (or rely on `.env.example` via `ENV_FILE`)
- No local Docker in Cloud Agent environments: use the GitHub Actions workflow instead

## Run locally

```bash
cp -n .env.example .env
./scripts/load-test.sh
```

Useful env overrides:

| Variable | Default | Meaning |
|---|---|---|
| `LOAD_MESSAGES` | `300000` | Backlog size for drain scenarios (~42 h of 2 msg/s) |
| `LOAD_STEADY_RATE` | `20` | Steady publish rate (msg/s), ×10 of the demo default |
| `LOAD_STEADY_SECONDS` | `60` | Steady scenario duration |
| `LOAD_PARTITIONS` | `4` | Topic partitions |
| `LOAD_SCENARIOS` | _(empty = all)_ | Comma-separated scenario names, e.g. `d1-baseline,s2-flush-1000` |
| `LOAD_OUT` | `artifacts/load-test` | Report directory |
| `LOAD_KEEP_STACK` | `0` | Keep compose stack after the run |
| `ENV_FILE` | `.env.example` | Compose env file |

Outputs under `LOAD_OUT`:

- `report.md` — markdown summary (also uploaded as the Actions job summary)
- `summary.csv` — one row per scenario
- `timeseries/*.csv` — 1 s samples (rows, backlog, lag, commits, CPU, RSS)
- `udf-microbench.csv` — single-thread key decode cost

## GitHub Actions

Workflow: [`.github/workflows/load-test.yml`](../.github/workflows/load-test.yml).

- Trigger: **workflow_dispatch** (inputs: mode, messages, steady rate/seconds, scenario filter, spike size) and a weekly schedule
- `mode=full` (default, also used by the weekly schedule): Kafka settings stand via `scripts/load-test.sh`
- `mode=adr-calibrate`: ADR-0001 TR-2..TR-4 calibration via `scripts/load-calibrate-adr.sh` on the real `orders` → `orders_agg_mv` path (see [load-calibration-tr2-tr4.md](./load-calibration-tr2-tr4.md))
- Artifacts: `load-test-report` or `load-calibration-report`

Example:

```bash
gh workflow run load-test.yml -f mode=full -f messages=300000 -f steady_rate=20 -f steady_seconds=60
gh workflow run load-test.yml -f mode=adr-calibrate -f messages=300000 -f steady_rate=20 -f steady_seconds=90 -f spike_messages=20000
```

## ADR TR-2..TR-4 calibration

For path-A go/no-go (P0-2), use the calibrate mode above or run locally:

```bash
./scripts/load-calibrate-adr.sh
```

It seeds the real `orders` pipeline, samples `system.view_refreshes` for `orders_agg_mv`, compares `ReadAggregates`-shaped query latency with/without ×10 ingest, records cgroup CPU, and times lag recovery after a publish spike. Results land in `artifacts/load-calibration/` and are summarized in [load-calibration-tr2-tr4.md](./load-calibration-tr2-tr4.md).

## What the stand measures

**Drain scenarios** (`d*`) pre-publish a backlog, then create `load_queue` / `load_mv` / `load_orders` with different Kafka settings and wait until all rows land. Throughput = messages / drain seconds. Net CPU subtracts idle ClickHouse usage.

**Steady scenarios** (`s*`) publish at a constant rate while measuring ingest latency (`event_time` → `ingested_at`).

**UDF key decode** runs after the baseline drain: compares raw `substring(kafka_key, 7)` vs `protobufWireBytes(...)` on the same rows (`system.query_log`).

Scenarios vary `kafka_num_consumers`, `kafka_thread_per_consumer`, `kafka_max_block_size`, `kafka_flush_interval_ms`, and whether the MV keeps the UDF key columns.

## Codegen settings (`KafkaSettingsConfig`)

Optional fields on `kafka` in `clickhouse.codegen.json` (see [clickhouse.codegen.md](../src/Sandbox.Contracts/clickhouse.codegen.md)):

| Config field | ClickHouse setting | When omitted |
|---|---|---|
| `numConsumers` | `kafka_num_consumers` | `1` |
| `maxBlockSize` | `kafka_max_block_size` | CH default (`max_insert_block_size`) |
| `flushIntervalMs` | `kafka_flush_interval_ms` | CH default (`stream_flush_interval_ms`) |
| `pollMaxBatchSize` | `kafka_poll_max_batch_size` | CH default (`max_block_size`) |
| `threadPerConsumer` | `kafka_thread_per_consumer` | `0` (shared poller thread) |

Set values are rendered into queue DDL, stored in `schema.snapshot.json`, and a change recreates the queue (`KafkaTableChanged`). Validation ranges: block/poll 1…10 000 000; flush 100…600 000 ms; `pollMaxBatchSize` ≤ `maxBlockSize`.

`PublishOrders` load knobs (env): `PublishOrders__Count`, `RatePerSecond`, `Parallelism`, `LogEvery`, `StopApplicationWhenDone` (defaults keep the 2 msg/s demo behaviour).

## How settings were chosen (CI evidence)

Reference run: GitHub Actions [Load test #39683750686](https://github.com/Tirael/kafka2ch/actions/runs/39683750686) (2026-10-21, ClickHouse 26.6.3.62, 4 vCPU / 15 GiB, 300 000 backlog, steady 20 msg/s × 60 s).

### UDF key decode

| Variant | ns/row (user time) |
|---|---|
| raw key substring | ~26 |
| `protobufWireBytes` decode | ~3133 |

Decode is ~120× more expensive per row in isolation. In a full drain with MV projection, removing UDF columns only improved throughput by ~7% (d1 16.1k → d2 17.2k msg/s). Keep the UDF path unless profiling shows decode dominating; it is not the primary bottleneck at demo key sizes (~44 B).

### Drain (catch-up) throughput

| Finding | Evidence |
|---|---|
| Extra consumers **without** `threadPerConsumer` hurt | d3 (2 consumers, shared thread): 10.4k msg/s vs d1 baseline 16.1k |
| `threadPerConsumer: true` unlocks parallelism | d4 (2 + tpc): **31.7k** msg/s; d5 (4 + tpc): 28.6k |
| Larger block alone does little on a single consumer | d7 (`maxBlockSize` 65536): ~16.2k ≈ baseline |
| Best catch-up combo | d8: 4 consumers + tpc + `maxBlockSize` 65536 + `flushIntervalMs` 1000 → **34.4k** msg/s |

Recommendation for **backlog recovery / high sustained ingest**: set `numConsumers` to the partition count (or less if CPU-bound), always pair with `threadPerConsumer: true`, and consider `maxBlockSize: 65536` with `flushIntervalMs: 1000`. On a 4-partition topic, **2 consumers + tpc** already nearly doubles drain vs baseline with lower peak CPU than 4.

### Steady flow (latency at ×10 rate)

| Finding | Evidence |
|---|---|
| Default flush leaves multi-second latency | s1: p50 **3770** ms, p95 **7128** ms |
| `flushIntervalMs: 1000` cuts latency ~7× | s2: p50 **536** ms, p95 **996** ms |
| UDF cost is small vs flush timer | s3 (no UDF) ≈ s2 |
| Extra consumers at 20 msg/s add part churn, not latency wins | s4: similar p50, **233** new parts vs 59 |

Recommendation for the **demo / low-rate path (≤ tens of msg/s)**: keep `numConsumers: 1` and set `flushIntervalMs: 1000` so MV blocks flush within ~1 s. Do not raise consumer count for latency at this rate.

`kafka_num_consumers` is **per ClickHouse server**. On the compose cluster (4 partitions, 4 nodes in one group) keep `numConsumers: 1` per node — same steady fragment as demo. Raising consumers helps only when **one** node owns many partitions.

### Defaults left in the sandbox config

Committed `clickhouse.codegen.json` still uses `numConsumers: 1` without the optional throughput fields so the demo stays simple and part-light (TR-4 already GREEN at that setting). Copy-ready overlays (not in `clickhouse.topologies.json`):

- Demo / prod-cluster steady: [`clickhouse.codegen.throughput-demo.example.json`](../src/Sandbox.Contracts/clickhouse.codegen.throughput-demo.example.json), [`clickhouse.codegen.throughput-prod.example.json`](../src/Sandbox.Contracts/clickhouse.codegen.throughput-prod.example.json) — both `{ "numConsumers": 1, "flushIntervalMs": 1000 }`
- Full table + catch-up snippets: [clickhouse.codegen.md § Demo / prod throughput overlay](../src/Sandbox.Contracts/clickhouse.codegen.md#demo--prod-throughput-overlay)

```json
"kafka": {
  "numConsumers": 1,
  "flushIntervalMs": 1000
}
```

Catch-up on a **single node** that owns the partitions (measured separately; always pair extra consumers with `threadPerConsumer: true`):

```json
"kafka": {
  "numConsumers": 2,
  "threadPerConsumer": true,
  "flushIntervalMs": 1000
}
```

```json
"kafka": {
  "numConsumers": 4,
  "threadPerConsumer": true,
  "maxBlockSize": 65536,
  "flushIntervalMs": 1000
}
```

Do not recommend `pollMaxBatchSize` from the stand — it was not varied in `load-test.sh` scenarios. Applying any of these into a live topology recreates the queue (`KafkaTableChanged`).

Re-run the stand after changing settings or ClickHouse/Kafka images; paste the new `report.md` summary into PRs that claim a throughput win.
