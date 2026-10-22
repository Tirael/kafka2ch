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

- Trigger: **workflow_dispatch** (inputs: messages, steady rate/seconds, scenario filter) and a weekly schedule
- Runner brings up the same compose stack as local (`scripts/load-test.sh`)
- Artifact: `load-test-report` (`artifacts/load-test/`)

Example:

```bash
gh workflow run load-test.yml -f messages=300000 -f steady_rate=20 -f steady_seconds=60
```

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

### Defaults left in the sandbox config

Committed `clickhouse.codegen.json` still uses `numConsumers: 1` without the optional throughput fields so the demo stays simple and part-light. Apply tuned values when you need catch-up or tighter latency:

```json
"kafka": {
  "numConsumers": 1,
  "flushIntervalMs": 1000
}
```

High-throughput catch-up example:

```json
"kafka": {
  "numConsumers": 2,
  "threadPerConsumer": true,
  "maxBlockSize": 65536,
  "flushIntervalMs": 1000,
  "pollMaxBatchSize": 16384
}
```

Re-run the stand after changing settings or ClickHouse/Kafka images; paste the new `report.md` summary into PRs that claim a throughput win.
