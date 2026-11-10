# ADR-0001 load calibration (TR-2..TR-4)

P0-2 calibration for [ADR-0001](./adr/0001-ingestion-path.md) §5 triggers TR-2..TR-4. **ADR status stays Proposed** — this document records measurements and a path-A go/no-go; it does not Accept the ADR.

## How to reproduce

```bash
# Local (Docker required)
./scripts/load-calibrate-adr.sh

# GitHub Actions (no local Docker)
gh workflow run load-test.yml \
  -f mode=adr-calibrate \
  -f messages=300000 \
  -f steady_rate=20 \
  -f steady_seconds=90 \
  -f spike_messages=20000
```

Artifact directory: `artifacts/load-calibration/` (`report.md`, `summary.env`, CSVs).

| Variable | Default | Meaning |
|---|---|---|
| `CAL_SEED_MESSAGES` | `300000` | Rows seeded into real `orders` before refresh/query samples (~×10 vs short demo windows; same order as load-test backlog) |
| `CAL_STEADY_RATE` | `20` | ×10 of demo 2 msg/s |
| `CAL_STEADY_SECONDS` | `90` | Concurrent ingest while sampling ReadAggregates |
| `CAL_SPIKE_MESSAGES` | `20000` | Burst after catch-up for TR-4 lag recovery |
| `CAL_OUT` | `artifacts/load-calibration` | Report directory |

## Thresholds (from ADR §5)

| Trigger | Red if |
|---|---|
| TR-2 | `orders_agg_mv` refresh p95 > 50% of interval (> 30 s / 30000 ms at `EVERY 1 MINUTE`) |
| TR-3 | Ingestion > 30% of node CPU at peak, **or** ReadAggregates p95 under ingest > baseline + 25% |
| TR-4 | Lag does not return to ≤ 60 s after ×10 spike (lag expressed as backlog seconds at the ×10 rate) |

**Path A go:** TR-2, TR-3, and TR-4 all green → OK to proceed toward P0-6 Accept.  
**Path A no-go:** any of TR-2/3/4 red → do **not** Accept A blindly; track P1-1 and/or P2-1.

## Measured results

_Pending CI run on this branch. This section will be filled with numbers from `artifacts/load-calibration/report.md` after the Actions job completes._

| Trigger | Measured | Verdict |
|---|---|---|
| TR-2 refresh p95 | _TBD_ | _TBD_ |
| TR-3 CPU peak % | _TBD_ | _TBD_ |
| TR-3 ReadAggregates p95 (baseline → load) | _TBD_ | _TBD_ |
| TR-4 lag recovery | _TBD_ | _TBD_ |

**Path A decision:** _TBD_

### Prior related evidence (load-test.sh, not TR-complete)

Weekly `load-test.sh` on `main` ([run 42639170726](https://github.com/Tirael/kafka2ch/actions/runs/42639170726), 2026-11-09) exercises temporary `load_*` tables only — useful for Kafka setting throughput/CPU, **not** for `orders_agg_mv` refresh or ReadAggregates:

- Drain 300k: ~0.8–1.9 avg CPU cores on 4 vCPU (~20–48% during catch-up); throughput ~16–34k msg/s depending on consumers/tpc.
- Steady 20 msg/s × 60 s: ~0.18–0.24 avg cores; ingest latency p95 ~1 s with `flushIntervalMs=1000`.

That stand **cannot** green/red TR-2 or the ReadAggregates leg of TR-3; use `load-calibrate-adr.sh` for those.

## Environment gap

Cloud Agent VMs in this project have **no Docker**. Calibration must run via the GitHub Actions job above (or a machine with Docker Compose). Local dry-run of the script is limited to `bash -n` / workflow wiring checks.
