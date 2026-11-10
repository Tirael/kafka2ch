# ADR-0001 load calibration (TR-2..TR-4)

P0-2 calibration for [ADR-0001](./adr/0001-ingestion-path.md) §5 triggers TR-2..TR-4. This document records measurements and the path-A go/no-go that unblocked P0-6. ADR status was left **Proposed** in #90; **Accepted** in P0-6 after P0-1..P0-5.

## How to reproduce

```bash
# Local (Docker required)
./scripts/load-calibrate-adr.sh

# GitHub Actions (preferred in CI; agent tokens may lack workflow_dispatch)
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
| `CAL_SEED_MESSAGES` | `300000` | Rows seeded into real `orders` before refresh/query samples |
| `CAL_STEADY_RATE` | `20` | ×10 of demo 2 msg/s |
| `CAL_STEADY_SECONDS` | `90` | Concurrent ingest while sampling ReadAggregates |
| `CAL_SPIKE_MESSAGES` | `20000` | Burst after catch-up for TR-4 lag recovery |
| `CAL_OUT` | `artifacts/load-calibration` | Report directory |

## Thresholds (from ADR §5)

| Trigger | Red if |
|---|---|
| TR-2 | `orders_agg_mv` refresh p95 > 50% of interval (> 30 s / 30000 ms at `EVERY 1 MINUTE`) |
| TR-3 | Ingestion > 30% of node CPU at peak, **or** ReadAggregates p95 under ingest > baseline + 25% |
| TR-4 | Lag does not return to ≤ 60 s after ×10 spike (lag as backlog seconds at the ×10 rate) |

**Path A go:** TR-2, TR-3, and TR-4 all green → OK to proceed toward P0-6 Accept.  
**Path A no-go:** any of TR-2/3/4 red → do **not** Accept A blindly; track P1-1 and/or P2-1.

## Measured results (2026-11-10)

Host: 4 vCPU / 15 GiB, Linux, ClickHouse **26.6.3.62**, single-node compose.  
TR-2/TR-3 seed: **350000** rows on real `orders` (`read_rows=350000` on refresh). Steady: **20 msg/s × 90 s**.  
TR-4: **20000**-message spike on a restored stack after **50000**-row seed (see caveats).

| Trigger | Measured | Verdict |
|---|---|---|
| TR-2 refresh p95 | **19 ms** (p50 17, max 23, n=5) | **GREEN** |
| TR-3 CPU peak % | **17.4%** (0.698 / 4 cores) | **GREEN** |
| TR-3 ReadAggregates p95 | baseline **120.3 ms** → under load **110.6 ms** (n=40/40) | **GREEN** |
| TR-4 lag recovery | **2.17 s** to ≤60 s; final lag **0** msgs | **GREEN** |

**Path A decision: GO** — all TR-2..TR-4 green. Remaining P0 items closed; ADR-0001 **Accepted** in P0-6.

### Caveats

1. Agent could not `workflow_dispatch` Load test (HTTP 403); run was local Docker on the Cloud Agent VM (needed `iptables FORWARD ACCEPT` for Kafka inter-container).
2. First full-script attempt hung on a pre-spike catch-up bug (already caught up but waited for `rows + load_count`). Bug fixed in `scripts/load-calibrate-adr.sh`; TR-4 finished in a follow-up spike run.
3. Stand is not a sized prod cluster; 350k ≈ load-test ×10 demo backlog, not multi-TB. Re-run on staging volume before treating TR-2 as capacity-proof forever.
4. Related `load-test.sh` weekly evidence ([run 42639170726](https://github.com/Tirael/kafka2ch/actions/runs/42639170726)) covers Kafka setting throughput on `load_*` tables only — not `orders_agg_mv` / ReadAggregates.

## Environment notes

Cloud Agent images historically ship without Docker; this calibration installed `docker.io` + Compose v2 in-session. Prefer GitHub Actions `mode=adr-calibrate` for repeatable CI evidence when the token can dispatch workflows.
