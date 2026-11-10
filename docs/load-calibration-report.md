# ADR-0001 load calibration (TR-2..TR-4)

- Date (UTC): 2026-11-10 20:19
- Host: 4 vCPU, 15 GiB RAM, Linux 6.12.113+
- ClickHouse: 26.6.3.62 (compose `.env.example` image)
- Seed for TR-2/TR-3: **350000** rows on real `orders` (50k prior + 300k seed); steady ×10 = **20 msg/s × 90 s**
- TR-4 spike: **20000** messages ASAP after a **50000**-row restored-stack seed (see caveats)
- Pipeline: `orders_queue` → `orders` → refreshable `orders_agg_mv` (`REFRESH EVERY 1 MINUTE`)
- Demo queue: `kafka_num_consumers = 1`, topic partitions = 1 (matches TR-4 consumers=partitions/node)

## Go / no-go (path A)

**GO — path A OK for P0-6.** Remaining P0 items closed; ADR-0001 **Accepted** in P0-6 (calibration PR #90 left status Proposed).

| Trigger | Measured | Threshold | Verdict |
|---|---|---|---|
| TR-2 refresh p95 | **19 ms** (n=5, p50=17, max=23; read_rows=350000) | > 30000 ms → red | **GREEN** |
| TR-3 ingestion CPU peak | **17.4%** of node (0.698 / 4 cores in under-load window) | > 30% → red | **GREEN** |
| TR-3 ReadAggregates p95 | baseline **120.3 ms** → under load **110.6 ms** (n=40/40) | > +25% vs baseline → red | **GREEN** |
| TR-4 lag after spike | recovered_in=**2.17s**; final_lag=0 msgs (~0.00s at 20 msg/s) | must return to ≤ 60 s | **GREEN** |

Combined TR-3: **GREEN**.

## Caveats

1. **No GitHub Actions dispatch** from this agent (`workflow_dispatch` HTTP 403). Run was local after installing Docker on the Cloud Agent VM; inter-container Kafka required `iptables FORWARD ACCEPT`.
2. **TR-2/TR-3** measured on 350k-row `orders` in one continuous run. Forced `SYSTEM REFRESH VIEW` + `SYSTEM WAIT VIEW`; durations 16–23 ms with `read_rows=350000` (full scan confirmed).
3. **TR-4** completed in a follow-up after the first script hung on a buggy pre-spike catch-up (already-caught-up row target). Spike measured on a restored stack seeded to 50k, then 20k burst; lag already 0 at first sample (~2.2 s including publish). Script catch-up bug is fixed in this PR.
4. Stand is **single-node compose**, not a sized prod cluster. Volume ≈ load-test ×10 demo backlog (300k), not multi-TB prod. Re-run on staging-sized data before treating TR-2 as forever-green.
5. Calibration PR filled numbers + go/no-go only; formal Accept is P0-6.

## Raw artifacts

`refresh-samples.csv`, `read-aggregates-*.csv`, `timeseries/`, `summary.env`, `tr4.env`.
