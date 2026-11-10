#!/usr/bin/env bash
# Smoke checks for the single-node stack (docker compose up).
# Hard gate: count() vs uniqExact(order_id) after FINAL (same predicate as
# verify-cluster.sh; cross-shard N1 checks are cluster-only).
set -euo pipefail

PASSWORD="${CLICKHOUSE_PASSWORD:-sandbox}"

ch() {
  docker exec clickhouse clickhouse-client --password "${PASSWORD}" --query "$1"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

echo "==> Tables"
ch "SHOW TABLES"

echo
echo "==> FINAL gate: count vs uniqExact(order_id)"
# Same business-key predicate as scripts/verify-cluster.sh (N1 A1). On a single
# node there is no cross-shard path; a mismatch still means unmerged or bad data.
# See docs/observability.md#n1-межшардовые-дубли-a1-accept
totals=$(ch "SELECT count(), uniqExact(order_id) FROM orders FINAL FORMAT TSV")
read -r total unique <<< "${totals}"
echo "rows after FINAL: ${total}, unique order_id: ${unique}"
[ "${total}" -gt 0 ] || fail "no rows in orders"
[ "${total}" = "${unique}" ] || fail "orders FINAL has ${total} rows for ${unique} unique order_id (duplicates survived FINAL)"

echo
echo "==> Aggregates"
ch "SELECT minute, category, sum(orders_count), sum(total_amount)
    FROM orders_agg_1m GROUP BY minute, category ORDER BY minute DESC, category LIMIT 6"

echo
echo "==> schema_migrations"
ch "SELECT kind, version, name, applied_at FROM schema_migrations ORDER BY kind, version"

echo
echo "==> kafka consumers"
ch "SELECT table, assignments.partition_id FROM system.kafka_consumers"

echo
echo "OK"
