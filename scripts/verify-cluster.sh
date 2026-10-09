#!/usr/bin/env bash
# Smoke checks for the clustered stack:
#   docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d --build
set -euo pipefail

CLUSTER="${CLICKHOUSE_CLUSTER:-kafka2ch}"
PASSWORD="${CLICKHOUSE_PASSWORD:-sandbox}"

ch() {
  docker exec clickhouse clickhouse-client --password "${PASSWORD}" --query "$1"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

echo "==> Cluster topology"
ch "SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster = '${CLUSTER}' ORDER BY shard_num, replica_num"

echo
echo "==> Rows per node (replicas of a shard must match)"
ch "SELECT getMacro('shard') AS shard, hostName() AS host, count() AS orders
    FROM clusterAllReplicas('${CLUSTER}', currentDatabase(), orders_local)
    GROUP BY shard, host ORDER BY shard, host"
mismatched=$(ch "SELECT count() FROM (
    SELECT s, uniqExact(c) AS distinct_counts FROM (
        SELECT getMacro('shard') AS s, hostName() AS h, count() AS c
        FROM clusterAllReplicas('${CLUSTER}', currentDatabase(), orders_local) GROUP BY s, h)
    GROUP BY s HAVING distinct_counts > 1)")
[ "${mismatched}" = "0" ] || echo "WARN: replica row counts differ (replication lag or failure)"

echo
echo "==> Distributed totals (no duplicates expected)"
ch "SELECT count() AS orders, uniqExact(order_id) AS unique_orders FROM orders"
total=$(ch "SELECT count() FROM orders")
[ "${total}" -gt 0 ] || fail "no rows in orders"

echo
echo "==> Aggregates through Distributed table"
ch "SELECT minute, category, sum(orders_count), sum(total_amount)
    FROM orders_agg_1m GROUP BY minute, category ORDER BY minute DESC, category LIMIT 6"

echo
echo "==> Kafka partition assignment per node (shared consumer group)"
ch "SELECT hostName() AS host, table, arrayStringConcat(assignments.partition_id, ',') AS partitions
    FROM clusterAllReplicas('${CLUSTER}', system.kafka_consumers) ORDER BY host, table"

echo
echo "==> schema_migrations on every node"
ch "SELECT hostName() AS host, countIf(kind = 'init') AS init, countIf(kind = 'migration') AS migrations
    FROM clusterAllReplicas('${CLUSTER}', currentDatabase(), schema_migrations) GROUP BY host ORDER BY host"
distinct_history=$(ch "SELECT uniqExact(c) FROM (SELECT hostName() AS h, count() AS c
    FROM clusterAllReplicas('${CLUSTER}', currentDatabase(), schema_migrations) GROUP BY h)")
[ "${distinct_history}" = "1" ] || fail "schema_migrations differs between nodes"

echo
echo "OK"
