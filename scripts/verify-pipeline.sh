#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose --env-file .env.example"

echo "==> Kafka envelope (expect: 00 + 4-byte schema id + 00)"
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic orders \
  --from-beginning --max-messages 1 --property print.key=false 2>/dev/null | xxd | head -3

echo
echo "==> ClickHouse tables"
docker exec clickhouse clickhouse-client --query "SHOW TABLES"

echo
echo "==> orders count"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM orders"

echo
echo "==> orders sample"
docker exec clickhouse clickhouse-client --query "SELECT * FROM orders LIMIT 3"

echo
echo "==> orders_agg_1m aggregates"
docker exec clickhouse clickhouse-client --query \
  "SELECT minute, category, sum(orders_count), sum(total_amount) FROM orders_agg_1m GROUP BY minute, category ORDER BY minute DESC LIMIT 5"

echo
echo "==> nested payload preserved in orders_full"
docker exec clickhouse clickhouse-client --password sandbox --query \
  "SELECT order_id, length(case_file.journal) AS journal, arraySum(arrayMap(x -> length(x.body), case_file.journal)) AS journal_chars FROM orders_full ORDER BY order_id DESC LIMIT 5"

echo
echo "==> kafka consumers"
docker exec clickhouse clickhouse-client --query "SELECT * FROM system.kafka_consumers FORMAT Vertical"
