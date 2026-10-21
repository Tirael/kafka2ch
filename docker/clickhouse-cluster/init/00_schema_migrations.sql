CREATE TABLE IF NOT EXISTS schema_migrations ON CLUSTER kafka2ch
(
    version             String,
    name                String,
    checksum            String,
    applied_at          DateTime,
    kind                LowCardinality(String) DEFAULT 'migration'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{shard}-{replica}')
ORDER BY version;

INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261016123828', 'add_order_lines2_by_line_id', '7fd29806d437e80711b703d99bd90a241e197e8d28ca051c7b7c30c2f05cc920', toDateTime(0), 'migration');
INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261021205602', 'dedup_orders_by_kafka_offset', '830aec92ac3d306fdbae64d3d913f09166c0c500e8df4c63a50df25e7e5ad137', toDateTime(0), 'migration');
