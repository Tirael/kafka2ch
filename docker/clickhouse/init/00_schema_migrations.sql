CREATE TABLE IF NOT EXISTS schema_migrations
(
    version             String,
    name                String,
    checksum            String,
    applied_at          DateTime,
    kind                LowCardinality(String) DEFAULT 'migration'
)
ENGINE = MergeTree
ORDER BY version;

INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261008115350', 'persist_protobuf_kafka_key', 'a24cfcd61e6e1ece2f0539a0c83acb2247de00dfaee1bae2a84154cc7dcb7146', toDateTime(0), 'migration');
INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261016123828', 'add_order_lines2_by_line_id', 'fa494497a9c987c898f3f9a4c4ba69f7be6aec44f0738fdc71c5a4e956d6aeac', toDateTime(0), 'migration');
INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261021205602', 'dedup_orders_by_kafka_offset', '0a55ff40fbbebf1b6e1d05db3129942b20ea77884a9743d3058f40b1e43c3fa1', toDateTime(0), 'migration');
