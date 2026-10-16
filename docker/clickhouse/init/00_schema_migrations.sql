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
INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261016123828', 'add_order_lines2_by_line_id', 'eac2e9bfce568e92dd711ca3eb354385e7996c21c396af357fde63c4f2da3759', toDateTime(0), 'migration');
