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
