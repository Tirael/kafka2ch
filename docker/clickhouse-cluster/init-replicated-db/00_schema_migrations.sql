CREATE DATABASE IF NOT EXISTS kafka2ch ON CLUSTER kafka2ch
ENGINE = Replicated('/clickhouse/databases/{uuid}', '{replica}');

CREATE TABLE IF NOT EXISTS default.schema_migrations ON CLUSTER kafka2ch
(
    version             String,
    name                String,
    checksum            String,
    applied_at          DateTime,
    kind                LowCardinality(String) DEFAULT 'migration'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{shard}-{replica}')
ORDER BY version;

USE kafka2ch;

INSERT INTO default.schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261016123828', 'add_order_lines2_by_line_id', 'b57c84fa75b76ebfb6cf31d45270195494d72681e07d0e1511db01de35554bcb', toDateTime(0), 'migration');
