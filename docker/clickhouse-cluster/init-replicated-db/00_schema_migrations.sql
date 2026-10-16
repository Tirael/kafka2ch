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

INSERT INTO default.schema_migrations (version, name, checksum, applied_at, kind) VALUES ('20261016123828', 'add_order_lines2_by_line_id', '483e3545ef4c60ec04b14826880b2a7ecd0c64e14cdfcc9b104598b908786d09', toDateTime(0), 'migration');
