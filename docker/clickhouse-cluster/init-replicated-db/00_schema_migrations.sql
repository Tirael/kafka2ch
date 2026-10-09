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


