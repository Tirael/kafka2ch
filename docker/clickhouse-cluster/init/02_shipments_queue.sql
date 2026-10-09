CREATE TABLE shipments_queue ON CLUSTER kafka2ch
(
    shipment_id          String,
    order_id             String,
    `shipped_at.seconds` Int64,
    `shipped_at.nanos`   Int32,
    status               Enum8('SHIPMENT_STATUS_UNSPECIFIED' = 0, 'SHIPMENT_STATUS_CREATED' = 1, 'SHIPMENT_STATUS_IN_TRANSIT' = 2, 'SHIPMENT_STATUS_DELIVERED' = 3, 'SHIPMENT_STATUS_FAILED' = 4),
    `destination.country` LowCardinality(String),
    `destination.city`   LowCardinality(String),
    `destination.street` String,
    `destination.postal_code` String,
    checkpoints          Nested(recorded_at_seconds Int64, recorded_at_nanos Int32, location String, status Enum8('SHIPMENT_STATUS_UNSPECIFIED' = 0, 'SHIPMENT_STATUS_CREATED' = 1, 'SHIPMENT_STATUS_IN_TRANSIT' = 2, 'SHIPMENT_STATUS_DELIVERED' = 3, 'SHIPMENT_STATUS_FAILED' = 4), scans Array(Tuple(code String, operator_note Nullable(String)))),
    carrier_metadata     Map(String, String),
    instructions         Nullable(String),
    `delivered.delivered_at.seconds` Int64,
    `delivered.delivered_at.nanos` Int32,
    `delivered.signed_by` String,
    `failed.reason`      String,
    `failed.retry_count` Nullable(Int32),
    delivery_outcome     Enum8('absent' = 0, 'delivered' = 9, 'failed' = 10),
    priority             Nullable(Int32),
    status_history       Array(Enum8('SHIPMENT_STATUS_UNSPECIFIED' = 0, 'SHIPMENT_STATUS_CREATED' = 1, 'SHIPMENT_STATUS_IN_TRANSIT' = 2, 'SHIPMENT_STATUS_DELIVERED' = 3, 'SHIPMENT_STATUS_FAILED' = 4)),
    documents            Tuple(labels Array(Tuple(id String, pages Nullable(Int32))), customs_forms Array(Tuple(id String, pages Nullable(Int32))))
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'shipments',
    kafka_group_name = 'clickhouse-shipments',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'shipment_event:ShipmentEvent',
    kafka_schema_registry_skip_bytes = 6,
    kafka_num_consumers = 1,
    flatten_nested = 0,
    input_format_protobuf_oneof_presence = 1,
    input_format_protobuf_flatten_google_wrappers = 1;

INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('02', '02_shipments_queue.sql', 'c209c7c17506062ea219cd17b0d674edfcf074a35a257f5003043263626bc496', now(), 'init');
