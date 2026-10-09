CREATE TABLE orders_queue ON CLUSTER kafka2ch
(
    order_id             String,
    category             LowCardinality(String),
    `price.currency`     String,
    `price.amount`       Float64,
    quantity             UInt32,
    `event_time.seconds` Int64,
    `event_time.nanos`   Int32,
    status               Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2),
    tags                 Array(LowCardinality(String)),
    items                Nested(sku String, qty UInt32, unit_price Float64, line_status Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2), parts Array(Tuple(sku String, qty UInt32, weight Nullable(Float64)))),
    metadata             Map(String, String),
    note                 Nullable(String),
    `card.last4`         String,
    `card.network`       String,
    `cash.received`      Float64,
    `wallet.provider`    String,
    `wallet.wallet_id`   String,
    payment              Enum8('absent' = 0, 'card' = 11, 'cash' = 12, 'wallet' = 13),
    promo_code           Nullable(String),
    status_history       Array(Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2)),
    loyalty_points       Nullable(Int32),
    attachments          Tuple(invoices Array(Tuple(name String, note Nullable(String))), receipts Array(Tuple(name String, note Nullable(String))))
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'order_event:OrderEvent',
    kafka_schema_registry_skip_bytes = 6,
    kafka_num_consumers = 1,
    flatten_nested = 0,
    input_format_protobuf_oneof_presence = 1,
    input_format_protobuf_flatten_google_wrappers = 1;

INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('01', '01_orders_queue.sql', '521a62f25f557f6b925574c4b30d2da67705c890c32d7e01432e6d6cec33b26a', now(), 'init');
