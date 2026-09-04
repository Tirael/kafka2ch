CREATE TABLE orders_queue
(
    order_id            String,
    category            LowCardinality(String),
    -- вложенный message Money из common/money.proto маппится на колонки с точкой
    `price.currency`    String,
    `price.amount`      Float64,
    quantity            UInt32,
    event_time_unix_ms  Int64,
    -- proto enum маппится по именам значений; fallback при ошибке парсинга — Int32 + toString() в MV
    status              Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'order_event:OrderEvent',
    kafka_schema_registry_skip_bytes = 6,
    kafka_num_consumers = 1;

CREATE TABLE orders
(
    order_id   String,
    category   LowCardinality(String),
    currency   LowCardinality(String),
    amount     Float64,
    quantity   UInt32,
    status     LowCardinality(String),
    event_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, order_id);

CREATE MATERIALIZED VIEW orders_mv TO orders AS
SELECT
    order_id,
    category,
    `price.currency`            AS currency,
    `price.amount`              AS amount,
    quantity,
    toString(status)            AS status,
    fromUnixTimestamp64Milli(event_time_unix_ms) AS event_time
FROM orders_queue;

CREATE TABLE orders_agg_1m
(
    minute        DateTime,
    category      LowCardinality(String),
    orders_count  UInt64,
    total_amount  Float64,
    total_qty     UInt64
)
ENGINE = SummingMergeTree
ORDER BY (minute, category);

CREATE MATERIALIZED VIEW orders_agg_mv TO orders_agg_1m AS
SELECT
    toStartOfMinute(event_time) AS minute,
    category,
    count()                     AS orders_count,
    sum(amount)                 AS total_amount,
    sum(quantity)               AS total_qty
FROM orders
GROUP BY minute, category;
