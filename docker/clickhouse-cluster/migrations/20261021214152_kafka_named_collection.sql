DETACH TABLE IF EXISTS orders_queue ON CLUSTER kafka2ch;
-- await:kafka_consumers_empty orders_queue ON CLUSTER kafka2ch

DROP VIEW IF EXISTS orders_mv ON CLUSTER kafka2ch;
DROP VIEW IF EXISTS orders_queue_errors_mv ON CLUSTER kafka2ch;
ATTACH TABLE IF NOT EXISTS orders_queue ON CLUSTER kafka2ch;
DROP TABLE IF EXISTS orders_queue ON CLUSTER kafka2ch;
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
    attachments          Tuple(invoices Array(Tuple(name String, note Nullable(String))), receipts Array(Tuple(name String, note Nullable(String)))),
    order_lines2_by_line_id Map(Int64, Tuple(line2_id Int64, value2_price2 Float64, value2_quantity2 Int32, value2_created_at2_seconds Int64, value2_created_at2_nanos Int32, value2_changed_at2_seconds Int64, value2_changed_at2_nanos Int32, is_active2 UInt8))
)
ENGINE = Kafka(kafka_orders)
SETTINGS
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'order_event:OrderEvent',
    kafka_schema_registry_skip_bytes = 6,
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    flatten_nested = 0,
    input_format_protobuf_oneof_presence = 1,
    input_format_protobuf_flatten_google_wrappers = 1;

CREATE OR REPLACE FUNCTION protobufWireVarint ON CLUSTER kafka2ch AS (m, p) ->
    arrayFold(
        (acc, b) -> if(
            acc.3 = 1,
            acc,
            (bitOr(acc.1, bitShiftLeft(toUInt64(bitAnd(b, 127)), 7 * acc.2)), acc.2 + 1, toUInt8(b < 128))),
        arrayMap(i -> reinterpretAsUInt8(substring(m, p + i, 1)), range(10)),
        (toUInt64(0), toUInt64(0), toUInt8(0)));

CREATE OR REPLACE FUNCTION protobufWireValueSize ON CLUSTER kafka2ch AS (m, wire_type, value_pos) ->
    toUInt64(multiIf(
        wire_type = 0, protobufWireVarint(m, value_pos).2,
        wire_type = 1, 8,
        wire_type = 2, arrayMap(v -> v.1 + v.2, [protobufWireVarint(m, value_pos)])[1],
        wire_type = 5, 4,
        length(m) + 1));

CREATE OR REPLACE FUNCTION protobufWireStep ON CLUSTER kafka2ch AS (m, field_number, acc, tag) ->
    arrayMap(
        x -> (
            x.3 + protobufWireValueSize(m, x.2, x.3),
            if(x.1 = field_number, x.3, acc.2),
            if(x.1 = field_number, x.2, acc.3)),
        [(bitShiftRight(tag.1, 3), bitAnd(tag.1, 7), acc.1 + tag.2)])[1];

CREATE OR REPLACE FUNCTION protobufWireField ON CLUSTER kafka2ch AS (m, field_number) ->
    arrayMap(
        msg -> arrayFold(
            (acc, i) -> if(
                acc.1 > length(msg),
                acc,
                protobufWireStep(msg, field_number, acc, protobufWireVarint(msg, acc.1))),
            range(length(msg)),
            (toUInt64(1), toUInt64(0), toUInt64(0))),
        [m])[1];

CREATE OR REPLACE FUNCTION protobufWireHas ON CLUSTER kafka2ch AS (m, field_number) ->
    protobufWireField(m, field_number).2 > 0;

CREATE OR REPLACE FUNCTION protobufWireBytes ON CLUSTER kafka2ch AS (m, field_number) ->
    arrayMap(
        msg -> arrayMap(
            f -> if(
                f.2 = 0 OR f.3 != 2,
                '',
                arrayMap(len -> substring(msg, f.2 + len.2, len.1), [protobufWireVarint(msg, f.2)])[1]),
            [protobufWireField(msg, field_number)])[1],
        [m])[1];

CREATE OR REPLACE FUNCTION protobufWireBits ON CLUSTER kafka2ch AS (m, field_number) ->
    arrayMap(
        msg -> arrayMap(
            f -> toUInt64(multiIf(
                f.2 = 0, 0,
                f.3 = 0, protobufWireVarint(msg, f.2).1,
                f.3 = 1, reinterpretAsUInt64(substring(msg, f.2, 8)),
                f.3 = 5, toUInt64(reinterpretAsUInt32(substring(msg, f.2, 4))),
                0)),
            [protobufWireField(msg, field_number)])[1],
        [m])[1];

CREATE OR REPLACE FUNCTION protobufWireZigZag ON CLUSTER kafka2ch AS (bits) ->
    arrayMap(
        v -> if(bitAnd(v, 1) = 0, reinterpretAsInt64(bitShiftRight(v, 1)), -1 - reinterpretAsInt64(bitShiftRight(v, 1))),
        [bits])[1];

CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders_local AS
SELECT
    order_id                     AS order_id,
    category                     AS category,
    `price.currency`             AS currency,
    `price.amount`               AS amount,
    quantity                     AS quantity,
    toString(status)             AS status,
    toString(payment)            AS payment,
    toDateTime64(event_time.seconds + event_time.nanos / 1000000000.0, 3) AS event_time,
    _key                         AS kafka_key,
    CAST(protobufWireBytes(substring(_key, 7), 1) AS String) AS `kafka_key.order_id`,
    mapFromArrays(`_headers.name`, `_headers.value`) AS kafka_headers,
    _partition                   AS kafka_partition,
    _offset                      AS kafka_offset
FROM orders_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW orders_queue_errors_mv ON CLUSTER kafka2ch TO orders_queue_errors_local AS
SELECT
    _topic                       AS _topic,
    _partition                   AS _partition,
    _offset                      AS _offset,
    _raw_message                 AS _raw_message,
    _error                       AS _error,
    now()                        AS received_at
FROM orders_queue
WHERE length(_error) > 0;

