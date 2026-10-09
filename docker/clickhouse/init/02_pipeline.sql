CREATE TABLE orders
(
    order_id             String,
    category             LowCardinality(String),
    currency             LowCardinality(String),
    amount               Float64,
    quantity             UInt32,
    status               LowCardinality(String),
    payment              LowCardinality(String),
    event_time           DateTime64(3),
    kafka_key            String,
    `kafka_key.order_id` String,
    kafka_headers        Map(String, String)
)
ENGINE = MergeTree
ORDER BY (event_time, order_id);

CREATE OR REPLACE FUNCTION protobufWireVarint AS (m, p) ->
    arrayFold(
        (acc, b) -> if(
            acc.3 = 1,
            acc,
            (bitOr(acc.1, bitShiftLeft(toUInt64(bitAnd(b, 127)), 7 * acc.2)), acc.2 + 1, toUInt8(b < 128))),
        arrayMap(i -> reinterpretAsUInt8(substring(m, p + i, 1)), range(10)),
        (toUInt64(0), toUInt64(0), toUInt8(0)));

CREATE OR REPLACE FUNCTION protobufWireValueSize AS (m, wire_type, value_pos) ->
    toUInt64(multiIf(
        wire_type = 0, protobufWireVarint(m, value_pos).2,
        wire_type = 1, 8,
        wire_type = 2, arrayMap(v -> v.1 + v.2, [protobufWireVarint(m, value_pos)])[1],
        wire_type = 5, 4,
        length(m) + 1));

CREATE OR REPLACE FUNCTION protobufWireStep AS (m, field_number, acc, tag) ->
    arrayMap(
        x -> (
            x.3 + protobufWireValueSize(m, x.2, x.3),
            if(x.1 = field_number, x.3, acc.2),
            if(x.1 = field_number, x.2, acc.3)),
        [(bitShiftRight(tag.1, 3), bitAnd(tag.1, 7), acc.1 + tag.2)])[1];

CREATE OR REPLACE FUNCTION protobufWireField AS (m, field_number) ->
    arrayMap(
        msg -> arrayFold(
            (acc, i) -> if(
                acc.1 > length(msg),
                acc,
                protobufWireStep(msg, field_number, acc, protobufWireVarint(msg, acc.1))),
            range(length(msg)),
            (toUInt64(1), toUInt64(0), toUInt64(0))),
        [m])[1];

CREATE OR REPLACE FUNCTION protobufWireHas AS (m, field_number) ->
    protobufWireField(m, field_number).2 > 0;

CREATE OR REPLACE FUNCTION protobufWireBytes AS (m, field_number) ->
    arrayMap(
        msg -> arrayMap(
            f -> if(
                f.2 = 0 OR f.3 != 2,
                '',
                arrayMap(len -> substring(msg, f.2 + len.2, len.1), [protobufWireVarint(msg, f.2)])[1]),
            [protobufWireField(msg, field_number)])[1],
        [m])[1];

CREATE OR REPLACE FUNCTION protobufWireBits AS (m, field_number) ->
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

CREATE OR REPLACE FUNCTION protobufWireZigZag AS (bits) ->
    arrayMap(
        v -> if(bitAnd(v, 1) = 0, reinterpretAsInt64(bitShiftRight(v, 1)), -1 - reinterpretAsInt64(bitShiftRight(v, 1))),
        [bits])[1];

CREATE MATERIALIZED VIEW orders_mv TO orders AS
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
    mapFromArrays(`_headers.name`, `_headers.value`) AS kafka_headers
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

INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES ('02', '02_pipeline.sql', 'a9effc8757c62e65d8be6a6cca50a31ac9100e9aac8f961ba5a1aabbf88f6600', now(), 'init');
