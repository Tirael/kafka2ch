namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// ClickHouse SQL UDFs that read protobuf wire format from a String (ClickHouse has no per-value protobuf decoder).
/// Field lookup returns the last occurrence of a field number, matching proto3 "last one wins" for scalars.
/// </summary>
public static class ProtobufWireSqlFunctions
{
    public const string Has = "protobufWireHas";

    public const string Bytes = "protobufWireBytes";

    public const string Bits = "protobufWireBits";

    public const string ZigZag = "protobufWireZigZag";

    // SQL UDFs are expanded inline, so an argument used twice is copied twice and nested calls grow
    // the query tree exponentially. Larger arguments are bound once via arrayMap(x -> ..., [arg])[1].
    // Order matters: a UDF must be created after the functions it calls.
    public const string Definitions = """
        -- (value, byte length) of the varint at 1-based position p
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

        -- acc: (next position, value position of field_number or 0, its wire type)
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

        -- Payload of a length-delimited field (string, bytes, nested message); '' when absent.
        CREATE OR REPLACE FUNCTION protobufWireBytes AS (m, field_number) ->
            arrayMap(
                msg -> arrayMap(
                    f -> if(
                        f.2 = 0 OR f.3 != 2,
                        '',
                        arrayMap(len -> substring(msg, f.2 + len.2, len.1), [protobufWireVarint(msg, f.2)])[1]),
                    [protobufWireField(msg, field_number)])[1],
                [m])[1];

        -- Raw 64-bit value of a varint, fixed64 or fixed32 field; 0 when absent.
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
        """;
}
