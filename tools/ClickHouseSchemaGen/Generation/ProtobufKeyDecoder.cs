namespace ClickHouseSchemaGen.Generation;

public static class ProtobufKeyDecoder
{
    public const string SimpleStringFunctionName = "sandbox_parse_proto_string";
    public const string KeyAlias = "_sandbox_key";
    private const int MaxKeyFields = 32;

    public static string CreateSimpleStringFunctionStatement() =>
        $"""
        CREATE OR REPLACE FUNCTION {SimpleStringFunctionName} AS (raw, skip_bytes, field_number) ->
            if(
                length(raw) <= skip_bytes,
                '',
                if(
                    reinterpretAsUInt8(substring(substring(raw, skip_bytes + 1), 1, 1)) != bitOr(bitShiftLeft(field_number, 3), 2),
                    '',
                    if(
                        reinterpretAsUInt8(substring(substring(raw, skip_bytes + 1), 2, 1)) < 128,
                        substring(
                            substring(raw, skip_bytes + 1),
                            3,
                            reinterpretAsUInt8(substring(substring(raw, skip_bytes + 1), 2, 1))),
                        if(
                            reinterpretAsUInt8(substring(substring(raw, skip_bytes + 1), 3, 1)) >= 128,
                            '',
                            substring(
                                substring(raw, skip_bytes + 1),
                                4,
                                bitOr(
                                    bitAnd(reinterpretAsUInt8(substring(substring(raw, skip_bytes + 1), 2, 1)), 127),
                                    bitShiftLeft(reinterpretAsUInt8(substring(substring(raw, skip_bytes + 1), 3, 1)), 7)))
                        )
                    )
                )
            );
        """;

    public static string CreateWireScannerFunctionStatements() =>
        $"""
        SET max_ast_elements = 10000000;
        SET max_expanded_ast_elements = 10000000;

        CREATE OR REPLACE FUNCTION sandbox_u8 AS (d, p) ->
            reinterpretAsUInt8(substring(d, p, 1));

        CREATE OR REPLACE FUNCTION sandbox_vi AS (d, p) ->
            if(
                sandbox_u8(d, p) < 128,
                (toUInt64(sandbox_u8(d, p)), p + 1),
                if(
                    sandbox_u8(d, p + 1) < 128,
                    (
                        toUInt64(bitAnd(sandbox_u8(d, p), 127))
                            + bitShiftLeft(toUInt64(sandbox_u8(d, p + 1)), 7),
                        p + 2
                    ),
                    (
                        toUInt64(bitAnd(sandbox_u8(d, p), 127))
                            + bitShiftLeft(toUInt64(bitAnd(sandbox_u8(d, p + 1), 127)), 7)
                            + bitShiftLeft(toUInt64(sandbox_u8(d, p + 2)), 14),
                        p + 3
                    )
                )
            );

        -- Walk protobuf wire fields into Map(field_number -> raw value bytes).
        CREATE OR REPLACE FUNCTION sandbox_proto_fields AS (payload) ->
            arrayFold(
                (acc, i) -> if(
                    acc.1 = 0 OR acc.1 > length(payload),
                    acc,
                    multiIf(
                        bitAnd(sandbox_vi(payload, acc.1).1, 7) = 2,
                        (
                            sandbox_vi(payload, sandbox_vi(payload, acc.1).2).2
                                + sandbox_vi(payload, sandbox_vi(payload, acc.1).2).1,
                            mapUpdate(
                                acc.2,
                                map(
                                    toUInt32(bitShiftRight(sandbox_vi(payload, acc.1).1, 3)),
                                    substring(
                                        payload,
                                        sandbox_vi(payload, sandbox_vi(payload, acc.1).2).2,
                                        sandbox_vi(payload, sandbox_vi(payload, acc.1).2).1)))
                        ),
                        bitAnd(sandbox_vi(payload, acc.1).1, 7) = 0,
                        (
                            sandbox_vi(payload, sandbox_vi(payload, acc.1).2).2,
                            mapUpdate(
                                acc.2,
                                map(
                                    toUInt32(bitShiftRight(sandbox_vi(payload, acc.1).1, 3)),
                                    substring(
                                        payload,
                                        sandbox_vi(payload, acc.1).2,
                                        sandbox_vi(payload, sandbox_vi(payload, acc.1).2).2
                                            - sandbox_vi(payload, acc.1).2)))
                        ),
                        bitAnd(sandbox_vi(payload, acc.1).1, 7) = 1,
                        (
                            sandbox_vi(payload, acc.1).2 + 8,
                            mapUpdate(
                                acc.2,
                                map(
                                    toUInt32(bitShiftRight(sandbox_vi(payload, acc.1).1, 3)),
                                    substring(payload, sandbox_vi(payload, acc.1).2, 8)))
                        ),
                        bitAnd(sandbox_vi(payload, acc.1).1, 7) = 5,
                        (
                            sandbox_vi(payload, acc.1).2 + 4,
                            mapUpdate(
                                acc.2,
                                map(
                                    toUInt32(bitShiftRight(sandbox_vi(payload, acc.1).1, 3)),
                                    substring(payload, sandbox_vi(payload, acc.1).2, 4)))
                        ),
                        (toUInt64(0), acc.2)
                    )
                ),
                range({MaxKeyFields}),
                (toUInt64(1), CAST(map(), 'Map(UInt32, String)'))
            ).2;
        """;

    public static string CreateKeyMessageParseFunction(
        string kafkaTableName,
        int skipBytes,
        IReadOnlyList<ProtobufKeyField> fields)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(kafkaTableName);
        ArgumentOutOfRangeException.ThrowIfNegative(skipBytes);
        if (fields.Count == 0)
            throw new ArgumentException("Key fields are required.", nameof(fields));
        if (fields.Count > MaxKeyFields)
        {
            throw new InvalidOperationException(
                $"Kafka key for '{kafkaTableName}' has {fields.Count} fields; maximum is {MaxKeyFields}.");
        }

        var tupleArgs = string.Join(",\n        ", fields.Select(field => field.TupleValueExpression));
        return
            $"""
            CREATE OR REPLACE FUNCTION {ParseFunctionName(kafkaTableName)} AS (raw) ->
            (
                SELECT (
                    {tupleArgs}
                )
                FROM (
                    SELECT sandbox_proto_fields(
                        if(length(raw) <= {skipBytes}, '', substring(raw, {skipBytes} + 1))
                    ) AS f
                )
            );
            """;
    }

    public static string ParseFunctionName(string kafkaTableName) =>
        $"sandbox_parse_key_{kafkaTableName}";

    public static string SimpleStringFieldExpression(int skipBytes, int fieldNumber) =>
        $"{SimpleStringFunctionName}(_key, {skipBytes}, {fieldNumber})";

    public static string KeyTupleElementExpression(int oneBasedIndex) =>
        $"tupleElement({KeyAlias}, {oneBasedIndex})";

    public static string KeyParseProjection(string kafkaTableName) =>
        $"{ParseFunctionName(kafkaTableName)}(_key) AS {KeyAlias}";
}
