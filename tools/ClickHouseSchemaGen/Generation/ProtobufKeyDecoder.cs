namespace ClickHouseSchemaGen.Generation;

public static class ProtobufKeyDecoder
{
    public const string FunctionName = "sandbox_parse_proto_string";

    public static string CreateFunctionStatement() =>
        $"""
        CREATE OR REPLACE FUNCTION {FunctionName} AS (raw, skip_bytes, field_number) ->
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

    public static string StringFieldExpression(int skipBytes, int fieldNumber) =>
        $"{FunctionName}(_key, {skipBytes}, {fieldNumber})";
}
