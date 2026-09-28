namespace ClickHouseSchemaGen.Generation;

public static class ProtobufKeyDecoder
{
    public const string KeyFieldsAlias = "_sandbox_key_fields";
    public const string KeyPayloadAlias = "_sandbox_key_payload";
    private const int MaxKeyFields = 32;

    public static string SimpleStringFieldExpression(int skipBytes, int fieldNumber) =>
        "if(" +
        $"length(_key) <= {skipBytes}, " +
        "'', " +
        "if(" +
        $"reinterpretAsUInt8(substring(substring(_key, {skipBytes} + 1), 1, 1)) != bitOr(bitShiftLeft({fieldNumber}, 3), 2), " +
        "'', " +
        "if(" +
        $"reinterpretAsUInt8(substring(substring(_key, {skipBytes} + 1), 2, 1)) < 128, " +
        $"substring(substring(_key, {skipBytes} + 1), 3, reinterpretAsUInt8(substring(substring(_key, {skipBytes} + 1), 2, 1))), " +
        "if(" +
        $"reinterpretAsUInt8(substring(substring(_key, {skipBytes} + 1), 3, 1)) >= 128, " +
        "'', " +
        $"substring(substring(_key, {skipBytes} + 1), 4, bitOr(bitAnd(reinterpretAsUInt8(substring(substring(_key, {skipBytes} + 1), 2, 1)), 127), bitShiftLeft(reinterpretAsUInt8(substring(substring(_key, {skipBytes} + 1), 3, 1)), 7)))" +
        "))))";

    public static string PayloadExpression(string rawExpression, int skipBytes) =>
        $"if(length({rawExpression}) <= {skipBytes}, '', substring({rawExpression}, {skipBytes} + 1))";

    public static string ProtoFieldsExpression(string payloadExpression) =>
        $"""
        arrayFold(
            (acc, i) -> if(
                acc.1 = 0 OR acc.1 > length({payloadExpression}),
                acc,
                multiIf(
                    bitAnd({VarintAt(payloadExpression, "acc.1")}.1, 7) = 2,
                    (
                        {VarintAt(payloadExpression, $"{VarintAt(payloadExpression, "acc.1")}.2")}.2
                            + {VarintAt(payloadExpression, $"{VarintAt(payloadExpression, "acc.1")}.2")}.1,
                        mapUpdate(
                            acc.2,
                            map(
                                toUInt32(bitShiftRight({VarintAt(payloadExpression, "acc.1")}.1, 3)),
                                substring(
                                    {payloadExpression},
                                    {VarintAt(payloadExpression, $"{VarintAt(payloadExpression, "acc.1")}.2")}.2,
                                    {VarintAt(payloadExpression, $"{VarintAt(payloadExpression, "acc.1")}.2")}.1)))
                    ),
                    bitAnd({VarintAt(payloadExpression, "acc.1")}.1, 7) = 0,
                    (
                        {VarintAt(payloadExpression, $"{VarintAt(payloadExpression, "acc.1")}.2")}.2,
                        mapUpdate(
                            acc.2,
                            map(
                                toUInt32(bitShiftRight({VarintAt(payloadExpression, "acc.1")}.1, 3)),
                                substring(
                                    {payloadExpression},
                                    {VarintAt(payloadExpression, "acc.1")}.2,
                                    {VarintAt(payloadExpression, $"{VarintAt(payloadExpression, "acc.1")}.2")}.2
                                        - {VarintAt(payloadExpression, "acc.1")}.2)))
                    ),
                    bitAnd({VarintAt(payloadExpression, "acc.1")}.1, 7) = 1,
                    (
                        {VarintAt(payloadExpression, "acc.1")}.2 + 8,
                        mapUpdate(
                            acc.2,
                            map(
                                toUInt32(bitShiftRight({VarintAt(payloadExpression, "acc.1")}.1, 3)),
                                substring({payloadExpression}, {VarintAt(payloadExpression, "acc.1")}.2, 8)))
                    ),
                    bitAnd({VarintAt(payloadExpression, "acc.1")}.1, 7) = 5,
                    (
                        {VarintAt(payloadExpression, "acc.1")}.2 + 4,
                        mapUpdate(
                            acc.2,
                            map(
                                toUInt32(bitShiftRight({VarintAt(payloadExpression, "acc.1")}.1, 3)),
                                substring({payloadExpression}, {VarintAt(payloadExpression, "acc.1")}.2, 4)))
                    ),
                    (toUInt64(0), acc.2)
                )
            ),
            range({MaxKeyFields}),
            (toUInt64(1), CAST(map(), 'Map(UInt32, String)'))
        ).2
        """;

    public static string VarintValueExpression(string bytesExpression) =>
        $"{VarintAt(bytesExpression, "1")}.1";

    public static string U8At(string dataExpression, string positionExpression) =>
        $"reinterpretAsUInt8(substring({dataExpression}, {positionExpression}, 1))";

    public static string VarintAt(string dataExpression, string positionExpression) =>
        $"""
        if(
            {U8At(dataExpression, positionExpression)} < 128,
            (toUInt64({U8At(dataExpression, positionExpression)}), {positionExpression} + 1),
            if(
                {U8At(dataExpression, $"{positionExpression} + 1")} < 128,
                (
                    toUInt64(bitAnd({U8At(dataExpression, positionExpression)}, 127))
                        + bitShiftLeft(toUInt64({U8At(dataExpression, $"{positionExpression} + 1")}), 7),
                    {positionExpression} + 2
                ),
                (
                    toUInt64(bitAnd({U8At(dataExpression, positionExpression)}, 127))
                        + bitShiftLeft(toUInt64(bitAnd({U8At(dataExpression, $"{positionExpression} + 1")}, 127)), 7)
                        + bitShiftLeft(toUInt64({U8At(dataExpression, $"{positionExpression} + 2")}), 14),
                    {positionExpression} + 3
                )
            )
        )
        """;
}
