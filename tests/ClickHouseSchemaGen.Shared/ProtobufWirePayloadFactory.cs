namespace ClickHouseSchemaGen.Shared;

public static class ProtobufWirePayloadFactory
{
    public static readonly byte[] ConfluentEnvelope = [0x00, 0x00, 0x00, 0x00, 0x2A, 0x00];

    public static byte[] FrameConfluent(byte[] protobufPayload) =>
        [.. ConfluentEnvelope, .. protobufPayload];

    public static byte[] EncodeOrderKey(string orderId) =>
        new OrderKey { OrderId = orderId }.ToByteArray();

    public static byte[] WithSurroundingUnknownFields(byte[] payload, UnknownFieldNoise noise)
    {
        var prefix = EncodeUnknownFields(noise with { Seed = unchecked(noise.Seed * 397) ^ 0xA5A5 });
        var suffix = EncodeUnknownFields(noise);
        return [.. prefix, .. payload, .. suffix];
    }

    public static byte[] EncodeUnknownFields(UnknownFieldNoise noise)
    {
        using var stream = new MemoryStream();
        WriteTag(stream, 90, WireType.Varint);
        WriteVarint(stream, unchecked((ulong)noise.Seed));

        WriteTag(stream, 91, WireType.Fixed32);
        Span<byte> fixed32 = stackalloc byte[4];
        BitConverter.TryWriteBytes(fixed32, (uint)noise.Seed);
        stream.Write(fixed32);

        WriteTag(stream, 92, WireType.Fixed64);
        Span<byte> fixed64 = stackalloc byte[8];
        BitConverter.TryWriteBytes(fixed64, (ulong)(uint)noise.Seed << 32 | (uint)noise.ExtraByte);
        stream.Write(fixed64);

        WriteTag(stream, 93, WireType.LengthDelimited);
        var nested = new byte[] { noise.ExtraByte, (byte)~noise.ExtraByte, 0x00, 0xFF };
        WriteVarint(stream, (ulong)nested.Length);
        stream.Write(nested);

        WriteTag(stream, 94, WireType.LengthDelimited);
        var packed = EncodePackedVarints(noise);
        WriteVarint(stream, (ulong)packed.Length);
        stream.Write(packed);

        return stream.ToArray();
    }

    public static Dictionary<string, string> ExpectedCompositeKeyValues(CompositeKey key, string nullMarker)
    {
        var statusName = CompositeKey.Descriptor.FindFieldByNumber(16)!.EnumType
            .FindValueByNumber((int)key.Status)!.Name;
        var scope = key.Scope ?? new KeyScope();
        var issuedAt = key.IssuedAt ?? new Timestamp();

        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["_key.tenant"] = key.Tenant ?? string.Empty,
            ["_key.int32_value"] = key.Int32Value.ToString(CultureInfo.InvariantCulture),
            ["_key.int64_value"] = key.Int64Value.ToString(CultureInfo.InvariantCulture),
            ["_key.uint32_value"] = key.Uint32Value.ToString(CultureInfo.InvariantCulture),
            ["_key.uint64_value"] = key.Uint64Value.ToString(CultureInfo.InvariantCulture),
            ["_key.sint32_value"] = key.Sint32Value.ToString(CultureInfo.InvariantCulture),
            ["_key.sint64_value"] = key.Sint64Value.ToString(CultureInfo.InvariantCulture),
            ["_key.fixed32_value"] = key.Fixed32Value.ToString(CultureInfo.InvariantCulture),
            ["_key.fixed64_value"] = key.Fixed64Value.ToString(CultureInfo.InvariantCulture),
            ["_key.sfixed32_value"] = key.Sfixed32Value.ToString(CultureInfo.InvariantCulture),
            ["_key.sfixed64_value"] = key.Sfixed64Value.ToString(CultureInfo.InvariantCulture),
            ["_key.flag"] = key.Flag ? "1" : "0",
            ["_key.ratio"] = BitConverter.SingleToUInt32Bits(key.Ratio).ToString(CultureInfo.InvariantCulture),
            ["_key.score"] = BitConverter.DoubleToUInt64Bits(key.Score).ToString(CultureInfo.InvariantCulture),
            ["_key.raw"] = Convert.ToHexString(key.Raw.Span),
            ["_key.status"] = statusName,
            ["_key.region"] = key.HasRegion ? key.Region : nullMarker,
            ["_key.scope.name"] = scope.Name ?? string.Empty,
            ["_key.scope.level"] = scope.Level.ToString(CultureInfo.InvariantCulture),
            ["_key.issued_at.seconds"] = issuedAt.Seconds.ToString(CultureInfo.InvariantCulture),
            ["_key.issued_at.nanos"] = issuedAt.Nanos.ToString(CultureInfo.InvariantCulture),
            ["_key.label"] = key.Label is null ? nullMarker : key.Label
        };
    }

    public static Dictionary<string, string> ExpectedOrderKeyValues(OrderKey key) =>
        new(StringComparer.Ordinal)
        {
            ["_key.order_id"] = key.OrderId ?? string.Empty
        };

    public static string SelectExpression(ClickHouseColumn column, string nullMarker)
    {
        var name = SqlColumnFormatter.FormatColumnName(column.Name);
        var escapedNull = nullMarker.Replace("'", "''", StringComparison.Ordinal);
        return column.Name switch
        {
            "_key.raw" => $"ifNull(hex({name}), '')",
            "_key.ratio" => $"toString(reinterpretAsUInt32({name}))",
            "_key.score" => $"toString(reinterpretAsUInt64({name}))",
            _ when column.Type.StartsWith("Nullable(", StringComparison.Ordinal) =>
                $"ifNull(toString({name}), '{escapedNull}')",
            _ => $"toString({name})"
        };
    }

    private static byte[] EncodePackedVarints(UnknownFieldNoise noise)
    {
        using var stream = new MemoryStream();
        WriteVarint(stream, (ulong)(uint)Math.Abs(noise.Seed % 127));
        WriteVarint(stream, unchecked((ulong)noise.Seed));
        WriteVarint(stream, (ulong)noise.ExtraByte);
        return stream.ToArray();
    }

    private static void WriteTag(Stream stream, int fieldNumber, WireType wireType) =>
        WriteVarint(stream, ((ulong)fieldNumber << 3) | (ulong)wireType);

    private static void WriteVarint(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }

        stream.WriteByte((byte)value);
    }

    private enum WireType : byte
    {
        Varint = 0,
        Fixed64 = 1,
        LengthDelimited = 2,
        Fixed32 = 5
    }
}

public readonly record struct UnknownFieldNoise(int Seed, byte ExtraByte);
