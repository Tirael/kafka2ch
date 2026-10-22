namespace ClickHouseSchemaGen.Shared;

public static class ProtobufWirePayloadGen
{
    private static readonly string[] Tenants =
    [
        "",
        "a",
        "tenant",
        "tenant-ü",
        "eu-west-1",
        new string('x', 48)
    ];

    private static readonly string[] Labels =
    [
        "",
        "lbl",
        "label-ü",
        new string('y', 32)
    ];

    private static readonly byte[][] Raws =
    [
        [],
        [0x00],
        [0xFF],
        [0x00, 0xFF, 0x41],
        [0x01, 0x02, 0x03, 0x04, 0x05]
    ];

    public static Gen<CompositeKey> CompositeKeys() =>
        from tenant in Gen.Elements(Tenants)
        from int32Value in Gen.Elements(0, 1, -1, -5, int.MinValue, int.MaxValue)
        from int64Value in Gen.Elements(0L, -1L, -9_000_000_000L, long.MinValue, long.MaxValue)
        from uint32Value in Gen.Elements(0u, 1u, 4_000_000_000u, uint.MaxValue)
        from uint64Value in Gen.Elements(0UL, 1UL, 18_446_744_073_709_551_000UL, ulong.MaxValue)
        from sint32Value in Gen.Elements(0, 1, -1, -7, int.MinValue / 2, int.MaxValue / 2)
        from sint64Value in Gen.Elements(0L, -1L, -123_456_789_012L, long.MinValue / 2)
        from fixed32Value in Gen.Elements(0u, 1u, uint.MaxValue)
        from fixed64Value in Gen.Elements(0UL, 1_234_567_890_123_456_789UL, ulong.MaxValue)
        from sfixed32Value in Gen.Elements(0, -2, int.MinValue, int.MaxValue)
        from sfixed64Value in Gen.Elements(0L, -3L, long.MinValue, long.MaxValue)
        from flag in Gen.Elements(true, false)
        from ratioBits in Gen.Choose(int.MinValue, int.MaxValue)
        from scoreBitsHi in Gen.Choose(int.MinValue, int.MaxValue)
        from scoreBitsLo in Gen.Choose(int.MinValue, int.MaxValue)
        from raw in Gen.Elements(Raws)
        from status in Gen.Elements(
            SampleStatus.Unspecified,
            SampleStatus.Active,
            SampleStatus.Archived)
        from region in Gen.Frequency(
            (2, Gen.Constant<string?>(null)),
            (1, Gen.Constant<string?>("")),
            (2, Gen.Elements("eu", "us", "ap-south-1").Select(v => (string?)v)))
        from includeScope in Gen.Elements(true, false)
        from scopeName in Gen.Elements("", "team", "scope-ü")
        from scopeLevel in Gen.Elements(0, -1, 42, int.MinValue, int.MaxValue)
        from includeIssuedAt in Gen.Elements(true, false)
        from seconds in Gen.Elements(0L, 1L, 1_700_000_000L, long.MaxValue / 2)
        from nanos in Gen.Elements(0, 1, 5, 999_999_999)
        from includeLabel in Gen.Elements(true, false)
        from label in Gen.Elements(Labels)
        select BuildCompositeKey(
            tenant,
            int32Value,
            int64Value,
            uint32Value,
            uint64Value,
            sint32Value,
            sint64Value,
            fixed32Value,
            fixed64Value,
            sfixed32Value,
            sfixed64Value,
            flag,
            FiniteFloat(ratioBits),
            FiniteDouble(((long)scoreBitsHi << 32) | (uint)scoreBitsLo),
            raw,
            status,
            region,
            includeScope,
            scopeName,
            scopeLevel,
            includeIssuedAt,
            seconds,
            nanos,
            includeLabel,
            label);

    public static Gen<OrderKey> OrderKeys() =>
        from orderId in Gen.Elements("", "o-1", "order-ü", new string('z', 64))
        select new OrderKey { OrderId = orderId };

    public static Gen<UnknownFieldNoise> UnknownFieldNoises() =>
        from seed in Gen.Choose(int.MinValue, int.MaxValue)
        from extra in Gen.Choose(0, 255)
        select new UnknownFieldNoise(seed, (byte)extra);

    public static Gen<(CompositeKey Key, UnknownFieldNoise Noise)> CompositeKeyCases() =>
        from key in CompositeKeys()
        from noise in UnknownFieldNoises()
        select (key, noise);

    public static Gen<(OrderKey Key, UnknownFieldNoise Noise)> OrderKeyCases() =>
        from key in OrderKeys()
        from noise in UnknownFieldNoises()
        select (key, noise);

    public static IReadOnlyList<T> Sample<T>(Gen<T> gen, int count, int size = 40) =>
        Gen.Sample(gen, count, size);

    private static CompositeKey BuildCompositeKey(
        string tenant,
        int int32Value,
        long int64Value,
        uint uint32Value,
        ulong uint64Value,
        int sint32Value,
        long sint64Value,
        uint fixed32Value,
        ulong fixed64Value,
        int sfixed32Value,
        long sfixed64Value,
        bool flag,
        float ratio,
        double score,
        byte[] raw,
        SampleStatus status,
        string? region,
        bool includeScope,
        string scopeName,
        int scopeLevel,
        bool includeIssuedAt,
        long seconds,
        int nanos,
        bool includeLabel,
        string label)
    {
        var key = new CompositeKey
        {
            Tenant = tenant,
            Int32Value = int32Value,
            Int64Value = int64Value,
            Uint32Value = uint32Value,
            Uint64Value = uint64Value,
            Sint32Value = sint32Value,
            Sint64Value = sint64Value,
            Fixed32Value = fixed32Value,
            Fixed64Value = fixed64Value,
            Sfixed32Value = sfixed32Value,
            Sfixed64Value = sfixed64Value,
            Flag = flag,
            Ratio = ratio,
            Score = score,
            Raw = ByteString.CopyFrom(raw),
            Status = status
        };

        if (region is not null)
            key.Region = region;

        if (includeScope)
            key.Scope = new KeyScope { Name = scopeName, Level = scopeLevel };

        if (includeIssuedAt)
            key.IssuedAt = new Timestamp { Seconds = seconds, Nanos = nanos };

        if (includeLabel)
            key.Label = label;

        return key;
    }

    private static float FiniteFloat(int bits)
    {
        var value = BitConverter.Int32BitsToSingle(bits);
        return float.IsFinite(value) ? value : 0f;
    }

    private static double FiniteDouble(long bits)
    {
        var value = BitConverter.Int64BitsToDouble(bits);
        return double.IsFinite(value) ? value : 0d;
    }
}
