namespace ClickHouseSchemaGen.Migration;

public static partial class TypeCompatibility
{
    public static TypeChangeKind Classify(string oldType, string newType)
    {
        if (string.Equals(oldType, newType, StringComparison.Ordinal))
            return TypeChangeKind.Safe;

        if (IsNullable(oldType) && !IsNullable(newType))
            return TypeChangeKind.Destructive;

        if (!IsNullable(oldType) && IsNullable(newType) && StripNullable(newType) == oldType)
            return TypeChangeKind.Safe;

        if (IsLowCardinality(oldType) && StripLowCardinality(oldType) == newType)
            return TypeChangeKind.Safe;

        if (IsLowCardinality(newType) && StripLowCardinality(newType) == oldType)
            return TypeChangeKind.Safe;

        if (IsIntegerWidening(oldType, newType))
            return TypeChangeKind.Safe;

        if (oldType == "Float32" && newType == "Float64")
            return TypeChangeKind.Safe;

        if (oldType == "DateTime" && newType.StartsWith("DateTime64", StringComparison.Ordinal))
            return TypeChangeKind.Rewrite;

        if (TryClassifyEnumChange(oldType, newType, out var enumKind))
            return enumKind;

        if (ContainsComplexType(oldType) || ContainsComplexType(newType))
            return TypeChangeKind.Manual;

        return TypeChangeKind.Manual;
    }

    private static bool IsNullable(string type) =>
        type.StartsWith("Nullable(", StringComparison.Ordinal) && type.EndsWith(')');

    private static string StripNullable(string type) =>
        type["Nullable(".Length..^1];

    private static bool IsLowCardinality(string type) =>
        type.StartsWith("LowCardinality(", StringComparison.Ordinal) && type.EndsWith(')');

    private static string StripLowCardinality(string type) =>
        type["LowCardinality(".Length..^1];

    private static bool IsIntegerWidening(string oldType, string newType) =>
        IntegerTypes.TryGetValue(oldType, out var oldInt)
        && IntegerTypes.TryGetValue(newType, out var newInt)
        && (!oldInt.Signed || newInt.Signed)
        && newInt.Bits > oldInt.Bits;

    private static bool TryClassifyEnumChange(string oldType, string newType, out TypeChangeKind kind)
    {
        kind = TypeChangeKind.Manual;

        if (!TryParseEnum(oldType, out var oldEnum) || !TryParseEnum(newType, out var newEnum))
            return false;

        if (oldEnum.Width == 8 && newEnum.Width == 16 && oldEnum.Values.IsSubsetOf(newEnum.Values))
        {
            kind = TypeChangeKind.Rewrite;
            return true;
        }

        if (oldEnum.Width == newEnum.Width && newEnum.Values.IsSupersetOf(oldEnum.Values))
        {
            kind = TypeChangeKind.Safe;
            return true;
        }

        if (newEnum.Values.IsProperSubsetOf(oldEnum.Values))
        {
            kind = TypeChangeKind.Destructive;
            return true;
        }

        return false;
    }

    private static bool ContainsComplexType(string type) =>
        type.Contains("Nested(", StringComparison.Ordinal)
        || type.Contains("Tuple(", StringComparison.Ordinal)
        || type.Contains("Map(", StringComparison.Ordinal)
        || type.Contains("Array(", StringComparison.Ordinal);

    private static bool TryParseEnum(string type, out EnumTypeInfo info)
    {
        info = default!;
        var match = EnumTypeRegex().Match(type);
        if (!match.Success)
            return false;

        var width = int.Parse(match.Groups["width"].Value);
        var values = match.Groups["values"].Value
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(v => v.Split('\'')[1])
            .ToHashSet(StringComparer.Ordinal);

        info = new EnumTypeInfo(width, values);
        return true;
    }

    private sealed record EnumTypeInfo(int Width, HashSet<string> Values);

    private sealed record IntegerTypeInfo(bool Signed, int Bits);

    private static readonly Dictionary<string, IntegerTypeInfo> IntegerTypes = new(StringComparer.Ordinal)
    {
        ["Int8"] = new(true, 8),
        ["Int16"] = new(true, 16),
        ["Int32"] = new(true, 32),
        ["Int64"] = new(true, 64),
        ["UInt8"] = new(false, 8),
        ["UInt16"] = new(false, 16),
        ["UInt32"] = new(false, 32),
        ["UInt64"] = new(false, 64)
    };

    [GeneratedRegex(@"^Enum(?<width>8|16)\((?<values>.+)\)$", RegexOptions.CultureInvariant)]
    private static partial Regex EnumTypeRegex();
}
