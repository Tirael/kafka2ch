namespace ClickHouseSchemaGen.Mapping;

public sealed record MappingContext
{
    public required CodegenDefaults Defaults { get; init; }

    public required IReadOnlyDictionary<string, FieldOverrideConfig> FieldOverrides { get; init; }

    public int Depth { get; init; }

    public FieldOverrideConfig? GetOverride(string columnPath) =>
        FieldOverrides.GetValueOrDefault(columnPath);

    public MappingStrategy? GetOverrideStrategy(string columnPath)
    {
        var strategyName = GetOverride(columnPath)?.Strategy;
        if (strategyName is null)
            return null;

        if (Enum.TryParse<MappingStrategy>(strategyName, ignoreCase: true, out var parsed))
            return parsed;

        return null;
    }
}
