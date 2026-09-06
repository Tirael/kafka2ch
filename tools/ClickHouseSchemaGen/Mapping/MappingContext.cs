namespace ClickHouseSchemaGen.Mapping;

public sealed record MappingContext
{
    public required CodegenDefaults Defaults { get; init; }

    public required IReadOnlyDictionary<string, FieldOverrideConfig> FieldOverrides { get; init; }

    public int Depth { get; init; }

    public FieldOverrideConfig? GetOverride(string columnPath) =>
        FieldOverrides.TryGetValue(columnPath, out var fieldOverride) ? fieldOverride : null;

    public MappingStrategy? GetOverrideStrategy(string columnPath)
    {
        var strategy = GetOverride(columnPath)?.Strategy;
        return strategy is not null
            && Enum.TryParse<MappingStrategy>(strategy, ignoreCase: true, out var parsed)
            ? parsed
            : null;
    }
}
