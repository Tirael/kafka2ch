namespace ClickHouseSchemaGen.Mapping;

public sealed record MappingContext
{
    public required CodegenDefaults Defaults { get; init; }

    public required IReadOnlyDictionary<string, FieldOverrideConfig> FieldOverrides { get; init; }

    public int Depth { get; init; }

    public string? ColumnPathPrefix { get; init; }

    public string Qualify(string columnPath) =>
        string.IsNullOrEmpty(ColumnPathPrefix) ? columnPath : $"{ColumnPathPrefix}.{columnPath}";

    public FieldOverrideConfig? GetOverride(string columnPath) =>
        FieldOverrides.GetValueOrDefault(Qualify(columnPath));

    public MappingStrategy? GetOverrideStrategy(string columnPath) =>
        Enum.TryParse<MappingStrategy>(GetOverride(columnPath)?.Strategy, ignoreCase: true, out var parsed)
            ? parsed
            : null;
}
