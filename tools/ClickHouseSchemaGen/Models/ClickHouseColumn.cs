namespace ClickHouseSchemaGen.Models;

public sealed record ClickHouseColumn
{
    public required string Name { get; init; }

    public required string Type { get; init; }

    public MappingStrategy Strategy { get; init; } = MappingStrategy.Direct;

    public string? Comment { get; init; }

    public string SourceFieldPath { get; init; } = "";

    /// <summary>
    /// Stable identity: proto field-number path (e.g. "3.2") or kafka meta (e.g. "kafka:_key").
    /// </summary>
    public string FieldNumberPath { get; init; } = "";

    /// <summary>
    /// True when this column, or a field nested inside it, is a flattened google.protobuf.*Value wrapper.
    /// </summary>
    public bool FlattensGoogleWrapper { get; init; }

    public static ClickHouseColumn Create(
        string name,
        string type,
        MappingStrategy strategy,
        string? comment = null,
        bool flattensGoogleWrapper = false,
        string fieldNumberPath = "") =>
        new()
        {
            Name = name,
            Type = type,
            Strategy = strategy,
            Comment = comment,
            SourceFieldPath = name,
            FieldNumberPath = fieldNumberPath,
            FlattensGoogleWrapper = flattensGoogleWrapper
        };
}
