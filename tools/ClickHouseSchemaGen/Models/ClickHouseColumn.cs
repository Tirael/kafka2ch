namespace ClickHouseSchemaGen.Models;

public sealed record ClickHouseColumn
{
    public required string Name { get; init; }

    public required string Type { get; init; }

    public MappingStrategy Strategy { get; init; } = MappingStrategy.Direct;

    public string? Comment { get; init; }

    public string SourceFieldPath { get; init; } = "";


    public string FieldNumberPath { get; init; } = "";


    public bool FlattensGoogleWrapper { get; init; }


    public string? SourceExpression { get; init; }

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
