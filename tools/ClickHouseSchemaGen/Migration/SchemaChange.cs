namespace ClickHouseSchemaGen.Migration;

public sealed record SchemaChange
{
    public required SchemaChangeKind Kind { get; init; }

    public required string ColumnName { get; init; }

    public string? NewType { get; init; }

    public string? OldType { get; init; }

    public string? Comment { get; init; }
}
