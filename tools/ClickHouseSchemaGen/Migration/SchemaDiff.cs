namespace ClickHouseSchemaGen.Migration;

public sealed record SchemaDiff
{
    public required IReadOnlyList<SchemaChange> Changes { get; init; }

    public required IReadOnlyList<ClickHouseColumn> Unchanged { get; init; }

    public bool IsEmpty => Changes.Count == 0;
}
