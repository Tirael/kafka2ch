namespace ClickHouseSchemaGen.Mapping;

public sealed record FieldMappingRequest(
    FieldDescriptor Field,
    string ColumnPath,
    MappingContext Context)
{
    public bool ForceNullable { get; init; }

    public string? OverrideType =>
        Context.GetOverride(ColumnPath)?.Type is not { } type || string.IsNullOrWhiteSpace(type)
            ? null
            : type;

    public FieldMappingRequest WithColumnPath(string columnPath) =>
        this with { ColumnPath = columnPath };

    public FieldMappingRequest WithForceNullable(bool forceNullable = true) =>
        this with { ForceNullable = forceNullable };
}
