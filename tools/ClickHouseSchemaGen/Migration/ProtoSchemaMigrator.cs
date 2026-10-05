namespace ClickHouseSchemaGen.Migration;

/// <summary>
/// High-level API: map two protobuf message descriptors with the same codegen rules,
/// diff the resulting ClickHouse columns, and emit <c>ALTER TABLE</c> SQL.
/// </summary>
public sealed class ProtoSchemaMigrator(DenormalizationPlanner planner)
{
    public ProtoSchemaMigrator()
        : this(new DenormalizationPlanner())
    {
    }

    public SchemaDiff Diff(
        MessageDescriptor fromDescriptor,
        MessageDescriptor toDescriptor,
        CodegenDefaults? defaults = null,
        IReadOnlyDictionary<string, FieldOverrideConfig>? overrides = null)
    {
        ArgumentNullException.ThrowIfNull(fromDescriptor);
        ArgumentNullException.ThrowIfNull(toDescriptor);

        defaults ??= new CodegenDefaults();
        overrides ??= new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase);

        var fromColumns = planner.MapMessage(fromDescriptor, defaults, overrides);
        var toColumns = planner.MapMessage(toDescriptor, defaults, overrides);
        return ColumnSchemaComparer.Compare(fromColumns, toColumns);
    }

    public SchemaDiff Diff(
        string fromMessageType,
        string toMessageType,
        CodegenDefaults? defaults = null,
        IReadOnlyDictionary<string, FieldOverrideConfig>? overrides = null) =>
        Diff(
            ProtoDescriptorResolver.ResolveDescriptor(fromMessageType),
            ProtoDescriptorResolver.ResolveDescriptor(toMessageType),
            defaults,
            overrides);

    public string GenerateAlterTableSql(
        string tableName,
        MessageDescriptor fromDescriptor,
        MessageDescriptor toDescriptor,
        CodegenDefaults? defaults = null,
        IReadOnlyDictionary<string, FieldOverrideConfig>? overrides = null,
        AlterTableMigrationOptions? options = null)
    {
        var diff = Diff(fromDescriptor, toDescriptor, defaults, overrides);
        return AlterTableMigrationGenerator.Generate(tableName, diff, options);
    }

    public string GenerateAlterTableSql(
        string tableName,
        string fromMessageType,
        string toMessageType,
        CodegenDefaults? defaults = null,
        IReadOnlyDictionary<string, FieldOverrideConfig>? overrides = null,
        AlterTableMigrationOptions? options = null)
    {
        var diff = Diff(fromMessageType, toMessageType, defaults, overrides);
        return AlterTableMigrationGenerator.Generate(tableName, diff, options);
    }
}
