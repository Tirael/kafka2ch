namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// Creates a 1:1 materialized view for each MergeTree table whose columns were
/// auto-filled from a Kafka queue and that has no explicit view of its own.
/// </summary>
public static class MaterializedViewAutoGenerator
{
    public static string DefaultName(string tableName) => $"{tableName}_mv";

    public static bool ShouldCreate(
        MergeTreeTableConfig table,
        IReadOnlyList<MaterializedViewConfig> explicitViews) =>
        table.Columns.Count == 0
        && !string.IsNullOrWhiteSpace(table.SourceTable)
        && explicitViews.All(view =>
            !string.Equals(view.TargetTable, table.TableName, StringComparison.OrdinalIgnoreCase));

    public static IReadOnlyList<MaterializedViewConfig> CreateForAutoColumns(
        IReadOnlyList<MergeTreeTableConfig> originalTables,
        IReadOnlyList<MergeTreeTableConfig> expandedTables,
        IReadOnlyList<MaterializedViewConfig> explicitViews,
        Func<string, PersistKafkaMetaConfig>? metaForSourceTable = null)
    {
        if (originalTables.Count != expandedTables.Count)
        {
            throw new ArgumentException(
                "Original and expanded MergeTree tables must be aligned.",
                nameof(expandedTables));
        }

        var usedNames = explicitViews
            .Select(view => view.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var created = new List<MaterializedViewConfig>();

        for (var i = 0; i < originalTables.Count; i++)
        {
            var original = originalTables[i];
            if (!ShouldCreate(original, explicitViews))
                continue;

            var name = DefaultName(original.TableName);
            if (!usedNames.Add(name))
            {
                throw new InvalidOperationException(
                    $"MergeTree table '{original.TableName}' auto-generates materialized view '{name}', " +
                    "but that name is already used. Rename the existing view or declare this mirror view explicitly.");
            }

            var columns = expandedTables[i].Columns
                .Select(column => new PipelineColumnMapping
                {
                    Source = column.Name,
                    Target = column.Name
                })
                .ToList();

            var meta = metaForSourceTable?.Invoke(original.SourceTable!) ?? new PersistKafkaMetaConfig();
            if (meta.AnyEnabled)
            {
                // Auto mirror uses queue column names for payload; replace kafka_* targets with virtual sources.
                columns.RemoveAll(mapping => KafkaMetaColumnFactory.IsKafkaMetaPath(
                    expandedTables[i].Columns
                        .FirstOrDefault(column => column.Name == mapping.Target)
                        ?.FieldNumberPath));
                columns.AddRange(KafkaMetaColumnFactory.CreateMappings(meta));
            }

            created.Add(new MaterializedViewConfig
            {
                Name = name,
                TargetTable = original.TableName,
                SourceTable = original.SourceTable!,
                Columns = columns
            });
        }

        return created;
    }
}
