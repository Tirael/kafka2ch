namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// Fills empty <c>pipeline.*.columns</c> from the mapped Kafka queue schema and optional kafka meta.
/// </summary>
public static class PipelineColumnExpander
{
    public static MergeTreeTableConfig ExpandMergeTreeTable(
        MergeTreeTableConfig table,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable) =>
        ExpandMergeTreeTable(table, queueColumnsByTable, new PersistKafkaMetaConfig());

    public static MergeTreeTableConfig ExpandMergeTreeTable(
        MergeTreeTableConfig table,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable,
        PersistKafkaMetaConfig meta,
        IReadOnlyList<ClickHouseColumn>? keyColumns = null)
    {
        List<PipelineColumnConfig> columns;

        if (table.Columns.Count > 0)
        {
            columns = table.Columns
                .Select(column => new PipelineColumnConfig
                {
                    Name = column.Name,
                    Type = column.Type,
                    FieldNumberPath = column.FieldNumberPath
                        ?? ResolveFieldNumberPath(column.Name, table.SourceTable, queueColumnsByTable)
                })
                .ToList();

            if (table.IncludeKafkaMeta == true)
                AppendMissingMetaColumns(columns, meta, keyColumns);
        }
        else
        {
            var sourceColumns = ResolveSourceColumns(
                table.SourceTable,
                queueColumnsByTable,
                table.TableName,
                "MergeTree table");
            columns = sourceColumns
                .Select(column => new PipelineColumnConfig
                {
                    Name = column.Name,
                    Type = column.Type,
                    FieldNumberPath = column.FieldNumberPath
                })
                .ToList();

            if (meta.AnyEnabled)
                AppendMissingMetaColumns(columns, meta, keyColumns);
        }

        return new MergeTreeTableConfig
        {
            TableName = table.TableName,
            OrderBy = table.OrderBy,
            Ttl = table.Ttl,
            ShardingKey = table.ShardingKey,
            SourceTable = table.SourceTable,
            IncludeKafkaMeta = table.IncludeKafkaMeta,
            Columns = columns
        };
    }

    public static MaterializedViewConfig ExpandMaterializedView(
        MaterializedViewConfig view,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable) =>
        ExpandMaterializedView(view, queueColumnsByTable, new PersistKafkaMetaConfig(), targetTable: null);

    public static MaterializedViewConfig ExpandMaterializedView(
        MaterializedViewConfig view,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable,
        PersistKafkaMetaConfig meta,
        MergeTreeTableConfig? targetTable,
        IReadOnlyList<ClickHouseColumn>? keyColumns = null)
    {
        List<PipelineColumnMapping> columns;

        if (view.Columns.Count > 0)
        {
            columns = view.Columns.ToList();
            var includeMeta = view.IncludeKafkaMeta == true
                || (view.IncludeKafkaMeta is null && targetTable?.IncludeKafkaMeta == true);
            if (includeMeta)
                AppendMissingMetaMappings(columns, meta, keyColumns);
        }
        else
        {
            var sourceColumns = ResolveSourceColumns(
                view.SourceTable,
                queueColumnsByTable,
                view.Name,
                "Materialized view");
            columns = sourceColumns
                .Select(column => new PipelineColumnMapping
                {
                    Source = column.Name,
                    Target = column.Name
                })
                .ToList();

            if (meta.AnyEnabled)
                AppendMissingMetaMappings(columns, meta, keyColumns);
        }

        return new MaterializedViewConfig
        {
            Name = view.Name,
            TargetTable = view.TargetTable,
            SourceTable = view.SourceTable,
            IncludeKafkaMeta = view.IncludeKafkaMeta,
            Columns = columns
        };
    }

    private static void AppendMissingMetaColumns(
        List<PipelineColumnConfig> columns,
        PersistKafkaMetaConfig meta,
        IReadOnlyList<ClickHouseColumn>? keyColumns)
    {
        var existing = columns.Select(column => column.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var metaColumn in KafkaMetaColumnFactory.CreateMergeTreeColumns(meta, keyColumns))
        {
            if (existing.Add(metaColumn.Name))
                columns.Add(metaColumn);
        }
    }

    private static void AppendMissingMetaMappings(
        List<PipelineColumnMapping> columns,
        PersistKafkaMetaConfig meta,
        IReadOnlyList<ClickHouseColumn>? keyColumns)
    {
        var existingTargets = columns.Select(column => column.Target).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in KafkaMetaColumnFactory.CreateMappings(meta, keyColumns))
        {
            if (existingTargets.Add(mapping.Target))
                columns.Add(mapping);
        }
    }

    private static string? ResolveFieldNumberPath(
        string columnName,
        string? sourceTable,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable)
    {
        if (string.IsNullOrWhiteSpace(sourceTable)
            || !queueColumnsByTable.TryGetValue(sourceTable, out var columns))
        {
            return null;
        }

        return columns.FirstOrDefault(column =>
                string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
            ?.FieldNumberPath;
    }

    private static IReadOnlyList<ClickHouseColumn> ResolveSourceColumns(
        string? sourceTable,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable,
        string ownerName,
        string ownerKind)
    {
        if (string.IsNullOrWhiteSpace(sourceTable))
        {
            throw new InvalidOperationException(
                $"{ownerKind} '{ownerName}' has an empty columns list and no sourceTable. " +
                "Either list columns explicitly or set sourceTable to a kafkaTables[].tableName.");
        }

        if (!queueColumnsByTable.TryGetValue(sourceTable, out var columns))
        {
            throw new InvalidOperationException(
                $"{ownerKind} '{ownerName}' references unknown Kafka source table '{sourceTable}'.");
        }

        if (columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"{ownerKind} '{ownerName}' cannot expand columns from empty Kafka table '{sourceTable}'.");
        }

        return columns;
    }
}
