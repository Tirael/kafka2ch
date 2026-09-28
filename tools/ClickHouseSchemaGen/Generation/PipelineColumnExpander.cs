namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// Fills empty <c>pipeline.*.columns</c> from the mapped Kafka queue schema.
/// </summary>
public static class PipelineColumnExpander
{
    public static MergeTreeTableConfig ExpandMergeTreeTable(
        MergeTreeTableConfig table,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable)
    {
        if (table.Columns.Count > 0)
            return table;

        var sourceColumns = ResolveSourceColumns(table.SourceTable, queueColumnsByTable, table.TableName, "MergeTree table");
        return new MergeTreeTableConfig
        {
            TableName = table.TableName,
            OrderBy = table.OrderBy,
            Ttl = table.Ttl,
            SourceTable = table.SourceTable,
            Columns = sourceColumns
                .Select(column => new PipelineColumnConfig
                {
                    Name = column.Name,
                    Type = column.Type
                })
                .ToList()
        };
    }

    public static MaterializedViewConfig ExpandMaterializedView(
        MaterializedViewConfig view,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable)
    {
        if (view.Columns.Count > 0)
            return view;

        var sourceColumns = ResolveSourceColumns(view.SourceTable, queueColumnsByTable, view.Name, "Materialized view");
        return new MaterializedViewConfig
        {
            Name = view.Name,
            TargetTable = view.TargetTable,
            SourceTable = view.SourceTable,
            Columns = sourceColumns
                .Select(column => new PipelineColumnMapping
                {
                    Source = column.Name,
                    Target = column.Name
                })
                .ToList()
        };
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
