namespace ClickHouseSchemaGen.Generation;

public static class AggregateGenerator
{
    public static string Generate(IEnumerable<AggregatePlan> aggregates, ClusterDdl cluster)
    {
        ArgumentNullException.ThrowIfNull(aggregates);

        var builder = new StringBuilder();
        foreach (var aggregate in aggregates)
            builder.Append(Generate(aggregate, cluster));

        return SqlText.Normalize(builder.ToString());
    }

    public static string Generate(AggregatePlan aggregate, ClusterDdl cluster, bool ifNotExists = false) =>
        GenerateTable(aggregate, cluster, ifNotExists) + GenerateView(aggregate, cluster);

    public static string GenerateTable(AggregatePlan aggregate, ClusterDdl cluster, bool ifNotExists = false)
    {
        ArgumentNullException.ThrowIfNull(aggregate);
        ArgumentNullException.ThrowIfNull(cluster);

        var create = ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
        var columns = aggregate.TargetColumns;
        var width = columns.Max(column => column.Name.Length) + 2;
        var builder = new StringBuilder()
            .AppendLine($"{create} {cluster.StorageTable(aggregate.Name)}{cluster.OnCluster}")
            .AppendLine("(");

        for (var i = 0; i < columns.Count; i++)
        {
            var comma = i < columns.Count - 1 ? "," : string.Empty;
            builder.AppendLine($"    {columns[i].Name.PadRight(width)}{columns[i].Type}{comma}");
        }

        builder
            .AppendLine(")")
            .AppendLine($"ENGINE = {cluster.RefreshTargetEngine(aggregate.Engine)}")
            .AppendLine($"ORDER BY {aggregate.OrderBy};")
            .AppendLine();

        if (cluster.Enabled)
        {
            builder
                .Append(cluster.DistributedTableStatement(aggregate.Name, shardingKey: null, ifNotExists))
                .AppendLine();
        }

        return SqlText.Normalize(builder.ToString());
    }

    public static string GenerateView(AggregatePlan aggregate, ClusterDdl cluster)
    {
        ArgumentNullException.ThrowIfNull(aggregate);
        ArgumentNullException.ThrowIfNull(cluster);

        var selections = SelectList(aggregate);
        var width = selections.Where(item => item.Alias is not null).Select(item => item.Expression.Length).DefaultIfEmpty(0).Max();
        var builder = new StringBuilder()
            .AppendLine($"CREATE MATERIALIZED VIEW {aggregate.ViewName}{cluster.OnCluster}")
            .AppendLine($"REFRESH EVERY {aggregate.RefreshEvery}")
            .AppendLine($"TO {cluster.StorageTable(aggregate.Name)} AS")
            .AppendLine("SELECT");

        for (var i = 0; i < selections.Count; i++)
        {
            var (expression, alias) = selections[i];
            var comma = i < selections.Count - 1 ? "," : string.Empty;
            builder.AppendLine(alias is null
                ? $"    {expression}{comma}"
                : $"    {expression.PadRight(width)} AS {alias}{comma}");
        }

        var final = aggregate.Final ? " FINAL" : string.Empty;
        var groupKeys = aggregate.GroupKeys;
        if (groupKeys.Count == 0)
        {
            builder.AppendLine($"FROM {cluster.RefreshSourceTable(aggregate.SourceTable)}{final};");
        }
        else
        {
            builder
                .AppendLine($"FROM {cluster.RefreshSourceTable(aggregate.SourceTable)}{final}")
                .AppendLine($"GROUP BY {string.Join(", ", groupKeys)};");
        }

        return SqlText.Normalize(builder.AppendLine().ToString());
    }

    private static List<(string Expression, string? Alias)> SelectList(AggregatePlan aggregate)
    {
        List<(string Expression, string? Alias)> items = [];
        if (aggregate.TimeBucket is not null)
            items.Add((aggregate.TimeBucket.Expression, aggregate.TimeBucket.Alias));

        items.AddRange(aggregate.GroupBy.Select(column => (column.Name, (string?)null)));
        items.AddRange(aggregate.Measures.Select(measure => (measure.Expression, (string?)measure.Alias)));
        return items;
    }
}
