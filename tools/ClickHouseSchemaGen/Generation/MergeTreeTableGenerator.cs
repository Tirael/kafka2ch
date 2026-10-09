namespace ClickHouseSchemaGen.Generation;

public static class MergeTreeTableGenerator
{
    public static string Generate(MergeTreeTableConfig config, bool ifNotExists = false, ClusterDdl? cluster = null)
    {
        cluster ??= ClusterDdl.SingleNode;
        var storageTable = cluster.StorageTable(config.TableName);
        var create = ifNotExists
            ? $"CREATE TABLE IF NOT EXISTS {storageTable}{cluster.OnCluster}"
            : $"CREATE TABLE {storageTable}{cluster.OnCluster}";

        var builder = new StringBuilder()
            .AppendLine(create)
            .AppendLine("(");

        SqlScriptWriter.AppendColumnDefinitions(builder, config.Columns);

        builder
            .AppendLine(")")
            .AppendLine($"ENGINE = {cluster.StorageEngine("MergeTree")}")
            .Append($"ORDER BY {config.OrderBy}");

        if (!string.IsNullOrWhiteSpace(config.Ttl))
        {
            builder
                .AppendLine()
                .Append($"TTL {config.Ttl.Trim()}");
        }

        if (RequiresFlattenNested(config.Columns))
        {
            builder
                .AppendLine()
                .Append("SETTINGS flatten_nested = 0");
        }

        builder
            .AppendLine(";")
            .AppendLine();

        if (cluster.Enabled)
        {
            builder
                .Append(cluster.DistributedTableStatement(config.TableName, config.ShardingKey, ifNotExists))
                .AppendLine();
        }

        return builder.ToString();
    }

    private static bool RequiresFlattenNested(IReadOnlyList<PipelineColumnConfig> columns) =>
        columns.Any(column => column.Type.StartsWith("Nested(", StringComparison.Ordinal));
}
