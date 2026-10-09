namespace ClickHouseSchemaGen.Generation;

public static class MaterializedViewGenerator
{
    /// <param name="includeFunctionDefinitions">
    /// Prepend the protobuf key UDFs the view uses, so the statement list is self-contained (migrations).
    /// </param>
    /// <param name="cluster">
    /// In cluster mode the view exists on every node and writes to the node-local storage table, so each
    /// node's Kafka consumers insert into their own shard.
    /// </param>
    public static string Generate(
        MaterializedViewConfig config,
        bool includeFunctionDefinitions = true,
        ClusterDdl? cluster = null)
    {
        cluster ??= ClusterDdl.SingleNode;
        var builder = new StringBuilder();
        if (includeFunctionDefinitions && ProtobufWireSqlFunctions.IsUsedBy(config))
        {
            builder
                .AppendLine(cluster.ProtobufWireFunctionDefinitions().TrimEnd())
                .AppendLine();
        }

        builder
            .AppendLine($"CREATE MATERIALIZED VIEW {config.Name}{cluster.OnCluster} TO {cluster.StorageTable(config.TargetTable)} AS")
            .AppendLine("SELECT");

        for (var i = 0; i < config.Columns.Count; i++)
        {
            var mapping = config.Columns[i];
            var comma = i < config.Columns.Count - 1 ? "," : string.Empty;
            builder.AppendLine($"    {ResolveExpression(mapping),-28} AS {SqlColumnFormatter.FormatColumnName(mapping.Target)}{comma}");
        }

        return builder
            .AppendLine($"FROM {config.SourceTable};")
            .AppendLine()
            .ToString();
    }

    private static string ResolveExpression(PipelineColumnMapping mapping) =>
        !string.IsNullOrWhiteSpace(mapping.Expression)
            ? mapping.Expression
            : SqlColumnFormatter.FormatColumnName(mapping.Source);
}
