namespace ClickHouseSchemaGen.Generation;

public static class MaterializedViewGenerator
{
    public static string Generate(
        MaterializedViewConfig config,
        bool includeFunctionDefinitions = true,
        ClusterDdl? cluster = null,
        bool? writeThroughDistributed = null)
    {
        cluster ??= ClusterDdl.SingleNode;
        var builder = new StringBuilder();
        if (includeFunctionDefinitions && ProtobufWireSqlFunctions.IsUsedBy(config))
        {
            builder
                .AppendLine(cluster.ProtobufWireFunctionDefinitions().TrimEnd())
                .AppendLine();
        }

        var target = cluster.MaterializedViewTarget(config.TargetTable, writeThroughDistributed);
        builder
            .AppendLine($"CREATE MATERIALIZED VIEW {config.Name}{cluster.OnCluster} TO {target} AS")
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
