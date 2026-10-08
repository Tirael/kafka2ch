namespace ClickHouseSchemaGen.Generation;

public static class MaterializedViewGenerator
{
    /// <param name="includeFunctionDefinitions">
    /// Prepend the protobuf key UDFs the view uses, so the statement list is self-contained (migrations).
    /// </param>
    public static string Generate(MaterializedViewConfig config, bool includeFunctionDefinitions = true)
    {
        var builder = new StringBuilder();
        if (includeFunctionDefinitions && ProtobufWireSqlFunctions.IsUsedBy(config))
        {
            builder
                .AppendLine(ProtobufWireSqlFunctions.Definitions.TrimEnd())
                .AppendLine();
        }

        builder
            .AppendLine($"CREATE MATERIALIZED VIEW {config.Name} TO {config.TargetTable} AS")
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
