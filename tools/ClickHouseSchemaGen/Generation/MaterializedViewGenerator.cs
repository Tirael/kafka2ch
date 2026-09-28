namespace ClickHouseSchemaGen.Generation;

public static class MaterializedViewGenerator
{
    public static string Generate(MaterializedViewConfig config)
    {
        var builder = new StringBuilder()
            .AppendLine($"CREATE MATERIALIZED VIEW {config.Name} TO {config.TargetTable} AS")
            .AppendLine("SELECT");

        for (var i = 0; i < config.Columns.Count; i++)
        {
            var mapping = config.Columns[i];
            var comma = i < config.Columns.Count - 1 ? "," : string.Empty;
            builder.AppendLine($"    {ResolveExpression(mapping),-28} AS {mapping.Target}{comma}");
        }

        if (config.SourceSelectExtras.Count == 0)
        {
            return builder
                .AppendLine($"FROM {config.SourceTable};")
                .AppendLine()
                .ToString();
        }

        builder.AppendLine("FROM")
            .AppendLine("(")
            .AppendLine("    SELECT")
            .AppendLine("        *,");

        for (var i = 0; i < config.SourceSelectExtras.Count; i++)
        {
            var comma = i < config.SourceSelectExtras.Count - 1 ? "," : string.Empty;
            builder.AppendLine($"        {config.SourceSelectExtras[i]}{comma}");
        }

        return builder
            .AppendLine($"    FROM {config.SourceTable}")
            .AppendLine(");")
            .AppendLine()
            .ToString();
    }

    private static string ResolveExpression(PipelineColumnMapping mapping) =>
        !string.IsNullOrWhiteSpace(mapping.Expression)
            ? mapping.Expression
            : SqlColumnFormatter.FormatColumnName(mapping.Source);
}
