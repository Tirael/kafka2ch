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
            var expression = ResolveExpression(mapping);
            var comma = i < config.Columns.Count - 1 ? "," : string.Empty;
            builder.AppendLine($"    {expression,-28} AS {mapping.Target}{comma}");
        }

        return builder
            .AppendLine($"FROM {config.SourceTable};")
            .AppendLine()
            .ToString();
    }

    private static string ResolveExpression(PipelineColumnMapping mapping)
    {
        if (!string.IsNullOrWhiteSpace(mapping.Expression))
            return mapping.Expression;

        return SqlColumnFormatter.FormatColumnName(mapping.Source);
    }
}
