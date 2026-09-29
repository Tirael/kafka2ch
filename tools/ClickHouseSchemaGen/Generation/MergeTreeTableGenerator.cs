namespace ClickHouseSchemaGen.Generation;

public static class MergeTreeTableGenerator
{
    public static string Generate(MergeTreeTableConfig config)
    {
        var builder = new StringBuilder()
            .AppendLine($"CREATE TABLE {config.TableName}")
            .AppendLine("(");

        SqlScriptWriter.AppendColumnDefinitions(builder, config.Columns);

        builder
            .AppendLine(")")
            .AppendLine("ENGINE = MergeTree")
            .Append($"ORDER BY {config.OrderBy}");

        if (!string.IsNullOrWhiteSpace(config.Ttl))
        {
            builder
                .AppendLine()
                .Append($"TTL {config.Ttl.Trim()}");
        }

        // Nested(...) is rejected unless the table disables ClickHouse's default flattening.
        if (RequiresFlattenNested(config.Columns))
        {
            builder
                .AppendLine()
                .Append("SETTINGS flatten_nested = 0");
        }

        return builder
            .AppendLine(";")
            .AppendLine()
            .ToString();
    }

    private static bool RequiresFlattenNested(IReadOnlyList<PipelineColumnConfig> columns) =>
        columns.Any(column => column.Type.StartsWith("Nested(", StringComparison.Ordinal));
}
