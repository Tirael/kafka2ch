namespace ClickHouseSchemaGen.Generation;

public static class MergeTreeTableGenerator
{
    public static string Generate(MergeTreeTableConfig config, bool ifNotExists = false)
    {
        var create = ifNotExists
            ? $"CREATE TABLE IF NOT EXISTS {config.TableName}"
            : $"CREATE TABLE {config.TableName}";

        var builder = new StringBuilder()
            .AppendLine(create)
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
