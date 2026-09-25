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

        return builder
            .AppendLine(";")
            .AppendLine()
            .ToString();
    }
}
