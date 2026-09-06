namespace ClickHouseSchemaGen.Generation;

public static class MergeTreeTableGenerator
{
    public static string Generate(MergeTreeTableConfig config)
    {
        var builder = new StringBuilder()
            .AppendLine($"CREATE TABLE {config.TableName}")
            .AppendLine("(");

        SqlScriptWriter.AppendColumnDefinitions(builder, config.Columns);

        return builder
            .AppendLine(")")
            .AppendLine("ENGINE = MergeTree")
            .AppendLine($"ORDER BY {config.OrderBy};")
            .AppendLine()
            .ToString();
    }
}
