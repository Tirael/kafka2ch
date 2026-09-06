namespace ClickHouseSchemaGen.Generation;

public static class MergeTreeTableGenerator
{
    public static string Generate(MergeTreeTableConfig config)
    {
        var builder = new StringBuilder()
            .AppendLine($"CREATE TABLE {config.TableName}")
            .AppendLine("(");

        for (var i = 0; i < config.Columns.Count; i++)
        {
            var column = config.Columns[i];
            var comma = i < config.Columns.Count - 1 ? "," : string.Empty;
            builder.AppendLine(SqlColumnFormatter.FormatColumnLine(column.Name, column.Type, comment: null, comma));
        }

        return builder
            .AppendLine(")")
            .AppendLine("ENGINE = MergeTree")
            .AppendLine($"ORDER BY {config.OrderBy};")
            .AppendLine()
            .ToString();
    }
}
