namespace ClickHouseSchemaGen.Generation;

public static class SqlText
{
    public const string NewLine = "\n";

    public static string Normalize(string sql)
    {
        var lines = sql.ReplaceLineEndings(NewLine).Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            var trimmed = lines[i].TrimStart();
            if (IsTopLevelClause(trimmed))
                lines[i] = trimmed;
        }

        return string.Join(NewLine, lines);
    }

    private static bool IsTopLevelClause(string line) =>
        line is ")"
        || line.StartsWith("ENGINE ", StringComparison.Ordinal)
        || line.StartsWith("ENGINE=", StringComparison.Ordinal)
        || line.StartsWith("ORDER BY ", StringComparison.Ordinal)
        || line.StartsWith("TTL ", StringComparison.Ordinal)
        || line.StartsWith("SETTINGS ", StringComparison.Ordinal);
}
