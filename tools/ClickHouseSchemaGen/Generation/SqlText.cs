namespace ClickHouseSchemaGen.Generation;

public static class SqlText
{
    public const string NewLine = "\n";

    public static string Normalize(string sql) => sql.ReplaceLineEndings(NewLine);
}
