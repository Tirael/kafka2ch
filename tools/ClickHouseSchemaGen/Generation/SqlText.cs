namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// ClickHouse SQL artifacts are consumed on Linux (Docker bind-mounts, init scripts).
/// Always emit LF so Windows agents do not write CRLF that breaks committed-file golden tests.
/// </summary>
public static class SqlText
{
    public const string NewLine = "\n";

    public static string Normalize(string sql) => sql.ReplaceLineEndings(NewLine);
}
