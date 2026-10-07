namespace ClickHouseSchemaGen.Snapshot;

public sealed class SchemaDriftException : Exception
{
    public SchemaDriftException(IReadOnlyList<string> differences, string configPath)
        : base(BuildMessage(differences, configPath))
    {
        Differences = differences;
        ConfigPath = configPath;
    }

    public IReadOnlyList<string> Differences { get; }

    public string ConfigPath { get; }

    private static string BuildMessage(IReadOnlyList<string> differences, string configPath)
    {
        var hint =
            $"Run: ClickHouseSchemaGen.Cli migrate --config {configPath} --name <migration_name>";
        if (differences.Count == 0)
            return $"ClickHouse schema drift detected. {hint}";

        return "ClickHouse schema drift detected:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, differences.Select(d => $"  - {d}"))
            + Environment.NewLine
            + hint;
    }
}
