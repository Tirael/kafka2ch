namespace ClickHouseSchemaGen.Generation;

internal static class SqlScriptWriter
{
    public static void AppendCommaSeparatedLines(
        StringBuilder builder,
        IReadOnlyList<string> lines,
        string lastLineSuffix = ";",
        string linePrefix = "    ")
    {
        for (var i = 0; i < lines.Count; i++)
        {
            var suffix = i < lines.Count - 1 ? "," : lastLineSuffix;
            builder.AppendLine($"{linePrefix}{lines[i]}{suffix}");
        }
    }
}
