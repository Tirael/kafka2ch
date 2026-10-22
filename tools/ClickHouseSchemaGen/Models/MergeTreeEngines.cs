namespace ClickHouseSchemaGen.Models;

public static class MergeTreeEngines
{
    public const string Default = "MergeTree";

    private static readonly Dictionary<string, (int Min, int Max)> ArgumentCounts = new(StringComparer.Ordinal)
    {
        [Default] = (0, 0),
        ["ReplacingMergeTree"] = (0, 2),
        ["SummingMergeTree"] = (0, 1),
        ["AggregatingMergeTree"] = (0, 0),
        ["CollapsingMergeTree"] = (1, 1)
    };

    public static IReadOnlyCollection<string> Supported => ArgumentCounts.Keys;

    public static string Normalize(string? engine)
    {
        if (string.IsNullOrWhiteSpace(engine))
            return Default;

        var trimmed = engine.Trim();
        if (!TryParse(trimmed, out var name, out var arguments))
            return trimmed;

        return arguments.Length == 0 ? name : $"{name}({arguments})";
    }

    public static string? ToSnapshot(string? engine)
    {
        var normalized = Normalize(engine);
        return normalized == Default ? null : normalized;
    }

    public static string? Validate(string? engine)
    {
        if (engine is null)
            return null;

        if (string.IsNullOrWhiteSpace(engine))
            return "engine must not be blank when provided.";

        if (!TryParse(engine.Trim(), out var name, out var arguments))
            return $"engine '{engine}' must look like Name or Name(arguments) with balanced parentheses.";

        if (name.StartsWith("Replicated", StringComparison.Ordinal))
        {
            return $"engine '{engine}' must not be Replicated*: replication is derived from the cluster config. " +
                $"Use '{name["Replicated".Length..]}' instead.";
        }

        if (!ArgumentCounts.TryGetValue(name, out var counts))
            return $"engine '{engine}' is not supported. Must be one of: {string.Join(", ", Supported)}.";

        var count = CountArguments(arguments);
        if (count < counts.Min || count > counts.Max)
        {
            var expected = counts.Min == counts.Max ? $"{counts.Min}" : $"{counts.Min}..{counts.Max}";
            return $"engine '{name}' takes {expected} argument(s), got {count}.";
        }

        return null;
    }

    private static bool TryParse(string engine, out string name, out string arguments)
    {
        var open = engine.IndexOf('(');
        name = (open < 0 ? engine : engine[..open]).Trim();
        arguments = string.Empty;
        if (name.Length == 0 || !name.All(char.IsAsciiLetterOrDigit))
            return false;

        if (open < 0)
            return true;

        if (engine[^1] != ')' || ClosingParenthesis(engine, open) != engine.Length - 1)
            return false;

        arguments = engine[(open + 1)..^1].Trim();
        return true;
    }

    private static int ClosingParenthesis(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            if (text[i] == '(')
                depth++;
            else if (text[i] == ')' && --depth == 0)
                return i;
        }

        return -1;
    }

    private static int CountArguments(string arguments)
    {
        if (arguments.Length == 0)
            return 0;

        var depth = 0;
        var count = 1;
        foreach (var ch in arguments)
        {
            if (ch == '(')
                depth++;
            else if (ch == ')')
                depth--;
            else if (ch == ',' && depth == 0)
                count++;
        }

        return count;
    }
}
