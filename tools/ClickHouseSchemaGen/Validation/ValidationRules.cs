using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Validation;

internal static partial class ValidationRules
{
    internal static readonly string[] RepeatedMessageStrategies = ["nested", "arraytuple", "flatten"];

    private static readonly HashSet<string> SqlExpressionKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "INTERVAL", "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS",
        "WEEK", "WEEKS", "MONTH", "MONTHS", "YEAR", "YEARS", "QUARTER", "QUARTERS",
        "TOINTERVALDAY", "TOINTERVALHOUR", "TOINTERVALMINUTE", "TOINTERVALSECOND",
        "TOINTERVALWEEK", "TOINTERVALMONTH", "TOINTERVALYEAR", "TOINTERVALQUARTER",
        "AND", "OR", "NOT", "NULL", "TRUE", "FALSE"
    };

    [GeneratedRegex("^[a-zA-Z_][a-zA-Z0-9_]*$")]
    private static partial Regex SqlIdentifierRegex();

    [GeneratedRegex("^[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")]
    private static partial Regex FieldPathRegex();

    [GeneratedRegex(@"[a-zA-Z_][a-zA-Z0-9_]*")]
    private static partial Regex SqlTokenRegex();

    public static bool IsSqlIdentifier(string value) => SqlIdentifierRegex().IsMatch(value);

    public static bool IsFieldPath(string value) => FieldPathRegex().IsMatch(value);

    public static bool IsMappingStrategy(string value) =>
        Enum.TryParse<MappingStrategy>(value, ignoreCase: true, out _);

    public static IEnumerable<string> ExtractColumnCandidates(string expression) =>
        SqlTokenRegex().Matches(expression)
            .Select(match => match.Value)
            .Where(token => !SqlExpressionKeywords.Contains(token))
            .Where(token => !ulong.TryParse(token, out _))
            .Distinct(StringComparer.OrdinalIgnoreCase);

    public static bool TtlReferencesKnownColumns(MergeTreeTableConfig table)
    {
        if (string.IsNullOrWhiteSpace(table.Ttl) || table.Columns.Count == 0)
            return true;

        var knownColumns = table.Columns
            .Select(column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return ExtractColumnCandidates(table.Ttl)
            .All(knownColumns.Contains);
    }
}
