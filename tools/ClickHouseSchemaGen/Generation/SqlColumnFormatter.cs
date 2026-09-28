namespace ClickHouseSchemaGen.Generation;

public static class SqlColumnFormatter
{
    public static string FormatColumnName(string name) =>
        name.Contains('.') ? $"`{name}`" : name;

    public static string FormatColumnLine(
        string name,
        string type,
        string? comment,
        string commaSuffix,
        string? aliasExpression = null)
    {
        var typedName = $"{FormatColumnName(name),-20} {type}";
        var definition = string.IsNullOrWhiteSpace(aliasExpression)
            ? typedName
            : $"{typedName} ALIAS {aliasExpression.Trim()}";
        return $"    {definition}{commaSuffix}{BuildCommentSuffix(comment)}";
    }

    public static string FormatBareDefinition(string name, string type) =>
        $"{FormatColumnName(name)} {type}";

    private static string BuildCommentSuffix(string? comment) =>
        string.IsNullOrWhiteSpace(comment) ? string.Empty : $"  -- {comment}";
}
