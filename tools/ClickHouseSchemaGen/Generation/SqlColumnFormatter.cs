namespace ClickHouseSchemaGen.Generation;

public static class SqlColumnFormatter
{
    public static string FormatColumnName(string name) =>
        name.Contains('.') ? $"`{name}`" : name;

    public static string FormatColumnLine(string name, string type, string? comment, string commaSuffix) =>
        $"    {FormatColumnName(name),-20} {type}{commaSuffix}{BuildCommentSuffix(comment)}";

    public static string FormatBareDefinition(string name, string type) =>
        $"{FormatColumnName(name)} {type}";

    private static string BuildCommentSuffix(string? comment) =>
        string.IsNullOrWhiteSpace(comment) ? string.Empty : $"  -- {comment}";
}
