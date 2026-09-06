namespace ClickHouseSchemaGen.Generation;

public static class SqlColumnFormatter
{
    public static string FormatColumnName(string name) =>
        name.Contains('.') ? $"`{name}`" : name;

    public static string FormatColumnLine(string name, string type, string? comment, string commaSuffix)
    {
        var formattedName = FormatColumnName(name);
        var commentSuffix = BuildCommentSuffix(comment);
        return $"    {formattedName,-20} {type}{commaSuffix}{commentSuffix}";
    }

    private static string BuildCommentSuffix(string? comment) =>
        string.IsNullOrWhiteSpace(comment) ? string.Empty : $"  -- {comment}";
}
