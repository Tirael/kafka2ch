using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Validation;

internal static partial class ValidationRules
{
    internal static readonly string[] RepeatedMessageStrategies = ["nested", "arraytuple", "flatten"];

    [GeneratedRegex("^[a-zA-Z_][a-zA-Z0-9_]*$")]
    private static partial Regex SqlIdentifierRegex();

    [GeneratedRegex("^[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")]
    private static partial Regex FieldPathRegex();

    public static bool IsSqlIdentifier(string value) => SqlIdentifierRegex().IsMatch(value);

    public static bool IsFieldPath(string value) => FieldPathRegex().IsMatch(value);

    public static bool IsMappingStrategy(string value) =>
        Enum.TryParse<MappingStrategy>(value, ignoreCase: true, out _);
}
